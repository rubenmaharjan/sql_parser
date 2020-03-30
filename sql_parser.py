__author__ = "Gaurav Khatri, Ruben Maharjan"
__version__ = "1.0.0"

import re
import linecache
import csv

class SqlParser:
    """
    A class containing all the fuctions required to parse sql queries
    """

    KEYWORDS = ('SELECT', 'FROM', 'WHERE', 'JOIN')

    @staticmethod
    def validity_check(list_elements):
        """
        Function to check if the extracted table names have valid schemas appended to them
        """
        list_of_acceptables = ['DW_', 'DWH', 'STG', 'TMP', 'SIM132']
        return [True for i in list_of_acceptables if i in list_elements]

    @staticmethod
    def replacer(txt):
        """
        This function is meant to clean the text
        """
        txt = txt.replace('\n', ' ')\
                .replace('\t', '')\
                .replace('"', '')\
                .replace(')', ' ) ')\
                .replace('(', ' ( ')\
                .split(' ')
        return [x for x in txt if x]			#bcoz empty string is interpreted as False in Python

    #helper function designed to find line num of a phrase in a given file
    @staticmethod
    def line_num(phrase, dfile):
        """
        Function to get the line index
        """
        with open(dfile, 'r') as f:
            return next((i for (i, line) in enumerate(f) if phrase in line), None)


    #
    @staticmethod
    def check_ending_pattern(starting_linenum, dfilename):
        """
        This function is used to jump to the ending timestamp
        once we have found the starting pattern
        """
        ending_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6}: )'

        line = linecache.getline(dfilename, starting_linenum)
        pattern_search = re.search(ending_pattern, line)

        while (bool(pattern_search)) is False:
            starting_linenum += 1
            line = linecache.getline(dfilename, starting_linenum)
            pattern_search = re.search(ending_pattern, line)

        return starting_linenum -1

    @staticmethod
    def schema_seperator(table: str):
        """
        Funtion to split out schema and table
        """
        if '.' in table:
            (schema, table_name) = table.split('.')
            return schema, table_name
        return '', ''

    @staticmethod
    def remote_db_seperator(table: str):
        """
        Funtion to split out remote db and table
        """
        if '@' in table:
            (table_name, remote_db) = table.split('@')
            return table_name, remote_db
        return table, ''


    @staticmethod 
    def csv_writer(output_file_name, input_file_name, query_registry: dict):
        with open(output_file_name, 'a') as output:
            writer = csv.writer(output)
            writer.writerow(["File_Name", "Activity_Type", "Source_Schema", "Source_Table", "Target_Schema", "Target_Table", "Remote_DB"])
            for query in query_registry.values():
                parsed_sql = SqlParser.source_table_finder(query)
                row_list = parsed_sql
                (target_schema, target_table_name) = SqlParser.schema_seperator(parsed_sql["target_table"])
                if parsed_sql['source_table_list']:
                    for value in parsed_sql['source_table_list']:
                        (source_schema, source_table_name) = SqlParser.schema_seperator(value)
                        (source_table_name, source_remote_db) = SqlParser.remote_db_seperator(source_table_name)
                        writer.writerow([input_file_name, parsed_sql["activity_type"], source_schema, source_table_name, target_schema, target_table_name, source_remote_db])
                else:
                    writer.writerow([input_file_name, parsed_sql["activity_type"], '', '', target_schema, target_table_name])

    @staticmethod 
    def file_parser(filename: str):
        """
        Input: filename 
        Output: List of line number where the valid query resides
        """
        with open(filename,'r') as myfile:
            query_dex = {}
            query_counter = 1
            for myline in myfile:

                x = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6}: *\n)', myline)
                if bool(re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6}: *\n)', myline)):
                    valid_line_data = x.group()
                    # print('Querydata : '+valid_line_data)
                    valid_line_index = SqlParser.line_num(valid_line_data,filename)+2      #index of entry point (starts from 0)
                    #print('start_index: '+ str(valid_line_index))
                    ending_line_index = SqlParser.check_ending_pattern(valid_line_index+2,filename)
                    # print('ending_line ' + str(ending_line_index))

                    query_dex[str(query_counter)] = [valid_line_index,ending_line_index]
                    query_counter += 1
            return query_dex

    """
    The following 2 functions are  the main part
    """
    @staticmethod
    def writer(query_dex, filename) -> dict:
        """
        Input : filename, dictionary that gives start_end lines of queries
        output : Reads a file, creates a dictionary of {index:query}
        """
        query_dict: dict = {}
        for key in query_dex:
            start_line = query_dex[key][0]
            end_line = query_dex[key][1]
            query_str = ' '
            while end_line >= start_line:
                line = linecache.getline(filename, start_line)
                start_line += 1
                query_str += line

            query_dict[key] = query_str
            # print(query_dict)

        return query_dict

    #it parses the query to find src and tgt tables
    @staticmethod
    def source_table_finder(txt):
        Source_tables_list = []
        Target_tables_list = []
        DELETE_FLG = False
        UPDATE_FLG = False
        data = {'activity_type': '', 'source_table_list': '', 'target_table': ''}

        txt_list = SqlParser.replacer(txt)

        ###Checking for DELETE FROM Statement
        for index,value in enumerate(txt_list):
            if value == 'DELETE' and txt_list[index+1] == 'FROM':
                data['activity_type'] = 'DELETE'
                DELETE_FLG = True
            if value == 'UPDATE':
                UPDATE_FLG = True
                data['activity_type'] = 'UPDATE'

        source_table_list = []

        if UPDATE_FLG == False and DELETE_FLG==False :
            data['activity_type'] = 'INSERT/SELECT'

        target_table = [txt_list[i+1] for i, x in enumerate(txt_list) if x == 'INTO']

        if UPDATE_FLG is True:
            target_table = [txt_list[i+1] for i,x in enumerate(txt_list) if x == 'UPDATE']

        # if DELETE_FLG is False:
        source_candidate  = [txt_list[i+1] for i,x in enumerate(txt_list) if x == 'FROM']
        filtered_candidate = [i for i in source_candidate if SqlParser.validity_check(i)]

        source_table_list += filtered_candidate

        source_candidate.clear()
        filtered_candidate.clear()

        source_candidate  = [txt_list[i+1] for i,x in enumerate(txt_list) if x == 'JOIN']

        
        filtered_candidate = [i for i in source_candidate if SqlParser.validity_check(i)]

        if(len(source_candidate)!= len(filtered_candidate)):
            pass
            #print('Certain list_elements has been removed')

        source_table_list += filtered_candidate

        if DELETE_FLG is True:
            target_table = source_table_list[0]
            source_table_list = source_table_list[1:]


        source_table_list = [i for i in set(source_table_list)]
        data['source_table_list'] = source_table_list
        if target_table:
            data['target_table'] = target_table[0]
        else:
            data['target_table'] = ''

        if data['activity_type'] == 'INSERT/SELECT' and len(data['target_table']) == 0:
            data['activity_type'] = 'SELECT'
        elif data['activity_type'] == 'INSERT/SELECT':
            data['activity_type'] = 'INSERT'


        return data

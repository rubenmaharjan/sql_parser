import sys
import os
from sql_parser import SqlParser


#The game starts here
directory = sys.argv[1]
list_of_files = []
if os.path.isdir(directory):
    for f in os.listdir(directory):
        filename = os.fsdecode(f)
        list_of_files.append(filename)
else:
    if len(sys.argv) >= 2:
        list_of_files.extend(sys.argv[1:])
    else:
        print("Please provide filename(s) : python <scriptname> <filename(s)>")
        sys.exit(0)

for filename in list_of_files:
    query_dex = SqlParser.file_parser(filename)
    query_registry: dict = SqlParser.writer(query_dex, filename)
    data = {}
    SqlParser.csv_writer('output.csv', filename, query_registry)

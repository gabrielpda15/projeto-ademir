import sys

import read_files
import tfidf
import similarity
import precision
import maps

database_name = 'database.db'
encoding = 'utf8'
precisions = [ 10, 20, 50, 100 ]
out_extension = '.txt'
in_extension = '.txt'

run_type = []
orded_run_types = [ 'read_files', 'tfidf', 'similarity', 'precision', 'maps' ]

if __name__ == "__main__":
    for i, arg in enumerate(sys.argv):
        if (i != 0 and arg.startswith('--')):
            splited = arg.split('=')
            if (len(splited) == 2):
                key = splited[0][2:]
                value = splited[1]
                if key == 'db':
                    database_name = value
                elif key == 'encoding':
                    encoding = value
                elif key == 'precisions':
                    precisions = [int(e) for e in value.split(';')]
                elif key == 'in_ext':
                    in_extension = value
                elif key == 'out_ext':
                    out_extension = value
            elif (len(splited) == 1):
                run_type.append(splited[0][2:])

    if ('full' in run_type):
        read_files.execute(database_name, encoding, in_extension)
        tfidf.execute(database_name)
        similarity.execute(database_name)
        precision.execute(database_name, out_extension, precisions)
        maps.execute(database_name, out_extension)
    else:
        for e in orded_run_types:
            if (e in run_type):
                if (e == 'read_files'):
                    read_files.execute(database_name, encoding, in_extension)
                elif (e == 'tfidf'):
                    tfidf.execute(database_name)
                elif (e == 'similarity'):
                    similarity.execute(database_name)
                elif (e == 'precision'):
                    precision.execute(database_name, out_extension, precisions)
                elif (e == 'maps'):
                    maps.execute(database_name, out_extension)

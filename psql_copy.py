#!/usr/bin/env python

""" Doing export of data from csv files into psql tables """

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import argparse
import psycopg2
import sys
from os import listdir, environ
from os.path import isfile, join
from json import load
from gizer.psql_requests import PsqlRequests
from gizer.psql_objects import create_psql_table
from mongo_schema.schema_engine import SchemaEngine
from mongo_schema.schema_engine import create_tables_load_bson_data

def message(mes, crret='\n'):
    """ write mes to stderr """
    sys.stderr.write(mes + crret)

def copy_from_csv(dbreq, input_f, table_name):
    """Fastest approach, need specific csv format"""
    #use two slashes as '\N' became '\\N' when writing escaping csv data
    dbreq.cursor.copy_from(input_f, table_name, null='\\\\N')
    dbreq.cursor.execute('COMMIT')
    message('Exported csv %s' % (input_f.name))

def main():
    """ main """
    parser = argparse.ArgumentParser()
    parser.add_argument("-cn", "--collection-name",
                        help="Mongo collection name",
                        type=str, required=True)
    parser.add_argument("-ifs", "--input-file-schema", action="store",
                        help="Input file with json schema",
                        type=file, required=True)
    parser.add_argument("--psql-table-name", type=str, required=True)
    parser.add_argument("--psql-schema-name", type=str, required=False)
    parser.add_argument("--psql-table-prefix", type=str, required=False)
    parser.add_argument("--input-csv-dir", type=str, required=True)
    args = parser.parse_args()

    schema_name = ""
    if args.psql_schema_name:
        schema_name = args.psql_schema_name

    table_prefix = ""
    if args.psql_table_prefix:
        table_prefix = args.psql_table_prefix

    schema = SchemaEngine(args.collection_name,
                          [load(args.input_file_schema)])
    table = create_tables_load_bson_data(schema, None)\
        .tables[args.psql_table_name]
    dbreq = PsqlRequests(psycopg2.connect(environ['PSQLCONN']))

    create_psql_table(table, dbreq, schema_name, table_prefix, drop=True)
    dbreq.cursor.execute('COMMIT')

    csv_files = [f \
                 for f in listdir(args.input_csv_dir) \
                 if isfile(join(args.input_csv_dir, f))]
    csv_files.sort()
    for name in csv_files:
        csvpath = join(args.input_csv_dir, name)
        with open(csvpath, 'rb') as csv_f:
            schema_name_subst = schema_name
            if len(schema_name):
                schema_name_subst += '.'
            tname = '%s"%s%s"' % (schema_name_subst,
                                  table_prefix,
                                  args.psql_table_name)
            copy_from_csv(dbreq, csv_f, tname)

if __name__ == "__main__":
    main()

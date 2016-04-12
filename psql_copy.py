#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import argparse
import psycopg2
import collections
import sys
from os import listdir, environ
from os.path import isfile, join
from json import load
from gizer.psql_requests import PsqlRequests
from gizer.opinsert import format_string_insert_query
from gizer.opcsv import CsvReader
from gizer.opcreate import generate_drop_table_statement
from gizer.opcreate import generate_create_table_statement
from mongo_schema.schema_engine import SchemaEngine
from mongo_schema.schema_engine import create_tables_load_bson_data

def message(mes, cr='\n'):
    sys.stderr.write( mes + cr)

def copy_from_csv(dbreq, f, table_name):
    #use two slashes as '\N' became '\\N' when writing escaping csv data
    dbreq.cursor.copy_from(f, table_name, null='\\\\N')
    dbreq.cursor.execute('COMMIT')
    message('Exported csv %s' % (f.name))

def export_csv_file(dbreq, f, fmtstring):
    reader = CsvReader(f)
    dbreq.cursor.execute('BEGIN')
    
    count = 0
    while True:
        csvrec = reader.read_record()
        if csvrec is not None:
            dbreq.cursor.execute(fmtstring, tuple(csvrec))
            count += 1
            message('.',cr='')
        else:
            break
    dbreq.cursor.execute('COMMIT')
    message('Exported %d records, csv %s' % (count, f.name))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-cn", "--collection-name", help="Mongo collection name", 
                        type=str, required=True)
    parser.add_argument("-ifs", "--input-file-schema", action="store",
                        help="Input file with json schema", type=file, required=True)
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

    js_schema = [load(args.input_file_schema)]
    schema = SchemaEngine(args.collection_name, js_schema)
    tables = create_tables_load_bson_data(schema, None).tables
    table = tables[args.psql_table_name]
    fmtstring = format_string_insert_query(table, schema_name, table_prefix)

    connstr = environ['PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))

    drop_table_stmt = \
        generate_drop_table_statement(table, schema_name, table_prefix)
    dbreq.cursor.execute(drop_table_stmt)

    create_table_stmt = \
        generate_create_table_statement(table, schema_name, table_prefix)
    dbreq.cursor.execute(create_table_stmt)

    csv_files = [f for f in listdir(args.input_csv_dir) if isfile(join(args.input_csv_dir, f))]
    csv_files.sort()
    for name in csv_files:
        csvpath = join(args.input_csv_dir, name)
        with open(csvpath, 'r') as f:
            schema_name_subst = schema_name
            if len(schema_name):
                schema_name_subst += '.'
            tname = '%s"%s%s"' % (schema_name_subst, table_prefix, args.psql_table_name)
            copy_from_csv(dbreq, f, tname)
            #export_csv_file(dbreq, f, fmtstring)


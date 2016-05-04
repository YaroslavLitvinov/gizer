#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

""" Copy all data related to specific mongo record into another 
tables set. Copying from one postgres DB to another is slow."""

import argparse
import psycopg2
import sys
from os import environ
from json import load
from gizer.psql_requests import PsqlRequests
from gizer.psql_objects import load_single_rec_into_tables_obj
from gizer.psql_objects import insert_tables_data_into_dst_psql
from gizer.psql_objects import insert_rec_from_one_tables_set_to_another
from mongo_schema.schema_engine import SchemaEngine
from mongo_schema.schema_engine import create_tables_load_bson_data


def message(mes, cr='\n'):
    sys.stderr.write( mes + cr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-cn", "--collection-name", help="Mongo collection name", 
                        type=str, required=True)
    parser.add_argument("-ifs", "--input-file-schema", action="store",
                        help="Input file with json schema", type=file, 
                        required=True)
    parser.add_argument("--src-psql-schema-name", type=str, required=False)
    parser.add_argument("--dst-psql-schema-name", type=str, required=False)
    parser.add_argument("--rec-id", action="store",
                        help="Mongo record id", type=str, required=True)
    args = parser.parse_args()

    dst_table_prefix = ''
    dst_schema_name = src_schema_name = ""
    if args.src_psql_schema_name:
        src_schema_name = args.src_psql_schema_name
    if args.dst_psql_schema_name:
        dst_schema_name = args.dst_psql_schema_name


    if environ['PSQLCONN'] == environ['TEST_PSQLCONN'] or \
            not environ['TEST_PSQLCONN'] or \
            not len(environ['TEST_PSQLCONN']):
        dst_dbreq = None
    else:
        # will use slow approach to copy data from one to another database
        dst_dbreq = PsqlRequests(psycopg2.connect(environ['TEST_PSQLCONN']))
    src_dbreq = PsqlRequests(psycopg2.connect(environ['PSQLCONN']))

    # load tables structures only, no data
    js_schema = [load(args.input_file_schema)]
    schema = SchemaEngine(args.collection_name, js_schema)

    if dst_dbreq:
        # fetch mongo rec by id from source psql
        tables_to_save = load_single_rec_into_tables_obj(src_dbreq,
                                                         schema, 
                                                         src_schema_name,
                                                         args.rec_id)
    
        insert_tables_data_into_dst_psql(dst_dbreq, tables_to_save,
                                         dst_schema_name, dst_table_prefix)
    else:
        print "Using the same DB as source and destination"
        tables_structure = create_tables_load_bson_data(schema, None)
        insert_rec_from_one_tables_set_to_another(src_dbreq, 
                                                  args.rec_id,
                                                  tables_structure,
                                                  src_schema_name,
                                                  dst_schema_name)

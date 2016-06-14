#!/usr/bin/env python

""" Doing export of data from csv files into psql tables """

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import argparse
import psycopg2
import configparser
import logging
from logging import getLogger
from os import listdir, environ
from os.path import isfile, join
from json import load
from mongo_schema.schema_engine import SchemaEngine
from mongo_schema.schema_engine import create_tables_load_bson_data
from gizer.psql_requests import PsqlRequests
from gizer.psql_objects import create_psql_table
from gizer.psql_requests import psql_conn_from_settings
from gizer.opconfig import psql_settings_from_config

def copy_from_csv(dbreq, input_f, table_name):
    """Fastest approach, need specific csv format"""
    #use two slashes as '\N' became '\\N' when writing escaping csv data
    dbreq.cursor.copy_from(input_f, table_name, null='\\\\N')
    dbreq.cursor.execute('COMMIT')
    getLogger(__name__).info('Exported csv %s' % (input_f.name))

def main():
    """ main """
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", action="store",
                        help="Settings file", type=file, required=True)
    parser.add_argument("--psql-section", help="Psql section name from config",
                        type=str, required=True)
    parser.add_argument("-cn", "--collection-name", help="Mongo collection name",
                        type=str, required=True)
    parser.add_argument("--psql-table-name", type=str, required=True)
    parser.add_argument("-psql-table-prefix", type=str, required=False)
    parser.add_argument("--input-csv-dir", type=str, required=True)
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read_file(args.config_file)

    schema_name = config['psql']['psql-schema-name']
    schemas_dir = config['misc']['schemas-dir']
    schema_path = join(schemas_dir, args.collection_name + '.json')
    schema_file = open(schema_path, 'r')

    psql_settings = psql_settings_from_config(config, args.psql_section)

    table_prefix = ""
    if args.psql_table_prefix:
        table_prefix = args.psql_table_prefix

    schema = SchemaEngine(args.collection_name, [load(schema_file)])
    table = create_tables_load_bson_data(schema, None)\
        .tables[args.psql_table_name]
    dbreq = PsqlRequests(psql_conn_from_settings(psql_settings))

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
    logging.basicConfig(level=logging.INFO,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    main()

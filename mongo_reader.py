#!/usr/bin/env python

""" Read data from mongo collection and save it's relational model to csvs """

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import pprint
import os
import sys
import json
import argparse
import configparser
from collections import namedtuple
# profiling
from pstats import Stats
from cProfile import Profile
# for data input
from mongo_reader.reader import MongoReader
from mongo_reader.reader import mongo_reader_from_settings
# modules mostly used by data output functions
from mongo_schema.schema_engine import SchemaEngine
from mongo_schema.schema_engine import create_tables_load_bson_data
from gizer.opcsv import CsvWriteManager
from gizer.opcsv import NULLVAL
from gizer.opcreate import generate_create_table_statement
from gizer.opcreate import generate_drop_table_statement
from gizer.opinsert import table_rows_list
from gizer.opinsert import ENCODE_ONLY
from gizer.opmultiprocessing import FastQueueProcessor
from gizer.opconfig import MongoSettings
from gizer.opconfig import mongo_settings_from_config

CSV_CHUNK_SIZE = 1024 * 1024 * 100  # 100MB
ETL_PROCESS_NUMBER = 8
ETL_QUEUE_SIZE = ETL_PROCESS_NUMBER*2
GET_QUEUE_SIZE = ETL_PROCESS_NUMBER*2

TablesToSave = namedtuple('TablesToSave', ['rows', 'errors'])
MongoSettings = namedtuple('MongoSettings',
                           ['host', 'port', 'dbname', 'collection',
                            'request'])

def message(mes, crret='\n'):
    """ put mes to stderr """
    sys.stderr.write(mes + crret)

def create_table(sqltable, psql_schema_name, table_prefix):
    """ get drop / create ddl statements """
    drop = generate_drop_table_statement(sqltable, psql_schema_name,
                                         table_prefix)
    create = generate_create_table_statement(sqltable, psql_schema_name,
                                             table_prefix)
    return drop + '\n' + create + '\n'

def merge_dicts(store, append):
    """ merge two dicts, return merged dict. """
    for index_key, index_val in append.iteritems():
        cached_val = 0
        if index_key in store:
            cached_val = store[index_key]
        store[index_key] = index_val + cached_val
    return store

def save_ddl_create_statements(create_statements_file,
                               schema_engine,
                               psql_schema_name,
                               table_prefix):
    """ save create table statements to file """
    ddls = {}
    if not psql_schema_name:
        psql_schema_name = ''
    if not table_prefix:
        table_prefix = ''
    sqltables = create_tables_load_bson_data(schema_engine, None).tables
    for tablename, sqltable in sqltables.iteritems():
        ddls[tablename] = create_table(sqltable, psql_schema_name,
                                       table_prefix)
    for table_name in ddls:
        create_query = ddls[table_name]
        create_statements_file.write(create_query)

def save_csvs(csm, tables_rows):
    """ write relational tables to csv files.
    tables_rows -- dict {table_name: [rows]} of tables of rows to save"""
    written = {}
    for table_name in tables_rows:
        written[table_name] = csm.write_csv(table_name,
                                            tables_rows[table_name])
    return written

def retrieve_mongo_record(mongo_reader):
    """ get next record from mongo collection """
    rec = mongo_reader.next()
    count = 0
    if not hasattr(retrieve_mongo_record, "mongo_count"):
        retrieve_mongo_record.mongo_count = mongo_reader.cursor.count()
    count = retrieve_mongo_record.mongo_count
    if rec:
        message(".", crret="")
        if mongo_reader.rec_i % 1000 == 0:
            message("\n%d of %d" % (mongo_reader.rec_i, count))
    return rec

def async_worker_handle_mongo_rec(schema_engine, rec):
    """ function intended to call by FastQueueProcessor.
    process mongo record / bson data in separate process.
    schema_engine -- SchemaEngine
    rec - bson record"""
    rows_as_dict = {}
    tables_obj = create_tables_load_bson_data(schema_engine, [rec])
    for table_name, table in tables_obj.tables.iteritems():
        rows = table_rows_list(table, ENCODE_ONLY, null_value=NULLVAL)
        rows_as_dict[table_name] = rows
    return TablesToSave(rows=rows_as_dict, errors=tables_obj.errors)

# Fast queue helpers


def put_record_get_tables_async(fastqueue, rec):
    """ Put mongo record into pipeline to do parallel work in multiple
    processes. Get results asynchronously if available.
    Pipeline queue size can never exceed specified limit.
    fastqueue -- asyncronous pipeline object
    rec -- mongo record to put into pipeline"""
    finish = False
    res = []
    if rec:
        fastqueue.put(rec)
    get_all = fastqueue.count() \
              or fastqueue.poll() or fastqueue.is_any_working()
    while fastqueue.count() >= ETL_QUEUE_SIZE or fastqueue.poll() \
            or (not rec and get_all and not finish):
        async_res = fastqueue.get()
        if async_res:
            res.append(async_res)
        else:
            finish = True
    return res

def getargs():
    """ get args from cmdline """
    default_request = '{}'
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", action="store",
                        help="Config file with settings",
                        type=file, required=True)
    parser.add_argument("-cn", "--collection-name",
                        help="Mongo collection name ", type=str, required=True)
    parser.add_argument("-js-request",
                        help='Mongo db search request in json format. \
default=%s' % (default_request), type=str)
    parser.add_argument("-psql-table-prefix", help="", type=str)
    parser.add_argument("--ddl-statements-file",
                        help="File to save create table statements",
                        type=argparse.FileType('w'), required=True)
    parser.add_argument("-stats-file",
                        help="File to write written record counts",
                        type=argparse.FileType('w'))
    parser.add_argument("--csv-path",
                        help="base path for results",
                        type=str, required=True)

    args = parser.parse_args()
    if args.js_request is None:
        args.js_request = default_request

    return args

def print_profiler_stats(profiler):
    """ profiling results """
    profiler.disable()
    state_printer = Stats(profiler).sort_stats('cumulative')
    state_printer.print_stats()

def print_etl_stats(errors, all_written, etl_recs_count):
    """ etl summary """
    ppinter = pprint.PrettyPrinter(indent=4)
    ppinter.pprint(errors)
    ppinter.pprint(all_written)
    print "Etl records count should be =", etl_recs_count

def save_etl_stats(out_file, all_written):
    """ save list of tables with processed counts """
    if out_file:
        for name, value in all_written.iteritems():
            out_file.write(name + " " + str(value) + "\n")

def main():
    """ main """
    profiler = Profile()  # profiling
    profiler.enable()

    args = getargs()

    config = configparser.ConfigParser()
    config.read_file(args.config_file)

    schema_name = config['psql']['psql-schema-name']
    schemas_dir = config['misc']['schemas-dir']
    schema_path = os.path.join(schemas_dir, args.collection_name + '.json')
    schema_file = open(schema_path, 'r')

    mongo_settings = mongo_settings_from_config(config, 'mongo')

    mongo_reader = mongo_reader_from_settings(mongo_settings,
                                              args.collection_name,
                                              json.loads(args.js_request))
    schema_engine = SchemaEngine(args.collection_name, [json.load(schema_file)])
    table_names = create_tables_load_bson_data(schema_engine, None).tables.keys()
    csm = CsvWriteManager(table_names, args.csv_path, CSV_CHUNK_SIZE)
    mongo_rec_multiprocessing \
        = FastQueueProcessor(async_worker_handle_mongo_rec,
                             schema_engine,
                             ETL_PROCESS_NUMBER)
    message("Connecting to mongo server " + mongo_settings.host)
    errors = {}
    all_written = {}
    etl_recs_count = 0
    try:
        record = retrieve_mongo_record(mongo_reader)
        while True and not mongo_rec_multiprocessing.error:
            tables_list = put_record_get_tables_async(mongo_rec_multiprocessing,
                                                      record)
#            print "loop.b", len(tables_list), mongo_rec_multiprocessing.count()
            for tables in tables_list:
                all_written = merge_dicts(all_written,
                                          save_csvs(csm, tables.rows))
                errors = merge_dicts(errors, tables.errors)
            if not record:
                break
            else:
                etl_recs_count += 1
            record = retrieve_mongo_record(mongo_reader)

    except:
        mongo_reader.failed = True
        del mongo_rec_multiprocessing
        raise
    message("")

    save_ddl_create_statements(args.ddl_statements_file,
                               schema_engine,
                               schema_name,
                               args.psql_table_prefix)
    # save csv files
    csm.finalize()

    print_profiler_stats(profiler)
    print_etl_stats(errors, all_written, etl_recs_count)
    save_etl_stats(args.stats_file, all_written)

    del mongo_rec_multiprocessing
    exit(mongo_reader.failed)

if __name__ == "__main__":
    main()

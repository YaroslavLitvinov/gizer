#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

"""Read data from mongo collection"""

import pprint
import time
import cProfile
import pstats  # profiling
# modules affected in data in
import sys
import json
import argparse
from mongo_reader.reader import MongoReader
# modules affected in data out
import os
from multiprocessing import Pool
from mongo_schema import schema_engine
from gizer.opcsv import CsvManager
from gizer.opcreate import generate_create_table_statement
from gizer.opcreate import generate_drop_table_statement
from gizer.opinsert import generate_insert_queries
from gizer.opmultiprocessing import FastQueueProcessor


CSV_CHUNK_SIZE = 1024 * 1024 * 100  # 100MB
PROCESS_NUMBER = 7
QUEUE_SIZE = 7

# helpers

def message(mes, cr='\n'):
    sys.stderr.write(mes + cr)

def create_table(sqltable, psql_schema_name, table_prefix):
    drop = generate_drop_table_statement(sqltable, psql_schema_name,
                                         table_prefix)
    create = generate_create_table_statement(sqltable, psql_schema_name,
                                             table_prefix)
    return drop + '\n' + create + '\n'


def merge_dicts(store, append):
    for index_key, index_val in append.iteritems():
        cached_val = 0
        if index_key in store:
            cached_val = store[index_key]
        store[index_key] = index_val + cached_val
    return store


def create_table_queries(sqltables, psql_schema_name, table_prefix):
    if not hasattr(create_table_queries, "created_tables"):
        create_table_queries.created_tables = {}

    for tablename, sqltable in sqltables.iteritems():
        if tablename not in create_table_queries.created_tables:
            create_table_queries.created_tables[tablename] = \
                create_table(sqltable, psql_schema_name,
                             table_prefix)

def save_csvs(csm, tables_obj):
    if not hasattr(save_csvs, "max_indexes"):
        save_csvs.max_indexes = {}

    written = {}
    for name, table in tables_obj.tables.iteritems():
        reccount = csm.write_csv(table)
        written[name] = reccount
# cache initial indexes
    save_csvs.max_indexes = merge_dicts(save_csvs.max_indexes,
                                        tables_obj.data_engine.indexes)
    return written

# Asynchronous workers

def worker_retrieve_mongo_record(mongo_reader, foo):
    rec = mongo_reader.next()
    count = 0
    if not hasattr(worker_retrieve_mongo_record, "mongo_count"):
        worker_retrieve_mongo_record.mongo_count = mongo_reader.cursor.count()
    count = worker_retrieve_mongo_record.mongo_count
    if rec:
        message(".", cr="")
        if mongo_reader.rec_i % 1000 == 0:
            message("\n%d of %d" % (mongo_reader.rec_i, \
                                        count))
    return rec

def worker_handle_mongo_record(schema, rec):
    return schema_engine.create_tables_load_bson_data(schema, 
                                                      [rec])

# Fast queue helpers

def request_mongo_recs_async(fastqueue3):
    records = []
    rec = True
    finish = False
    # wait while queue size is not exceeded or if result available
    while (fastqueue3.count() >= QUEUE_SIZE or fastqueue3.poll()\
            or (finish and (fastqueue3.count() or fastqueue3.poll()))):
        rec = fastqueue3.get()
        records.append(rec)
        if not rec:
            finish = True
    for i in xrange(len(records)):
        fastqueue3.put('foo')
    return records

def get_tables_from_rec_async(fastqueue, mongo_rec, finish):
    tables_list = []    
    if mongo_rec:
        fastqueue.put(mongo_rec)
    while fastqueue.count() >= QUEUE_SIZE or fastqueue.poll()\
            or (finish and (fastqueue.count() or fastqueue.poll())):
        tables_list.append(fastqueue.get())
    return tables_list

def handle_tables_data_async(fastqueue2, tables_list, all_written):
    for tables_obj in tables_list:
        fastqueue2.put(tables_obj)
    while fastqueue2.count() >= QUEUE_SIZE or fastqueue2.poll():
        all_written = merge_dicts(all_written, fastqueue2.get())
    return all_written

def handle_rest_tables_data_sync(tables_list, schema_name, 
                                 table_prefix, all_errors):
    # rest of data handle synchronously
    for tables_obj in tables_list:
        create_table_queries(
            tables_obj.tables, schema_name, table_prefix)
        all_errors = merge_dicts(all_errors, tables_obj.errors)
    return all_errors


if __name__ == "__main__":

    default_request = '{}'

    parser = argparse.ArgumentParser()
    parser.add_argument("-ssl", action="store_true",
                        help="connect to ssl port")
    parser.add_argument("--host", help="Mongo db host:port",
                        type=str, required=True)
    parser.add_argument("-user", help="Mongo db user", type=str)
    parser.add_argument("-passw", help="Mongo db pass", type=str)
    parser.add_argument("-cn", "--collection-name", help="Mongo collection name that is \
expected in format db_name.collection_name", type=str, required=True)
    parser.add_argument("-ifs", "--input-file-schema", action="store",
                        help="Input file with json schema", type=file, required=True)
    parser.add_argument("-js-request", help='Mongo db search request in json format. \
default=%s' % (default_request), type=str)
    parser.add_argument("-psql-schema-name", help="", type=str)
    parser.add_argument("-psql-table-name-prefix", help="", type=str)
    parser.add_argument("--ddl-statements-file", help="File to save create table \
statements", type=argparse.FileType('w'), required=True)
    parser.add_argument("-stats-file", help="File to write written record counts",
                        type=argparse.FileType('w'))
    parser.add_argument(
        "--csv-path", help="base path for results", type=str, required=True)

    args = parser.parse_args()

    split_name = args.collection_name.split('.')
    if len(split_name) != 2:
        message("collection name is expected in format db_name.collection_name")
        exit(1)

    message("Connecting to mongo server " + args.host)
    split_host = args.host.split(':')
    if len(split_host) > 1:
        host = split_host[0]
        port = split_host[1]
    else:
        host = args.host
        port = 27017

    dbname = split_name[0]
    collection_name = split_name[1]

    if args.js_request is None:
        args.js_request = default_request

    pr = cProfile.Profile()  # profiling
    pr.enable()  # profiling

    search_request = json.loads(args.js_request)

    js_schema = [json.load(args.input_file_schema)]
    schema = schema_engine.SchemaEngine(collection_name, js_schema)
    mongo_reader = MongoReader(args.ssl, host, port,
                               dbname, collection_name,
                               args.user, args.passw, search_request)

    psql_schema_name = args.psql_schema_name
    if not args.psql_schema_name:
        psql_schema_name = ''

    table_prefix = args.psql_table_name_prefix
    if not args.psql_table_name_prefix:
        table_prefix = ''

    sqltables = schema_engine.create_tables_load_bson_data(schema, None).tables
    table_names = sqltables.keys()

    csm = CsvManager(table_names, args.csv_path, CSV_CHUNK_SIZE)
    pp = pprint.PrettyPrinter(indent=4)
    errors = {}
    all_wrtitten_reccount = {}
    # create pymongo retriever to be used as parallel process
    pymongo_request_processing \
        = FastQueueProcessor(worker_retrieve_mongo_record, mongo_reader, 1) 
    mongo_rec_multiprocessing \
        = FastQueueProcessor(worker_handle_mongo_record, 
                             schema, 
                             PROCESS_NUMBER)
    records_available = True
    records = []
    for i in range(QUEUE_SIZE):
        pymongo_request_processing.put('foo')
    try:
        while len(records) or records_available:
            # print "mongo_reader loop", len(records), records_available
            for rec in records:
                if not rec:
                    records_available = False
                tables_list = get_tables_from_rec_async(\
                    mongo_rec_multiprocessing, rec, not records_available)
                for tables_obj in tables_list:
                    all_wrtitten_reccount = merge_dicts(all_wrtitten_reccount, 
                                                        save_csvs(csm, tables_obj))
                errors = handle_rest_tables_data_sync(tables_list, 
                                                      psql_schema_name,
                                                      table_prefix,
                                                      errors)
            del records[:]
            if records_available:
                records = request_mongo_recs_async(pymongo_request_processing)
    except KeyboardInterrupt:
        mongo_reader.failed = True

# save create table statements
    for table_name, create_query in \
            create_table_queries.created_tables.iteritems():
        args.ddl_statements_file.write(create_query)
# save csv files
    csm.finalize()
    message("")
    pr.disable()
    ps = pstats.Stats(pr).sort_stats('cumulative')  # profiling
    ps.print_stats()

    pp.pprint(errors)
    pp.pprint(all_wrtitten_reccount)
    if args.stats_file:
        for name, value in all_wrtitten_reccount.iteritems():
            args.stats_file.write(name + " " + str(value) + "\n")

    del(pymongo_request_processing)
    del(mongo_rec_multiprocessing)
    exit(mongo_reader.failed)

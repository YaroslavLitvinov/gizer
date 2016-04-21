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
from collections import namedtuple
from mongo_schema import schema_engine
from mongo_schema.schema_engine import create_tables_load_bson_data
from gizer.opcsv import CsvManager
from gizer.opcsv import NULLVAL
from gizer.opcreate import generate_create_table_statement
from gizer.opcreate import generate_drop_table_statement
from gizer.opinsert import generate_insert_queries
from gizer.opinsert import table_rows_list
from gizer.opinsert import index_columns_as_dict
from gizer.opinsert import apply_indexes_to_table_rows
from gizer.opmultiprocessing import FastQueueProcessor


CSV_CHUNK_SIZE = 1024 * 1024 * 100  # 100MB
ETL_PROCESS_NUMBER = 10
ETL_QUEUE_SIZE = 10
GET_QUEUE_SIZE = 30

TablesToSave = namedtuple('TablesToSave', 
                          ['rows', 'index_keys', 'indexes', 'errors'])

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


def create_table_queries(schema_engine, psql_schema_name, table_prefix):
    res = {}
    sqltables = create_tables_load_bson_data(schema_engine, None).tables
    for tablename, sqltable in sqltables.iteritems():
        res[tablename] = create_table(sqltable, psql_schema_name,
                                      table_prefix)
    return res

def save_csvs(csm, tables_to_save):
    if not hasattr(save_csvs, "max_indexes"):
        save_csvs.max_indexes = {}

    written = {}
    for table_name in tables_to_save.rows:
        rows = apply_indexes_to_table_rows(tables_to_save.rows[table_name],
                                           tables_to_save.index_keys[table_name],
                                           save_csvs.max_indexes)
        written[table_name] = csm.write_csv(table_name, rows)
# cache initial indexes
    save_csvs.max_indexes = merge_dicts(save_csvs.max_indexes,
                                        tables_to_save.indexes)
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
    rows_as_dict = {}
    index_keys = {}
    tables_obj = create_tables_load_bson_data(schema, [rec])
    for table_name, table in tables_obj.tables.iteritems():
        rows = table_rows_list(table, False, null_value = NULLVAL)
        rows_as_dict[table_name] = rows
        index_keys[table_name] = index_columns_as_dict(table)
    return TablesToSave(rows = rows_as_dict,
                        index_keys = index_keys,
                        indexes = tables_obj.data_engine.indexes,
                        errors = tables_obj.errors)

# Fast queue helpers

def request_mongo_recs_async(fastqueue3):
    records = []
    last_record = None
    # wait while queue size is not exceeded or if result available
    while fastqueue3.count() >= GET_QUEUE_SIZE \
            or fastqueue3.poll() \
            or (not len(records) and fastqueue3.count()):
        rec = fastqueue3.get()
        if rec:
            records.append(rec)
        else:
            last_record = True
    if not last_record:
        for i in xrange(len(records)):
            if records[i]:
                fastqueue3.put('foo')
    get_all = fastqueue3.count() \
        or fastqueue3.poll() or fastqueue3.is_any_working()
    while last_record and get_all:
        rec = fastqueue3.get()
        if rec:
            records.append(rec)
        else:
            break
    return records

def get_tables_from_rec_async(fastqueue, records):
    finish = False
    res = []
    for rec in records:
        if rec:
            fastqueue.put(rec)
    get_all = fastqueue.count() or fastqueue.poll() or fastqueue.is_any_working()
    while fastqueue.count() >= ETL_QUEUE_SIZE or fastqueue.poll() \
            or (not len(records) and get_all and not finish):
        async_res = fastqueue.get()
        if async_res:
            res.append(async_res)
        else:
            finish = True
    return res


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

    sqltables = create_tables_load_bson_data(schema, None).tables
    table_names = sqltables.keys()

    csm = CsvManager(table_names, args.csv_path, CSV_CHUNK_SIZE)
    pp = pprint.PrettyPrinter(indent=4)
    errors = {}
    all_written = {}
    # create pymongo retriever to be used as parallel process
    pymongo_request_processing \
        = FastQueueProcessor(worker_retrieve_mongo_record, mongo_reader, 1) 
    mongo_rec_multiprocessing \
        = FastQueueProcessor(worker_handle_mongo_record, 
                             schema, 
                             ETL_PROCESS_NUMBER)
    c=0
    # form a queue for fast retrieving of data
    for i in range(GET_QUEUE_SIZE):
        pymongo_request_processing.put('foo')
    try:
        records = request_mongo_recs_async(pymongo_request_processing)
        while True:
#            print "loop.a", len(records), pymongo_request_processing.count()
            # tables_to_save is [TablesToSave]
            tables_to_save = get_tables_from_rec_async(mongo_rec_multiprocessing,
                                                    records)
#            print "loop.b", len(tables_to_save), mongo_rec_multiprocessing.count()
            c+=len(records)
            for table_to_save in tables_to_save:
                all_written = merge_dicts(all_written, 
                                          save_csvs(csm, table_to_save))
                errors = merge_dicts(errors, table_to_save.errors)

            if not len(records):
                break
            del records[:]
            records = request_mongo_recs_async(pymongo_request_processing)

    except KeyboardInterrupt:
        mongo_reader.failed = True

# save create table statements
    create_statements \
        = create_table_queries(schema, psql_schema_name, table_prefix)
    for table_name in create_statements:
        create_query = create_statements[table_name]
        args.ddl_statements_file.write(create_query)

# save csv files
    csm.finalize()
    message("")
    pr.disable()
    ps = pstats.Stats(pr).sort_stats('cumulative')  # profiling
    ps.print_stats()

    pp.pprint(errors)
    pp.pprint(all_written)
    print "Etl records count should be =", c
    if args.stats_file:
        for name, value in all_written.iteritems():
            args.stats_file.write(name + " " + str(value) + "\n")

    del(pymongo_request_processing)
    del(mongo_rec_multiprocessing)
    exit(mongo_reader.failed)

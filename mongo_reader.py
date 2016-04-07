#!/usr/bin/env python

"""Read data from mongo collection"""

import pprint
import time
import cProfile, pstats # profiling
# modules affected in data in
import sys
import json
import argparse
import pymongo
from pymongo.mongo_client import MongoClient
# modules affected in data out
import os
from mongo_schema import schema_engine
from gizer.opcsv import CsvManager
from gizer.opcreate import generate_create_table_statement
from gizer.opcreate import generate_drop_table_statement
from gizer.opinsert import generate_insert_queries


CSV_CHUNK_SIZE = 1024*1024*100 # 100MB

def message(mes, cr='\n'):
    sys.stderr.write( mes + cr)

class MongoReader:
    def __init__(self, ssl, host, port, dbname, collection, \
                 user, passw, request):
        self.ssl = ssl
        self.host = host
        self.port = int(port)
        self.dbname = dbname
        self.collection = collection
        self.user = user
        self.passw = passw
        self.request = request
        self.rec_i = 0
        self.cursor = None
        self.client = None
        self.failed = False
        self.attempts = 0

    def connauthreq(self):
        self.client = MongoClient(self.host, self.port, ssl=self.ssl)
        if self.user and self.passw:
            self.client[self.dbname].authenticate(self.user, self.passw)
            message("Authenticated")
        mongo_collection = self.client[self.dbname][self.collection]
        self.cursor = mongo_collection.find(self.request)
        self.cursor.batch_size(1000)
        return self.cursor

    def nextrec(self):
        if not self.cursor:
            self.connauthreq()

        self.attempts = 0
        rec = None
        while self.cursor.alive and self.failed is False:
            try:
                rec = self.cursor.next()
                self.rec_i += 1
            except pymongo.errors.AutoReconnect:
                self.attempts += 1
                if self.attempts <= 4:
                    time.sleep(pow(2, self.attempts))
                    message("Connect attempt #%d, %s" % (self.attempts, str(time.time())))
                    continue
                else:
                    self.failed = True
            except pymongo.errors.OperationFailure:
                self.failed = True
                message("Exception: pymongo.errors.OperationFailure")
            break
        return rec


def create_table(sqltable, psql_schema_name, psql_table_name_prefix):
    drop = generate_drop_table_statement(sqltable, psql_schema_name, 
                                         psql_table_name_prefix)
    create = generate_create_table_statement(sqltable, psql_schema_name, 
                                             psql_table_name_prefix)
    return drop + '\n' + create + '\n';

def merge_dicts(store, append):
    for index_key, index_val in append.iteritems():
        cached_val = 0
        if index_key in store:
            cached_val = store[index_key]
        store[index_key] = index_val + cached_val
    return store


def create_table_queries(sqltables, psql_schema_name, psql_table_name_prefix):
    if not hasattr(create_table_queries, "created_tables"):
        create_table_queries.created_tables = {}

    for tablename, sqltable in sqltables.iteritems():
        if tablename not in create_table_queries.created_tables:
            create_table_queries.created_tables[tablename] = \
                create_table(sqltable, psql_schema_name, psql_table_name_prefix)


def gen_insert_queries(tables_obj, csm):
    if not hasattr(gen_insert_queries, "max_indexes"):
        gen_insert_queries.max_indexes = {}

    written = {}
    for name, table in tables_obj.tables.iteritems():
        reccount = csm.write_csv(table)
        written[name] = reccount
    return written

#cache initial indexes
    gen_insert_queries.max_indexes = \
        merge_dicts(gen_insert_queries.max_indexes, tables_obj.data_engine.indexes)


if __name__ == "__main__":
    
    default_request = '{}'

    parser = argparse.ArgumentParser()
    parser.add_argument("-ssl", action="store_true", help="connect to ssl port")
    parser.add_argument("--host", help="Mongo db host:port", type=str, required=True)
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
    parser.add_argument("--hdfs-path", help="Hdfs path (at least 3 letters) to save \
folders with csv files", type=str, required=True)
    parser.add_argument("--tmp-path", help="tmp path", type=str, required=True)

    args = parser.parse_args()

    if len(args.hdfs_path) < 3:
        message("--hdfs-path param should be at least tree chars length")
        exit(1)

    split_name = args.collection_name.split('.')
    if len(split_name) != 2:
        message("collection name is expected in format db_name.collection_name")
        exit(1)

    message("Connecting to mongo server "+args.host)
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

    pr = cProfile.Profile() #profiling
    pr.enable() #profiling

    search_request = json.loads(args.js_request)

    js_schema = [json.load(args.input_file_schema)]
    schema = schema_engine.SchemaEngine(collection_name, js_schema)
    mongo_reader = MongoReader(args.ssl, host, port, \
                                   dbname, collection_name, \
                                   args.user, args.passw, search_request)

    psql_schema_name = args.psql_schema_name
    if not args.psql_schema_name:
        psql_schema_name = ''

    psql_table_name_prefix = args.psql_table_name_prefix
    if not args.psql_table_name_prefix:
        psql_table_name_prefix = ''

    csm = CsvManager(args.tmp_path, args.hdfs_path, CSV_CHUNK_SIZE)
    pp = pprint.PrettyPrinter(indent=4)
    errors = {}
    all_wrtitten_reccount = {}
    count = None
    rec = True
    try:
        while rec:
            rec = mongo_reader.nextrec()
            if count is None:
                count = mongo_reader.cursor.count()
            if rec:
                tables_obj = schema_engine.create_tables_load_bson_data(schema, [rec])
                create_table_queries(tables_obj.tables, psql_schema_name, psql_table_name_prefix)
                written = gen_insert_queries(tables_obj, csm)
                all_wrtitten_reccount = merge_dicts(all_wrtitten_reccount, written)
                errors = merge_dicts(errors, tables_obj.errors)
                message(".", cr="")
                if mongo_reader.rec_i % 1000 == 0:
                    message("\n%d of %d" % (mongo_reader.rec_i, count))
    except KeyboardInterrupt:
        mongo_reader.failed = True

#save create table statements    
    for table_name, create_query in \
            create_table_queries.created_tables.iteritems():
        args.ddl_statements_file.write(create_query)
#save csv files
    csm.finalize()
    message("")
    pr.disable()
    ps = pstats.Stats(pr).sort_stats('cumulative') #profiling
    ps.print_stats()

    pp.pprint(errors)
    pp.pprint(all_wrtitten_reccount)
    if args.stats_file:
        for name, value in all_wrtitten_reccount.iteritems():
            args.stats_file.write(name+" "+str(value)+"\n")

    exit(mongo_reader.failed)

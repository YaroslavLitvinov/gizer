#!/usr/bin/env python

"""Read data from mongo collection"""

import cProfile, pstats #profiling
import pprint

import sys
import json
import argparse
from pymongo.mongo_client import MongoClient
# modules or psql requests
import os
import psycopg2
from mongo_to_hive_mapping import schema_engine
from gizer.psql_requests import PsqlRequests
from gizer.opcreate import generate_create_table_statement
from gizer.opcreate import generate_drop_table_statement
from gizer.opinsert import generate_insert_queries

def message(mes, cr='\n'):
    sys.stderr.write( mes + cr)

class MongoReader:
    def __init__(self, host, port, dbname, collection, \
                 user, passw, request):
        self.host = host
        self.port = int(port)
        self.dbname = dbname
        self.collection = collection
        self.user = user
        self.passw = passw
        self.request = request
        self.rec_i = 0
        self.cursor = None
        self.failed = False
        self.attempts = 0

    def connauthreq(self):
        client = MongoClient(self.host, self.port)
        if self.user and self.passw:
            client[self.dbname].authenticate(self.user, self.passw)
            message("Authenticated")
        mongo_collection = client[self.dbname][self.collection]
        self.cursor = mongo_collection.find(self.request)
        self.cursor.batch_size(1000)
        return self.cursor

    def nextrec(self):
        if not self.cursor:
            message("Connect attempt #%d" % (self.attempts))
            self.connauthreq()

        if self.rec_i < self.cursor.count():
            try:
                rec = self.cursor[self.rec_i]
                self.rec_i += 1
            except:
                self.cursor.close()
                self.cursor = None
                self.attempts += 1
                if self.attempts <= 3:
                    rec = self.nextrec()
                else:
                    self.failed = True
                    rec = None
            return rec
        else:
            return None


def create_table(dbreq, sqltable, psql_schema_name):
    drop_table = generate_drop_table_statement(sqltable, psql_schema_name)
    print drop_table
    dbreq.cursor.execute(drop_table)
    create_table = generate_create_table_statement(sqltable, psql_schema_name)
    print create_table
    dbreq.cursor.execute(create_table)

def merge_dicts(store, append):
    for index_key, index_val in append.iteritems():
        cached_val = 0
        if index_key in store:
            cached_val = store[index_key]
        store[index_key] = index_val + cached_val
    return store


def make_psql_requests(dbreq, tables_obj, psql_schema_name, use_cached):
    if not hasattr(make_psql_requests, "created_tables"):
        make_psql_requests.created_tables = {}

    if psql_schema_name is None:
        psql_schema_name = ""
    
    if use_cached is True:
        if hasattr(make_psql_requests, "indexes"):
            indexes = make_psql_requests.indexes
        else:
            indexes = make_psql_requests.indexes = {}
    tables = tables_obj.tables
    for table in tables:
        if tables[table].table_name not in make_psql_requests.created_tables:
            create_table(dbreq, tables[table], psql_schema_name)
            make_psql_requests.created_tables[tables[table].table_name] = 1
        if use_cached is not True:
            indexes = dbreq.get_table_max_indexes(tables[table], psql_schema_name)
        inserts = generate_insert_queries(tables[table], psql_schema_name, \
                                              initial_indexes = indexes)
        for query in inserts[1]:
            try:
                dbreq.cursor.execute(inserts[0], query)
            except:
                print inserts[0]
                print query
                raise

#cache initial indexes
    if use_cached is True:
        make_psql_requests.indexes \
            = merge_dicts(make_psql_requests.indexes,
                          tables_obj.data_engine.indexes)


if __name__ == "__main__":
    
    default_request = '{}'

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", help="Mongo db host:port", type=str)
    parser.add_argument("-user", help="Mongo db user", type=str)
    parser.add_argument("-passw", help="Mongo db pass", type=str)
    parser.add_argument("-cn", "--collection-name", help="Mongo collection name that is expected in format db_name.collection_name", type=str)
    parser.add_argument("-ifs", "--input-file-schema", action="store",
                        help="Input file with json schema", type=file)
    parser.add_argument("-js-request", help='Mongo db search request in json format. default=%s' % (default_request), type=str)
    parser.add_argument("-psql-schema-name", help="", type=str)
    parser.add_argument("-use-cached-indexes", action="store_true", help="Use cached indexes instead making get_max_index request from psql db")


    args = parser.parse_args()

    if args.host == None or args.collection_name == None:
        parser.print_help()
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
    mongo_reader = MongoReader(host, port, dbname, collection_name, \
                           args.user, args.passw, search_request)

    connstr = os.environ['PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))

    pp = pprint.PrettyPrinter(indent=4)
    errors = {}
    first = True
    while True:
        rec = mongo_reader.nextrec()
        if mongo_reader.failed == True or rec is None:
            message("")
            break
        else:
#            print rec
            tables_obj = schema_engine.create_tables_load_bson_data(schema, [rec])
            make_psql_requests(dbreq, tables_obj, args.psql_schema_name, 
                               args.use_cached_indexes )
            errors = merge_dicts(errors, tables_obj.errors)
            first = False
            message(".", cr="")

    dbreq.cursor.execute('COMMIT')
    pr.disable()
    ps = pstats.Stats(pr).sort_stats('cumulative') #profiling
    ps.print_stats()

    pp.pprint(errors)

    exit(mongo_reader.failed)

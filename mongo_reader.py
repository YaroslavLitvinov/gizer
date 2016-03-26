#!/usr/bin/env python

"""Read data from mongo collection"""

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
        return self.cursor

    def nextrec(self):
        if not self.cursor:
            message("Connect attempt #%d" % (self.attempts+1))
            self.connauthreq()

        if self.rec_i < self.cursor.count():
            try:
                rec = self.cursor[self.rec_i]
                self.rec_i += 1
            except:
                self.cursor.close()
                self.cursor = None
                self.attempts += 1
                if self.attempts < 3:
                    rec = self.nextrec()
                else:
                    self.failed = True
                    rec = None
            return rec
        else:
            return None


def make_psql_requests(dbreq, schema, rec):
    tables = schema_engine.create_tables_load_bson_data(schema, [rec]).tables
    for table in tables:
        create_table = generate_create_table_statement(tables[table])
        print create_table
        dbreq.cursor.execute(create_table)
        indexes = dbreq.get_table_max_indexes(tables[table])
        inserts = generate_insert_queries(tables[table], initial_indexes = indexes)
        for query in inserts[1]:
            dbreq.cursor.execute(inserts[0], query)
        dbreq.cursor.execute('COMMIT')


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
    search_request = json.loads(args.js_request)

    js_schema = [json.load(args.input_file_schema)]
    schema = schema_engine.SchemaEngine(collection_name, js_schema)
    print js_schema

    mongo_reader = MongoReader(host, port, dbname, collection_name, \
                           args.user, args.passw, search_request)

    connstr = os.environ['PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))

    while True:
        rec = mongo_reader.nextrec()
        if mongo_reader.failed == True or rec is None:
            message("")
            break
        else:
            print rec
            make_psql_requests(dbreq, schema, rec)
            message(".", cr="")

    exit(mongo_reader.failed)

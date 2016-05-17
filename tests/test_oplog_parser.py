#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import psycopg2
from collections import namedtuple
from gizer.psql_requests import PsqlRequests
from gizer.oplog_parser import create_truncate_psql_objects
from gizer.oplog_parser import sync_oplog
from gizer.oplog_parser import compare_psql_and_mongo_records
from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.psql_objects import insert_tables_data_into_dst_psql
from mongo_schema.schema_engine import create_tables_load_bson_data
from mock_mongo_reader import MongoReaderMock

# THis schema must be precreated before running tests
TMP_SCHEMA_NAME = 'operational'
MAIN_SCHEMA_NAME = ''

OplogTest = namedtuple('OplogTest', ['ts', 'before', 'oplog', 'after'])

def initial_load(dbreq, schemas_path, psql_schema):
    create_truncate_psql_objects(dbreq, schemas_path, psql_schema)

def mongo_reader_mock(mongo_data_path):
    mongo_reader = None
    with open(mongo_data_path) as opfile:
        posts_data = opfile.read()
        mongo_reader = MongoReaderMock(posts_data)
        opfile.close()
    return mongo_reader

def oplog_reader_mock(oplog_data_path):
    oplog_reader = None
    with open(oplog_data_path) as opfile:
        oplog_data = opfile.read()
        oplog_reader = MongoReaderMock(oplog_data)
        opfile.close()
    return oplog_reader


def load_mongo_data_to_psql(mongo_reader, psql, psql_schema):
    rec = mongo_reader.next()
    while rec:
        tables = create_tables_load_bson_data(schema_engine, None)
        tables.load_external_tables_data(rec)
        insert_tables_data_into_dst_psql(psql, tables, psql_schema, '')
        rec = mongo_reader.next()


def check_oplog_sync(oplog_test):
    connstr = os.environ['TEST_PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))

    schemas_path = "./test_data/schemas/rails4_mongoid_development"
    oplog_reader = oplog_reader_mock(oplog_test.oplog)
    # do initial load into main schema (not operational)
    for name, mongo_data_path in oplog_test.before.iteritems():
        mongo_reader_before_data = mongo_reader_mock(mongo_data_path)
        load_mongo_data_to_psql(mongo_reader_before_data, dbreq, MAIN_SCHEMA_NAME)
    mongo_readers = {}
    for name, mongo_data_path in oplog_test.after.iteritems():
        mongo_readers[name] = mongo_reader_mock(mongo_data_path)
    # oplog_ts_to_test is timestamp starting from which oplog records
    # should be applied to psql tables to locate ts which corresponds to
    # initially loaded psql data;
    # None - means oplog records should be tested starting from beginning
    oplog_ts_to_test = oplog_test.ts
    psql_schema_to_apply_ops = TMP_SCHEMA_NAME
    psql_schema_initial_load = MAIN_SCHEMA_NAME
    initial_load(dbreq, schemas_path, MAIN_SCHEMA_NAME)
    sync_res = sync_oplog(oplog_ts_to_test, 
                          dbreq, 
                          mongo_readers, 
                          oplog_reader,
                          schemas_path, psql_schema_to_apply_ops,
                          psql_schema_initial_load)
    while True:
        if sync_res is False or sync_res is True:
            break
        else:
            oplog_ts_to_test = sync_res
        sync_res = sync_oplog(oplog_ts_to_test, dbreq, mongo_reader,
                              oplog_reader, schemas_path,
                              psql_schema_to_apply_ops,
                              psql_schema_initial_load)
    return sync_res

def test_oplog_sync():
    oplog_test1 \
        = OplogTest(None, 
                    {'posts': 'test_data/oplog1/before_collection_posts.js'},
                    'test_data/oplog1/oplog.js',
                    {'posts': 'test_data/oplog1/after_collection_posts.js'})
    res = check_oplog_sync(oplog_test1)
    assert(res == True)
    # temporarily disabled tests
    #res = check_oplog_sync('6249008760904220673')
    #assert(res == True)
    #res = check_oplog_sync('6249012068029138000')
    #assert(res == False)

def test_compare_empty_compare_psql_and_mongo_records():
    connstr = os.environ['TEST_PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))
    empty_mongo = 'test_data/oplog1/before_collection_posts.js'
    mongo_reader = mongo_reader_mock(empty_mongo)
    schemas_path = "./test_data/schemas/rails4_mongoid_development"
    schema_engines = get_schema_engines_as_dict(schemas_path)

    #cmpare non existing record
    res = compare_psql_and_mongo_records(
        dbreq, mongo_reader, schema_engines['posts'], 
        "111111111111111111111110", TMP_SCHEMA_NAME)
    assert(res == True)


if __name__ == '__main__':
    test_oplog_sync()

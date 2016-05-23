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
from gizer.oplog_parser import do_oplog_sync
from gizer.oplog_parser import compare_psql_and_mongo_records
from gizer.oplog_parser import EMPTY_TS
from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.psql_objects import insert_tables_data_into_dst_psql
from mongo_schema.schema_engine import create_tables_load_file
from mock_mongo_reader import MongoReaderMock

# THis schema must be precreated before running tests
TMP_SCHEMA_NAME = 'operational'
MAIN_SCHEMA_NAME = ''

OplogTest = namedtuple('OplogTest', ['ts', 'before', 'oplog', 'after'])

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


def load_mongo_data_to_psql(schema_engine, mongo_data_path, psql, psql_schema):
    tables = create_tables_load_file(schema_engine, mongo_data_path)
    insert_tables_data_into_dst_psql(psql, tables, psql_schema, '')
    psql.cursor.execute('COMMIT')

def check_oplog_sync_point(oplog_test):
    connstr = os.environ['TEST_PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))

    psql_schema_to_apply_ops = TMP_SCHEMA_NAME
    psql_schema_initial_load = MAIN_SCHEMA_NAME

    schemas_path = "./test_data/schemas/rails4_mongoid_development"
    schema_engines = get_schema_engines_as_dict(schemas_path)
    oplog_reader = oplog_reader_mock(oplog_test.oplog)

    create_truncate_psql_objects(dbreq, schemas_path, psql_schema_to_apply_ops)
    dbreq.cursor.execute('COMMIT')
    for name, mongo_data_path in oplog_test.before.iteritems():
        load_mongo_data_to_psql(schema_engines[name],
                                mongo_data_path,
                                dbreq, psql_schema_to_apply_ops)
    # do initial load into main schema (not operational)
    create_truncate_psql_objects(dbreq, schemas_path, psql_schema_initial_load)
    dbreq.cursor.execute('COMMIT')
    for name, mongo_data_path in oplog_test.before.iteritems():
        load_mongo_data_to_psql(schema_engines[name],
                                mongo_data_path,
                                dbreq, psql_schema_initial_load)

    mongo_readers_after = {}
    for name, mongo_data_path in oplog_test.after.iteritems():
        mongo_readers_after[name] = mongo_reader_mock(mongo_data_path)

    res = do_oplog_sync(oplog_test.ts, dbreq, 
                        psql_schema_to_apply_ops, 
                        psql_schema_initial_load,
                        oplog_reader, mongo_readers_after, schemas_path)
    if res:
        return True
    else:
        return False

def test_oplog_sync():

    oplog_test1 \
        = OplogTest(None,
                    {'posts': 'test_data/oplog1/before_collection_posts.js',
                     'guests': 'test_data/oplog1/before_collection_guests.js'},
                    'test_data/oplog1/oplog.js',
                    {'posts': 'test_data/oplog1/after_collection_posts.js',
                     'guests': 'test_data/oplog1/after_collection_guests.js'})
    res = check_oplog_sync_point(oplog_test1)
    assert(res == True)

    oplog_test2 \
        = OplogTest('6249008760904220673',
                    {'posts': 'test_data/oplog2/before_collection_posts.js',
                     'guests': 'test_data/oplog2/before_collection_guests.js'},
                    'test_data/oplog2/oplog.js',
                    {'posts': 'test_data/oplog2/after_collection_posts.js',
                     'guests': 'test_data/oplog2/after_collection_guests.js'})
    res = check_oplog_sync_point(oplog_test2)
    assert(res == True)


    # oplog_test2 \
    #     = OplogTest('6249012068029138000',
    #                 {'posts': 'test_data/oplog2/before_collection_posts.js',
    #                  'guests': 'test_data/oplog2/before_collection_guests.js'},
    #                 'test_data/oplog2/oplog.js',
    #                 {'posts': 'test_data/oplog2/after_collection_posts.js',
    #                  'guests': 'test_data/oplog2/after_collection_guests.js'})
    # res = check_oplog_sync_point(oplog_test2)
    # assert(res == True)

    # temporarily disabled tests
    #res = check_oplog_sync_point('6249008760904220673')
    #assert(res == True)
    #res = check_oplog_sync_point('6249012068029138000')
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

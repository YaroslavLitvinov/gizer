#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import sys
import psycopg2
import logging
from collections import namedtuple
from gizer.psql_requests import PsqlRequests
from gizer.oplog_highlevel import OplogHighLevel
from gizer.oplog_highlevel import compare_psql_and_mongo_records
from gizer.oplog_parser import EMPTY_TS
from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.psql_objects import insert_tables_data_into_dst_psql
from gizer.psql_objects import create_truncate_psql_objects
from mongo_schema.schema_engine import create_tables_load_file
from mock_mongo_reader import MongoReaderMock

SCHEMAS_PATH = "./test_data/schemas/rails4_mongoid_development"
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


def check_oplog_sync_point(oplog_test, schemas_path):
    connstr = os.environ['TEST_PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))

    psql_schema = MAIN_SCHEMA_NAME

    schema_engines = get_schema_engines_as_dict(schemas_path)
    oplog_reader = oplog_reader_mock(oplog_test.oplog)

    create_truncate_psql_objects(dbreq, schemas_path, psql_schema)
    dbreq.cursor.execute('COMMIT')
    for name, mongo_data_path in oplog_test.before.iteritems():
        load_mongo_data_to_psql(schema_engines[name],
                                mongo_data_path, dbreq, psql_schema)

    mongo_readers_after = {}
    for name, mongo_data_path in oplog_test.after.iteritems():
        mongo_readers_after[name] = mongo_reader_mock(mongo_data_path)

    ohl = OplogHighLevel(dbreq, mongo_readers_after, oplog_reader,
                 schemas_path, schema_engines, psql_schema)

    res = ohl.do_oplog_sync(oplog_test.ts)
    if res:
        return True
    else:
        return False


def test_oplog_sync():
    print('\ntest#1')
    oplog_test1 \
        = OplogTest(None,
                    {'posts': 'test_data/oplog1/before_collection_posts.js',
                     'guests': 'test_data/oplog1/before_collection_guests.js'},
                    'test_data/oplog1/oplog.js',
                    {'posts': 'test_data/oplog1/after_collection_posts.js',
                     'guests': 'test_data/oplog1/after_collection_guests.js'})
    res = check_oplog_sync_point(oplog_test1, SCHEMAS_PATH)
    assert(res == True)
    

    print('\ntest#2')
    oplog_test2 \
        = OplogTest("Timestamp(1164278289, 1))",
                    {'posts': 'test_data/oplog2/before_collection_posts.js',
                     'guests': 'test_data/oplog2/before_collection_guests.js'},
                    'test_data/oplog2/oplog.js',
                    {'posts': 'test_data/oplog2/after_collection_posts.js',
                     'guests': 'test_data/oplog2/after_collection_guests.js'})
    res = check_oplog_sync_point(oplog_test2, SCHEMAS_PATH)
    assert(res == True)

    print('\ntest#3')
    oplog_test3 \
        = OplogTest("Timestamp(1000000001, 1))",
                    {'posts': 'test_data/oplog3/before_collection_posts.js',
                     'guests': 'test_data/oplog3/before_collection_guests.js',
                     'posts2': 'test_data/oplog3/before_collection_posts2.js',
                     'rated_posts': 'test_data/oplog3/before_collection_rated_posts.js'
                     },
                    'test_data/oplog3/oplog.js',
                    {'posts': 'test_data/oplog3/after_collection_posts.js',
                     'guests': 'test_data/oplog3/after_collection_guests.js',
                     'posts2': 'test_data/oplog3/after_collection_posts2.js',
                     'rated_posts': 'test_data/oplog3/after_collection_rated_posts.js'
                     })
    res = check_oplog_sync_point(oplog_test3, SCHEMAS_PATH)
    assert(res == True)

    # oplog_test4 \
    #     = OplogTest("Timestamp(1364278289, 1))",
    #                 {'posts': 'test_data/oplog2/before_collection_posts.js',
    #                  'guests': 'test_data/oplog2/before_collection_guests.js'},
    #                 'test_data/oplog2/oplog.js',
    #                 {'posts': 'test_data/oplog2/after_collection_posts.js',
    #                  'guests': 'test_data/oplog2/after_collection_guests.js'})
    # res = check_oplog_sync_point(oplog_test4)
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
    """ Test external data by providing path to schemas folder, 
    data folder as args """
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    schemas_path = sys.argv[1]
    data_path = sys.argv[2]
    mongo_oplog = os.path.join(data_path, 'mongo_oplog.json')

    empty_data_before = {}
    data_after = {}
    schema_engines = get_schema_engines_as_dict(schemas_path)
    for schema_name in schema_engines:
        path_with_data = os.path.join(data_path, 'mongo_%s.json' % schema_name)
        data_after[schema_name] = path_with_data
    oplog_test1 \
        = OplogTest(None, 
                    empty_data_before,
                    mongo_oplog,
                    data_after)
    res = check_oplog_sync_point(oplog_test1, schemas_path)
    assert(res == True)

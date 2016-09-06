#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import psycopg2
import bson
import logging
from logging import getLogger
from bson.json_util import loads
#from datetime import datetime
import datetime
#from gizer.oppartial_record import get_record_with_data
from gizer.oppartial_record import get_tables_data_from_oplog_set_command
from gizer.opinsert import generate_insert_queries
from gizer.psql_objects import create_psql_tables
from gizer.psql_objects import load_single_rec_into_tables_obj
from gizer.psql_objects import insert_tables_data_into_dst_psql
from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.psql_requests import PsqlRequests
from mongo_schema.schema_engine import create_tables_load_bson_data

def test_complete_partial_record():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(message)s')

    object_id_bson_raw_data = '{\
"_id": { "$oid": "56b8da59f9fcee1b00000007" }\
}'
    array_bson_raw_data = '{\
    "comments": [{\
    "_id": {"$oid": "56b8f344f9fcee1b00000018"},\
    "updated_at": { "$date" : "2016-02-08T19:57:56.678Z"},\
    "created_at": { "$date" : "2016-02-08T19:57:56.678Z"}\
    }\
]}'
    sample_data = {
        'post_comments': {
            'id_oid': [str(loads('{ "$oid": "56b8f344f9fcee1b00000018" }'))],
            'updated_at': [loads('{ "$date" : "2016-02-08T19:57:56.678Z"}')],
            'created_at': [loads('{ "$date" : "2016-02-08T19:57:56.678Z"}')]
        }
    }

    dbname = 'rails4_mongoid_development'
    db_schemas_path = '/'.join(['test_data', 'schemas', dbname])
    schemas = get_schema_engines_as_dict(db_schemas_path)
    schema_engine = schemas['posts']
    
    bson_data = loads(array_bson_raw_data)
    object_id_bson_data = loads(object_id_bson_raw_data)
    partial_inserts_list = get_tables_data_from_oplog_set_command(\
        schema_engine, bson_data, object_id_bson_data)
    tables = partial_inserts_list[0].tables
    print tables
    for table_name in tables:
        assert(True==tables[table_name].compare_with_sample(sample_data))

def test_complete_partial_record2():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(message)s')

    PSQL_SCHEMA_NAME = ''
    # etalon of data
    sample_data_before = {
        'posts2': {
            'id': [133],
            "updated_at": [loads('{ "$date" : "2016-02-08T20:02:12.985Z"}')]
        },
        'posts2_comments': {
            'idx': [1,2],
            'id_oid': [str(loads('{ "$oid": "56b8f35ef9fcee1b0000001a" }')),
                       str(loads('{ "$oid": "56b8f344f9fcee1b00000018" }'))],
            'updated_at': [loads('{ "$date" : "2016-02-08T20:02:12.985Z"}'),
                           loads('{ "$date" : "2016-02-08T20:02:12.985Z"}')]
        },
        'posts2_comment_struct_tests': {
            'v': [1,2,3],
            'idx': [1,2,1]
        },
        'posts2_comment_struct_test_nested': {
            'nested': [20,23,24,25,26],
            'idx': [1,1,2,1,2]
        }
    }

    
    wrong_raw_bson_data = '[{\
     "_id": 133,\
     "updated_at": { "$date" : "2016-02-08T20:02:12.985Z"},\
     "comments": "error in data"\
    }]'

    existing_raw_bson_data = '[{\
     "_id": 133,\
     "updated_at": { "$date" : "2016-02-08T20:02:12.985Z"},\
     "comments": [ {\
          "_id": { "$oid": "56b8f35ef9fcee1b0000001a" },\
          "updated_at": { "$date" : "2016-02-08T20:02:12.985Z"},\
          "struct" : {\
              "tests": [{\
                  "v": 1,\
                  "nested": [20]\
              }, {\
                  "v": 2,\
                  "nested": [23, 24]\
              }]}\
        }, {\
          "_id": { "$oid": "56b8f344f9fcee1b00000018" },\
          "updated_at": { "$date" : "2016-02-08T20:02:12.985Z"},\
          "struct" : {\
              "tests": [{\
                  "v": 3,\
                  "nested": [25, 26]\
              }]}\
        } ]\
    }]'

    oplog_object_id_bson_raw_data = '{"_id": 133}'
    # insert request should be created, to add 'tests' item
    oplog_path_array_bson_raw_data = '{\
"comments.0.struct.tests.0.nested.1": 21,\
"comments.2": { \
    "_id": { "$oid": "56b8f35ef9fcee1b0000001a" },\
    "updated_at": { "$date" : "2016-02-08T20:02:14.985Z"},\
    "struct": {\
        "tests": [{\
            "v": 12,\
            "nested": [30]\
         }, {\
            "v": 13,\
            "nested": [32, 31]\
         }\
    ]}}\
}'

    dbname = 'rails4_mongoid_development'
    db_schemas_path = '/'.join(['test_data', 'schemas', dbname])
    schemas = get_schema_engines_as_dict(db_schemas_path)
    schema_engine = schemas['posts2']

    connstr = os.environ['TEST_PSQLCONN']
    psql = PsqlRequests(psycopg2.connect(connstr))


    # test wrong bson data
    existing_bson_data = loads(wrong_raw_bson_data)
    tables_obj_before = \
        create_tables_load_bson_data(schema_engine, 
                                     existing_bson_data)
    assert(False==tables_obj_before.compare_with_sample(sample_data_before))



    # tables loaded from existing_raW_bson_data
    existing_bson_data = loads(existing_raw_bson_data)
    tables_obj_before = \
        create_tables_load_bson_data(schema_engine, 
                                     existing_bson_data)

    assert(False==tables_obj_before.compare_with_sample({}))
    assert(True==tables_obj_before.compare_with_sample(sample_data_before))

    # create table structure, drop existing
    create_psql_tables(tables_obj_before, psql, PSQL_SCHEMA_NAME, '', True)
    # insert data totables
    insert_tables_data_into_dst_psql(psql, tables_obj_before, PSQL_SCHEMA_NAME, '')

    # oplog path with indexes. insert array item
    bson_data = loads(oplog_path_array_bson_raw_data)
    object_id_bson_data = loads(oplog_object_id_bson_raw_data)
    partial_inserts_list = get_tables_data_from_oplog_set_command(\
        schema_engine, bson_data, object_id_bson_data)

    for partial_insert in partial_inserts_list:
        tables_for_insert = partial_insert.tables
        initial_indexes = partial_insert.initial_indexes

        for name, table in tables_for_insert.iteritems():
            query_tuple = generate_insert_queries(table, 
                                                  PSQL_SCHEMA_NAME, "", 
                                                  initial_indexes)
            for query in query_tuple[1]:
                getLogger(__name__).debug("EXECUTE: " + \
                                              str(query_tuple[0]) + str(query))
                psql.cursor.execute(query_tuple[0], query)

    # tables loaded from existing_bson_data
    rec_obj_id = object_id_bson_data['_id']
    tables_obj_after = load_single_rec_into_tables_obj(psql,
                                                       schema_engine,
                                                       PSQL_SCHEMA_NAME,
                                                       rec_obj_id)
    sample_data_after = sample_data_before
    sample_data_after['posts2_comments']['idx'].append(3)
    sample_data_after['posts2_comments']['id_oid'].append(\
        "56b8f35ef9fcee1b0000001a")
    sample_data_after['posts2_comments']['updated_at'].append(
        loads('{ "$date" : "2016-02-08T20:02:14.985Z"}'))
    sample_data_after['posts2_comment_struct_tests'] = {
            'v': [1,2,3,12,13],
            'idx': [1,2,1,1,2]
    }
    sample_data_after['posts2_comment_struct_test_nested'] = {
            'nested': [20,21,23,24,25,26,30,32,31],
            'idx': [1,2,1,2,1,2,1,1,2]
    }

    assert(False==tables_obj_after.compare_with_sample({}))
    assert(True==tables_obj_after.compare_with_sample(sample_data_after))


def test_complete_partial_record3():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(message)s')

    PSQL_SCHEMA_NAME = ''
    # etalon of data
    sample_data_before = {
        'posts': {
            'id_oid': ['56b8da59f9fcee1b00000007'],
            "updated_at": [loads('{ "$date" : "2016-02-08T20:02:12.985Z"}')]
        },
        'post_comments': {
            'id_oid': [str(loads('{ "$oid": "56b8f35ef9fcee1b0000001a" }')),
                       str(loads('{ "$oid": "56b8f344f9fcee1b00000018" }'))],
            'updated_at': [loads('{ "$date" : "2016-02-08T20:02:12.985Z"}'),
                           loads('{ "$date" : "2016-02-08T20:02:13.985Z"}')],
            'idx': [1,2]
        },
        'post_comment_tests': {
            'tests': [0,2],
            'idx': [1,2]
        }
    }

    existing_raw_bson_data = '[{\
     "_id": { "$oid": "56b8da59f9fcee1b00000007" },\
     "updated_at": { "$date" : "2016-02-08T20:02:12.985Z"},\
     "comments": [ {\
          "_id": { "$oid": "56b8f35ef9fcee1b0000001a" },\
          "updated_at": { "$date" : "2016-02-08T20:02:12.985Z"}\
        }, {\
          "_id": { "$oid": "56b8f344f9fcee1b00000018" },\
          "updated_at": { "$date" : "2016-02-08T20:02:13.985Z"},\
          "tests": [0,2]\
        } ]\
 }]'

    oplog_object_id_bson_raw_data = '{\
"_id": { "$oid": "56b8da59f9fcee1b00000007" }\
}'
    # insert request should be created, to add a record with only single field: updated_at
    oplog_path_array_bson_raw_data = '{"comments.2.updated_at": \
{ "$date" : "2016-02-08T20:02:14.985Z"}}'

    dbname = 'rails4_mongoid_development'
    db_schemas_path = '/'.join(['test_data', 'schemas', dbname])
    schemas = get_schema_engines_as_dict(db_schemas_path)
    schema_engine = schemas['posts']

    connstr = os.environ['TEST_PSQLCONN']
    psql = PsqlRequests(psycopg2.connect(connstr))

    # tables loaded from existing_raW_bson_data
    existing_bson_data = loads(existing_raw_bson_data)
    tables_obj_before = \
        create_tables_load_bson_data(schema_engine, 
                                     existing_bson_data)
    assert(True==tables_obj_before.compare_with_sample(sample_data_before))

    # create table structure, drop existing
    create_psql_tables(tables_obj_before, psql, PSQL_SCHEMA_NAME, '', True)
    # insert data totables
    insert_tables_data_into_dst_psql(psql, tables_obj_before, 
                                     PSQL_SCHEMA_NAME, '')

    # oplog path inserting just a field
    bson_data = loads(oplog_path_array_bson_raw_data)
    print bson_data
    object_id_bson_data = loads(oplog_object_id_bson_raw_data)
    partial_inserts_list = get_tables_data_from_oplog_set_command(\
        schema_engine, bson_data, object_id_bson_data)
    tables_for_insert = partial_inserts_list[0].tables
    initial_indexes = partial_inserts_list[0].initial_indexes
    print "tables_for_insert", tables_for_insert.keys()
    print "initial_indexes", initial_indexes
    insert_tests_t = tables_for_insert['post_comments']
    insert_query = generate_insert_queries(insert_tests_t, "", "", 
                                           initial_indexes)
    print "columns", insert_tests_t.sql_column_names
    print "insert_query=", insert_query
    for query in insert_query[1]:
        print insert_query[0], query
        psql.cursor.execute(insert_query[0], query)

    # tables loaded from existing_bson_data
    rec_obj_id = object_id_bson_data['_id']
    tables_obj_after = load_single_rec_into_tables_obj(psql,
                                                       schema_engine,
                                                       PSQL_SCHEMA_NAME,
                                                       rec_obj_id)
    sample_data_after = sample_data_before
    sample_data_after['post_comments']['idx'].append(3)
    sample_data_after['post_comments']['id_oid'].append(None)
    sample_data_after['post_comments']['updated_at'].append(
        loads('{ "$date" : "2016-02-08T20:02:14.985Z"}'))

    assert(True==tables_obj_after.compare_with_sample(sample_data_after))

def test_complete_partial_record4():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(message)s')

    object_id_bson_raw_data = '{\
"_id": { "$oid": "56b8da59f9fcee1b00000007" }\
}'
    array_bson_raw_data = '{\
"comments.0": {\
"_id": {"$oid": "56b8f344f9fcee1b00000018"},\
"updated_at": "2016-02-08T19:57:56.678Z",\
"created_at": "2016-02-08T19:57:56.678Z"}\
}'

    dbname = 'rails4_mongoid_development'
    db_schemas_path = '/'.join(['test_data', 'schemas', dbname])
    schemas = get_schema_engines_as_dict(db_schemas_path)
    schema_engine = schemas['posts']
    
    bson_data = loads(array_bson_raw_data)
    object_id_bson_data = loads(object_id_bson_raw_data)
    partial_inserts_list = get_tables_data_from_oplog_set_command(\
        schema_engine, bson_data, object_id_bson_data)
    tables = partial_inserts_list[0].tables
    assert(tables['post_comments'].sql_columns['posts_id_oid'].values[0]=="56b8da59f9fcee1b00000007")
    assert(tables['post_comments'].sql_columns['id_oid'].values[0]=="56b8f344f9fcee1b00000018")
    assert(tables['post_comments'].sql_columns['idx'].values[0]==1)
    assert(len(tables)==1)

if __name__=='__main__':
    test_complete_partial_record()
    test_complete_partial_record2()
    test_complete_partial_record4()
    test_complete_partial_record3()


#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import psycopg2
import bson
from bson.json_util import loads
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
    object_id_bson_raw_data = '{\
"_id": { "$oid": "56b8da59f9fcee1b00000007" }\
}'
    array_bson_raw_data = '{\
"comments": [{\
"_id": {"$oid": "56b8f344f9fcee1b00000018"},\
"updated_at": "2016-02-08T19:57:56.678Z",\
"created_at": "2016-02-08T19:57:56.678Z"}]\
}'

    dbname = 'rails4_mongoid_development'
    db_schemas_path = '/'.join(['test_data', 'schemas', dbname])
    schemas = get_schema_engines_as_dict(db_schemas_path)
    schema_engine = schemas['posts']
    
    bson_data = loads(array_bson_raw_data)
    object_id_bson_data = loads(object_id_bson_raw_data)
    tables_tuple = get_tables_data_from_oplog_set_command(\
        schema_engine, bson_data, object_id_bson_data)
    tables = tables_tuple[0]
    assert(tables['post_comments'].sql_columns['posts_id_oid'].values[0]=="56b8da59f9fcee1b00000007")
    assert(tables['post_comments'].sql_columns['id_oid'].values[0]=="56b8f344f9fcee1b00000018")
    assert(len(tables)==1)

def test_complete_partial_record2():
    MAIN_SCHEMA_NAME = ''
    existing_raw_bson_data = '[{\
     "_id": { "$oid": "56b8da59f9fcee1b00000007" },\
     "updated_at": { "$date" : "2016-02-08T20:02:12.985Z"},\
     "comments": [ {\
          "_id": { "$oid": "56b8f35ef9fcee1b0000001a" },\
          "updated_at": { "$date" : "2016-02-08T20:02:12.985Z"}\
        }, {\
          "_id": { "$oid": "56b8f344f9fcee1b00000018" },\
          "updated_at": { "$date" : "2016-02-08T20:02:12.985Z"},\
          "tests": [0,2]\
        } ]\
 }]'

    oplog_object_id_bson_raw_data = '{\
"_id": { "$oid": "56b8da59f9fcee1b00000007" }\
}'

    # insert request should be created, to add 'tests' item
    oplog_path_array_bson_raw_data = '{"comments.1.tests.2": 1000}'

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
    print tables_obj_before.tables.keys()
    table_tests_before = tables_obj_before.tables['post_comment_tests']
    # data before
    assert(table_tests_before.sql_columns['tests'].values[0]==0)
    assert(table_tests_before.sql_columns['tests'].values[1]==2)
    # indexes before
    assert(table_tests_before.sql_columns['idx'].values[0]==1)
    assert(table_tests_before.sql_columns['idx'].values[1]==2)
    # items count in items array
    assert(len(table_tests_before.sql_columns['idx'].values)==2)
    # create table structure, drop existing
    create_psql_tables(tables_obj_before, psql, MAIN_SCHEMA_NAME, '', True)
    # insert data totables
    insert_tables_data_into_dst_psql(psql, tables_obj_before, MAIN_SCHEMA_NAME, '')

    # oplog path with indexes
    bson_data = loads(oplog_path_array_bson_raw_data)
    object_id_bson_data = loads(oplog_object_id_bson_raw_data)
    tables_tuple = get_tables_data_from_oplog_set_command(\
        schema_engine, bson_data, object_id_bson_data)
    tables_for_insert = tables_tuple[0]
    initial_indexes = tables_tuple[1]
    print "tables_for_insert", tables_for_insert.keys()
    print "initial_indexes", initial_indexes
    insert_tests_t = tables_for_insert['post_comment_tests']
    insert_query = generate_insert_queries(insert_tests_t, "", "", initial_indexes)
    print "columns", insert_tests_t.sql_column_names
    print "insert_query=", insert_query
    for query in insert_query[1]:
        print insert_query[0], query
        psql.cursor.execute(insert_query[0], query)

    # tables loaded from existing_bson_data
    rec_obj_id = object_id_bson_data['_id']
    tables_obj_after = load_single_rec_into_tables_obj(psql,
                                                       schema_engine,
                                                       MAIN_SCHEMA_NAME,
                                                       rec_obj_id)
    table_tests_after = tables_obj_after.tables['post_comment_tests']
    # data after
    assert(table_tests_after.sql_columns['tests'].values[0]==0)
    assert(table_tests_after.sql_columns['tests'].values[1]==2)
    assert(table_tests_after.sql_columns['tests'].values[2]==1000)
    # indexes after
    assert(table_tests_after.sql_columns['idx'].values[0]==1)
    assert(table_tests_after.sql_columns['idx'].values[1]==2)
    assert(table_tests_after.sql_columns['idx'].values[2]==3)
    # items count in items array
    assert(len(table_tests_after.sql_columns['idx'].values)==3)


    # initial indexes used in mongo_schema is starting from 0
    # unless mongo oplog where indexes is starting from 1
    # assert(initial_indexes=={u'posts_comments': 1, u'posts_comments_tests': 2})
    # assert(insert_tests_t.sql_columns['tests'].values[0]==1000)
    # assert(insert_tests_t.sql_columns['idx'].values[0]==3)
    # assert(insert_tests_t.sql_columns['posts_comments_idx'].values[0]==2)
    # assert(len(insert_tests_t.sql_columns['idx'].values)==1)
    # assert(len(insert_tests_t.sql_columns['posts_comments_idx'].values)==1)
    # assert(len(tables_for_insert)==1)


def test_complete_partial_record3():
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
    tables_tuple = get_tables_data_from_oplog_set_command(\
        schema_engine, bson_data, object_id_bson_data)
    tables = tables_tuple[0]
    assert(tables['post_comments'].sql_columns['posts_id_oid'].values[0]=="56b8da59f9fcee1b00000007")
    assert(tables['post_comments'].sql_columns['id_oid'].values[0]=="56b8f344f9fcee1b00000018")
    assert(tables['post_comments'].sql_columns['idx'].values[0]==1)
    assert(len(tables)==1)

if __name__=='__main__':
    test_complete_partial_record()
    test_complete_partial_record2()
    test_complete_partial_record3()

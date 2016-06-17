#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import os
import pprint 
import psycopg2
from gizer.psql_requests import PsqlRequests
from mongo_schema.tests.test_schema_engine import get_schema_engine, get_schema_tables
from mongo_schema import schema_engine
from gizer.psql_objects import insert_tables_data_into_dst_psql
from gizer.psql_objects import load_single_rec_into_tables_obj

files = {'a_inserts': ('../test_data/opinsert/json_schema2.txt',
                       '../test_data/opinsert/bson_data2.txt'),
         'a_somethings': ('../test_data/opinsert/json_schema3.txt',
                          '../test_data/opinsert/bson_data3.txt')}

def get_schema_engine(collection_name):
    dirpath=os.path.dirname(os.path.abspath(__file__))
    schema_fname = files[collection_name][0]
    schema_path = os.path.join(dirpath, schema_fname)
    return schema_engine.create_schema_engine(collection_name, schema_path)

def get_schema_tables(schema_engine_obj):
    collection_name = schema_engine_obj.root_node.name
    dirpath=os.path.dirname(os.path.abspath(__file__))
    data_fname = files[collection_name][1]
    data_path = os.path.join(dirpath, data_fname)
    return schema_engine.create_tables_load_file(schema_engine_obj, \
                                                 data_path)

def collection_tables(collection_name):
    schema_engine = get_schema_engine(collection_name)
    tables = get_schema_tables(schema_engine)
    return tables

def test_tables():
    """ Test collection with id of ObjectId type """
    collection_name = "a_inserts"
    tables = collection_tables(collection_name)
    assert(tables.tables.keys() == ['a_insert_comment_items',
                                    'a_inserts',
                                    'a_insert_comments',
                                    'a_insert_comment_slugs'])
    pp = pprint.PrettyPrinter()
    pp.pprint(tables.errors)
    pp.pprint(tables.data_engine.indexes)
    expected_indexes = {'a_inserts': 1,
                        u'a_inserts_comments': 2,
                        u'a_inserts_comments_items': 2,
                        u'a_inserts_comments_slugs': 1}
    assert(tables.data_engine.indexes == expected_indexes)


def test_tables2():
    """ Test collection with id of INT type """
    collection_name = 'a_somethings'
    tables = collection_tables(collection_name)
    root_t = tables.tables[collection_name]
    assert('id' in root_t.sql_columns)
    assert(root_t.sql_columns['id'].values[0] == 777)
    comments_t = tables.tables['a_something_comments']
    assert('a_somethings_id' in comments_t.sql_columns)
    assert(comments_t.sql_columns['a_somethings_id'].values[0] == 777)
    items_t = tables.tables['a_something_comment_items']
    assert('a_somethings_id' in items_t.sql_columns)
    assert('customer_id_bsontype' in root_t.sql_columns)
    print root_t.sql_columns['customer_id_bsontype'].values
    assert(root_t.sql_columns['customer_id_bsontype'].values[0] == 7)
    assert(root_t.sql_columns['customer_id_oid'].values[0]\
               == '56b8f05cf9fcee1b00000000')

    #test load / unload data
    connstr = os.environ['TEST_PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))
    insert_tables_data_into_dst_psql(dbreq, tables, '','')
    
    loaded_tables = load_single_rec_into_tables_obj(dbreq, 
                                                    tables.schema_engine, 
                                                    '', 777)
    assert(tables.compare(loaded_tables)==True)


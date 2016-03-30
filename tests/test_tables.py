#!/usr/bin/env python

import pprint
import os
from mongo_to_hive_mapping.test_schema_engine import get_schema_engine, get_schema_tables
from mongo_to_hive_mapping import schema_engine

files = {'a_inserts': ('../test_data/opinsert/json_schema2.txt',
                       '../test_data/opinsert/bson_data2.txt')}

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

def test_tables():
    collection_name = 'a_inserts'
    schema_engine = get_schema_engine(collection_name)
    tables = get_schema_tables(schema_engine)
    assert(tables.tables.keys() == ['a_insert_comment_items',
                                    'a_inserts',
                                    'a_insert_comments',
                                    'a_insert_comment_slugs'])
    pp = pprint.PrettyPrinter()
    pp.pprint(tables.errors)
    return tables.tables

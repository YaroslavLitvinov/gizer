#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import os
import pprint
from mongo_schema.tests.test_schema_engine import get_schema_engine, get_schema_tables
from mongo_schema import schema_engine

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
    pp.pprint(tables.data_engine.indexes)
    expected_indexes = {'a_inserts': 1,
                        u'a_inserts_comments': 2,
                        u'a_inserts_comments_items': 2,
                        u'a_inserts_comments_slugs': 1}
    assert(tables.data_engine.indexes == expected_indexes)
    return tables.tables

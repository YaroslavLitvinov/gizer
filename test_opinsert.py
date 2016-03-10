#!/usr/bin/env python

import os
from mongo_to_hive_mapping.test_schema_engine import get_schema_engine, get_schema_tables
from opinsert import generate_insert_queries
from mongo_to_hive_mapping import schema_engine

files = {'a_inserts': ('test_data/opinsert/json_schema2.txt',
                       'test_data/opinsert/bson_data2.txt')}

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


def test_insert1():
    collection_name = 'a_inserts'
    schema_engine = get_schema_engine(collection_name)
    tables = get_schema_tables(schema_engine)
    assert(tables.tables.keys() == ['a_insert_comment_items', \
                                    'a_inserts',
                                    'a_insert_comments'])

    sqltable1 = tables.tables[collection_name]
    inserts1 = generate_insert_queries(sqltable1)
    assert(len(inserts1[1])==1)
    query_fmt1 = 'INSERT INTO TABLE a_inserts (body, user_id, title, created_at, updated_at, id_oid, id_bsontype, idx) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);'
    values11 = (u'body3', u'56b8d7caf9fcee1b00000001', u'title3', u'2016-02-08T19:45:32.501Z', u'2016-02-08T19:45:32.501Z', '56b8f05cf9fcee1b00000010', 7, 1)
    assert(query_fmt1==inserts1[0])
    assert(values11==inserts1[1][0])
#test another table
    sqltable2 = tables.tables[collection_name[:-1]+'_comments']
    inserts2 = generate_insert_queries(sqltable2)
    assert(len(inserts2[1])==2)
    query_fmt2 = 'INSERT INTO TABLE a_insert_comments (body, created_at, id_oid, id_bsontype, updated_at, a_inserts_id_oid, a_inserts_idx, idx) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);'
    values21 = (u'body3', u'2016-02-08T19:45:32.501Z', '56b8f05cf9fcee1b00000110', 7, u'2016-02-08T19:45:32.501Z', '56b8f05cf9fcee1b00000010', 1, 1)
    values22 = (u'body2', u'2016-02-08T19:45:33.501Z', '56b8f05cf9fcee1b00000011', 7, u'2016-02-08T19:45:33.501Z', '56b8f05cf9fcee1b00000010', 1, 2)
    assert(query_fmt2==inserts2[0])
    assert(values21==inserts2[1][0])
    assert(values22==inserts2[1][1])
#test another table
    sqltable3 = tables.tables[collection_name[:-1]+'_comment_items']
    inserts3 = generate_insert_queries(sqltable3)
    assert(len(inserts3[1])==2)
    query_fmt3 = 'INSERT INTO TABLE a_insert_comment_items (data, a_inserts_id_oid, a_inserts_comments_id_oid, a_inserts_idx, a_inserts_comments_idx, idx) VALUES(%s, %s, %s, %s, %s, %s);'
    values31 = (u'1', '56b8f05cf9fcee1b00000010', '56b8f05cf9fcee1b00000110', 1, 1, 1)
    values32 = (u'2', '56b8f05cf9fcee1b00000010', '56b8f05cf9fcee1b00000011', 1, 2, 2)
    assert(query_fmt3==inserts3[0])
    assert(values31==inserts3[1][0])
    assert(values32==inserts3[1][1])


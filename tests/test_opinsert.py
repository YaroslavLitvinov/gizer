#!/usr/bin/env python

import os
from mongo_to_hive_mapping.test_schema_engine import get_schema_engine, get_schema_tables
from gizer.opinsert import generate_insert_queries
from mongo_to_hive_mapping import schema_engine
from test_tables import test_tables

def test_insert1():
    collection_name = 'a_inserts'
    tables = test_tables()
    assert(tables.keys() == ['a_insert_comment_items',
                                    'a_inserts',
                                    'a_insert_comments',
                                    'a_insert_comment_slugs'])

    sqltable1 = tables[collection_name]
    inserts1 = generate_insert_queries(sqltable1)
    assert(len(inserts1[1])==1)
    query_fmt1 = 'INSERT INTO a_inserts (body, created_at, id_bsontype, id_oid, title, updated_at, user_id, idx) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);'
    values11 = (u'body3"\tbody2\nbody1', u'2016-02-08T19:45:32.501Z', 7, '56b8f05cf9fcee1b00000010', u'title3', u'2016-02-08T19:45:32.501Z', u'56b8d7caf9fcee1b00000001', 1)
    assert(query_fmt1==inserts1[0])
    assert(values11==inserts1[1][0])
#test another table with initial_indexes
    sqltable2 = tables[collection_name[:-1]+'_comments']
    initial_indexes = { 'a_inserts': 50,
                        'a_inserts_comments': 100}
    inserts2 = generate_insert_queries(sqltable2, initial_indexes)
    assert(len(inserts2[1])==2)
    query_fmt2 = 'INSERT INTO a_insert_comments (a_inserts_id_oid, body, created_at, id_bsontype, id_oid, updated_at, a_inserts_idx, idx) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);'
    values21 = ('56b8f05cf9fcee1b00000010', None, u'2016-02-08T19:45:32.501Z', 7, '56b8f05cf9fcee1b00000110', u'2016-02-08T19:45:32.501Z', 
                initial_indexes['a_inserts']+1, initial_indexes['a_inserts_comments']+1)
    values22 = ('56b8f05cf9fcee1b00000010', u'body2', u'2016-02-08T19:45:33.501Z', 7, '56b8f05cf9fcee1b00000011', u'2016-02-08T19:45:33.501Z', 
                initial_indexes['a_inserts']+1, initial_indexes['a_inserts_comments']+2)
    assert(query_fmt2==inserts2[0])
    assert(values21==inserts2[1][0])
    assert(values22==inserts2[1][1])
#test another table
    sqltable3 = tables[collection_name[:-1]+'_comment_items']
    inserts3 = generate_insert_queries(sqltable3)
    assert(len(inserts3[1])==2)
    query_fmt3 = 'INSERT INTO a_insert_comment_items (a_inserts_comments_id_oid, a_inserts_id_oid, data, a_inserts_idx, a_inserts_comments_idx, idx) VALUES(%s, %s, %s, %s, %s, %s);'
    values31 = ('56b8f05cf9fcee1b00000110', '56b8f05cf9fcee1b00000010', u'1', 1, 1, 1)
    values32 = ('56b8f05cf9fcee1b00000011', '56b8f05cf9fcee1b00000010', u'2', 1, 2, 2)
    assert(query_fmt3==inserts3[0])
    assert(values31==inserts3[1][0])
    assert(values32==inserts3[1][1])
#test another table
    sqltable4 = tables[collection_name[:-1]+'_comment_slugs']
    inserts4 = generate_insert_queries(sqltable4)
    assert(len(inserts4[1])==1)
    query_fmt4 = 'INSERT INTO a_insert_comment_slugs (slugs, a_inserts_idx, a_inserts_comments_idx, idx) VALUES(%s, %s, %s, %s);'
    values41 = (22, 1, 1, 1)
    assert(query_fmt4==inserts4[0])
    assert(values41==inserts4[1][0])




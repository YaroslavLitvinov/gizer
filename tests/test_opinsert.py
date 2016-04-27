#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import os
from mongo_schema.tests.test_schema_engine import get_schema_engine, get_schema_tables
from gizer.opinsert import generate_insert_queries
from mongo_schema import schema_engine
from test_tables import test_tables
from datetime import datetime

def d(str_date, tzinfo):
    timestamp_fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    l = datetime.strptime(str_date, timestamp_fmt)
    return datetime(l.year, l.month, l.day, l.hour, l.minute, l.second, l.microsecond, tzinfo)


def expect(generated, expected_query, expected_data):
    assert(len(generated[1]) == len(expected_data))
    if generated[0] != expected_query:
        print "generated query", generated[0]
        print "expected query", expected_query
    assert(generated[0] == expected_query)
    for data_i in xrange(len(expected_data)):
        if generated[0] != expected_query:
            print "generated data[", data_i, "]=", generated[1][data_i]
            print "expected data[", data_i, "]=", expected_data[data_i]
        assert(generated[1][data_i] == expected_data[data_i])


def test_insert1():
    collection_name = 'a_inserts'
    tables = test_tables()
    assert(tables.keys() == ['a_insert_comment_items',
                             'a_inserts',
                             'a_insert_comments',
                             'a_insert_comment_slugs'])

    sqltable1 = tables[collection_name]
    inserts1 = generate_insert_queries(sqltable1, "schema_name", "prefix_")
    tz = inserts1[1][0][1].tzinfo
    expect(inserts1, \
               'INSERT INTO schema_name."prefix_a_inserts" \
("body", "created_at", "id_bsontype", "id_oid", "title", "updated_at", "user_id") \
VALUES(%s, %s, %s, %s, %s, %s, %s);',
           [(u'body3"\tbody2\nbody1', 
             d('2016-02-08T19:45:32.501Z', tz), 
             7, 
             '56b8f05cf9fcee1b00000010', 
             u'title3', 
             d('2016-02-08T19:45:32.501Z', tz), 
             u'56b8d7caf9fcee1b00000001')])

    sqltable2 = tables[collection_name[:-1]+'_comments']
    initial_indexes = { 'a_inserts': 50,
                        'a_inserts_comments': 100}

    expect(generate_insert_queries(sqltable2, "", "", initial_indexes),
           'INSERT INTO "a_insert_comments" \
("a_inserts_id_oid", "body", "created_at", "id_bsontype", "id_oid", "updated_at", "idx") \
VALUES(%s, %s, %s, %s, %s, %s, %s);',
           [
            ('56b8f05cf9fcee1b00000010', 
             None, 
             d('2016-02-08T19:45:32.501Z',tz), 
             7, 
             '56b8f05cf9fcee1b00000110', 
             d('2016-02-08T19:45:32.501Z',tz), 
             initial_indexes['a_inserts_comments']+1),
            ('56b8f05cf9fcee1b00000010', 
             u'body2', 
             d('2016-02-08T19:45:33.501Z',tz), 
             7, 
             '56b8f05cf9fcee1b00000011', 
             d('2016-02-08T19:45:33.501Z',tz), 
             initial_indexes['a_inserts_comments']+2)
            ])

    sqltable3 = tables[collection_name[:-1]+'_comment_items']
    expect(generate_insert_queries(sqltable3, "", ""),
           'INSERT INTO "a_insert_comment_items" \
("a_inserts_id_oid", "data", "a_inserts_comments_idx", "idx") \
VALUES(%s, %s, %s, %s);',
           [
            ('56b8f05cf9fcee1b00000010', u'1', 1, 1),
            ('56b8f05cf9fcee1b00000010', u'2', 2, 2)])
   
    sqltable4 = tables[collection_name[:-1]+'_comment_slugs']
    expect(generate_insert_queries(sqltable4, "", ""),
           'INSERT INTO "a_insert_comment_slugs" \
("a_inserts_id_oid", "slugs", "a_inserts_comments_idx", "idx") \
VALUES(%s, %s, %s, %s);',
           [('56b8f05cf9fcee1b00000010', 22, 1, 1)])




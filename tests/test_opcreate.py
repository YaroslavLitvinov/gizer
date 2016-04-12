#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import os
from mongo_schema.tests.test_schema_engine import get_schema_engine, get_schema_tables
from gizer.opcreate import generate_create_table_statement
from mongo_schema import schema_engine
from test_tables import test_tables

def test_insert1():
    collection_name = 'a_inserts'
    tables = test_tables()
    assert(tables.keys() == ['a_insert_comment_items',
                             'a_inserts',
                             'a_insert_comments',
                             'a_insert_comment_slugs'])

    sqltable1 = tables[collection_name]
    create1 = generate_create_table_statement(sqltable1, "", "9999_12_31_")
    query1 = 'CREATE TABLE IF NOT EXISTS "9999_12_31_a_inserts" ("body" TEXT, "created_at" TIMESTAMP, "id_bsontype" INTEGER, "id_oid" TEXT, "title" TEXT, "updated_at" TIMESTAMP, "user_id" TEXT, "idx" BIGINT);'
    assert(query1==create1)
#test another table
    sqltable2 = tables[collection_name[:-1]+'_comments']
    create2 = generate_create_table_statement(sqltable2, "", "")
    query2 = 'CREATE TABLE IF NOT EXISTS "a_insert_comments" ("a_inserts_id_oid" TEXT, "body" TEXT, "created_at" TIMESTAMP, "id_bsontype" INTEGER, "id_oid" TEXT, "updated_at" TIMESTAMP, "a_inserts_idx" BIGINT, "idx" BIGINT);'
    assert(query2==create2)
#test another table
    sqltable3 = tables[collection_name[:-1]+'_comment_items']
    create3 = generate_create_table_statement(sqltable3, "", "")
    query3 = 'CREATE TABLE IF NOT EXISTS "a_insert_comment_items" ("a_inserts_comments_id_oid" TEXT, "a_inserts_id_oid" TEXT, "data" TEXT, "a_inserts_idx" BIGINT, "a_inserts_comments_idx" BIGINT, "idx" BIGINT);'
    assert(query3==create3)
#test another table
    sqltable4 = tables[collection_name[:-1]+'_comment_slugs']
    create4 = generate_create_table_statement(sqltable4, "", "")
    query4 = 'CREATE TABLE IF NOT EXISTS "a_insert_comment_slugs" ("a_inserts_comments_id_oid" TEXT, "a_inserts_id_oid" TEXT, "slugs" INTEGER, "a_inserts_idx" BIGINT, "a_inserts_comments_idx" BIGINT, "idx" BIGINT);'
    assert(query4==create4)

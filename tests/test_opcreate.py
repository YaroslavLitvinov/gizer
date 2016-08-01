#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import os
from mongo_schema.tests.test_schema_engine import get_schema_engine, get_schema_tables
from gizer.opcreate import generate_create_table_statement
from gizer.opcreate import generate_create_table_index_statement
from mongo_schema import schema_engine
from test_tables import collection_tables

def test_insert1():
    collection_name = 'a_inserts'
    tables = collection_tables(collection_name).tables
    assert(tables.keys() == ['a_insert_comment_items',
                             'a_inserts',
                             'a_insert_comments',
                             'a_insert_comment_slugs'])

    sqltable1 = tables[collection_name]
    create1 = generate_create_table_statement(sqltable1, "", "9999_12_31_")
    query1 = 'CREATE TABLE IF NOT EXISTS "9999_12_31_a_inserts" ("body" TEXT, "created_at" TIMESTAMP WITH TIME ZONE, "id_bsontype" INTEGER, "id_oid" TEXT, "title" TEXT, "updated_at" TIMESTAMP WITH TIME ZONE, "user_id" TEXT);'
    assert(query1==create1)
#test another table
    sqltable2 = tables[collection_name[:-1]+'_comments']
    create2 = generate_create_table_statement(sqltable2, "", "")
    query2 = 'CREATE TABLE IF NOT EXISTS "a_insert_comments" ("a_inserts_id_oid" TEXT, "body" TEXT, "created_at" TIMESTAMP WITH TIME ZONE, "id_bsontype" INTEGER, "id_oid" TEXT, "updated_at" TIMESTAMP WITH TIME ZONE, "idx" BIGINT);'
    assert(query2==create2)
#test another table
    sqltable3 = tables[collection_name[:-1]+'_comment_items']
    create3 = generate_create_table_statement(sqltable3, "", "")
    query3 = 'CREATE TABLE IF NOT EXISTS "a_insert_comment_items" ("a_inserts_id_oid" TEXT, "data" TEXT, "a_inserts_comments_idx" BIGINT, "idx" BIGINT);'
    assert(query3==create3)
#test another table
    sqltable4 = tables[collection_name[:-1]+'_comment_slugs']
    create4 = generate_create_table_statement(sqltable4, "", "")
    query4 = 'CREATE TABLE IF NOT EXISTS "a_insert_comment_slugs" ("a_inserts_id_oid" TEXT, "slugs" INTEGER, "a_inserts_comments_idx" BIGINT, "idx" BIGINT);'
    assert(query4==create4)

def test_create_indexes():
    collection_name = 'a_inserts'
    tables = collection_tables(collection_name).tables
    # table 1
    sqltable1 = tables[collection_name]
    indexes1 = generate_create_table_index_statement(sqltable1, '', 'PREFIX_')
    print indexes1
    expect = 'CREATE INDEX "index_PREFIX_a_inserts" ON "PREFIX_a_inserts" ("id_oid");'
    assert( indexes1 == expect )
    # table 2
    sqltable2 = tables[collection_name[:-1]+'_comments']
    indexes2 = generate_create_table_index_statement(sqltable2, '', 'PREFIX_')
    print indexes2
    expect = 'CREATE INDEX "index_PREFIX_a_insert_comments" ON "PREFIX_a_insert_comments" ("a_inserts_id_oid", "idx");'
    assert( indexes2 == expect )
    # table 3
    sqltable3 = tables[collection_name[:-1]+'_comment_items']
    indexes3 = generate_create_table_index_statement(sqltable3, '', 'PREFIX_')
    print indexes3
    expect = 'CREATE INDEX "index_PREFIX_a_insert_comment_items" ON "PREFIX_a_insert_comment_items" ("a_inserts_comments_idx", "a_inserts_id_oid", "idx");'
    assert( indexes3 == expect )
    # table 4
    sqltable4 = tables[collection_name[:-1]+'_comment_slugs']
    indexes4 = generate_create_table_index_statement(sqltable4, '', 'PREFIX_')
    print indexes4
    expect = 'CREATE INDEX "index_PREFIX_a_insert_comment_slugs" ON "PREFIX_a_insert_comment_slugs" ("a_inserts_comments_idx", "a_inserts_id_oid", "idx");'
    assert( indexes4 == expect )

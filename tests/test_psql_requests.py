#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import psycopg2
from gizer.psql_requests import PsqlRequests
from gizer.opcreate import generate_create_table_statement
from gizer.opinsert import generate_insert_queries
from test_tables import collection_tables

def test_all():
    connstr = os.environ['TEST_PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))
    tables = collection_tables("a_inserts").tables
    for table in tables:
        create_table = generate_create_table_statement(tables[table], "", "")
        print create_table
        dbreq.cursor.execute(create_table)
        indexes = dbreq.get_table_max_indexes(tables[table], "")
        inserts = generate_insert_queries(tables[table], "", "", initial_indexes = indexes)
        for query in inserts[1]:
            dbreq.cursor.execute(inserts[0], query)
    dbreq.cursor.execute('COMMIT')


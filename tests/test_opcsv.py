#!/usr/bin/env python

import sys
import os
import StringIO
from mongo_schema.tests.test_schema_engine import get_schema_engine, get_schema_tables
from mongo_schema import schema_engine
from gizer.opcsv import CsvWriter
from test_tables import test_tables

def test_csv1():
    collection_name = 'a_inserts'
    tables = test_tables()
    csvs = {}
    for table_name, table in tables.iteritems():
        output = StringIO.StringIO()
        if table_name not in csvs.keys():
            csvs[table_name] = [output, CsvWriter(output, null_val_as='\N')]
        csvs[table_name][1].write_csv(table)

    table1_name = collection_name
    table1_data = csvs[table1_name][0].getvalue()
    print table1_data
    table2_name = collection_name[:-1]+'_comments'
    table2_data = csvs[table2_name][0].getvalue()
    print table2_data
    assert(table2_data.split('\t')[1] == '\\\\N')
    assert(len(table2_data.splitlines(False)) == 2)


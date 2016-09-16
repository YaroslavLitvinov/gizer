#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import os
from io import BytesIO
import collections
from mongo_schema.tests.test_schema_engine import get_schema_engine, get_schema_tables
from mongo_schema import schema_engine
from gizer.opcsv import CsvWriter, CsvReader, CsvWriteManager
from gizer.opcsv import NULLVAL
from gizer.opinsert import table_rows_list
from gizer.opinsert import ENCODE_ESCAPE, NO_ENCODE_NO_ESCAPE
from test_tables import collection_tables

def row_by_idx(sqltable, idx):
    csvvals = []
    for i in sqltable.sql_column_names:
        val = sqltable.sql_columns[i].values[idx]
        csvvals.append(val)
    return csvvals

def test_csv_error():
    table_name = collection_name = 'a_inserts'
    tables = collection_tables(collection_name).tables
    csvs = {}
    CsvStruct = collections.namedtuple('CsvStruct', ['output', 'writer'])
    table = tables[table_name]
    output = BytesIO()
    csvs[table_name] = CsvStruct(output = output, 
                                 writer=CsvWriter(output, False))
    rows = table_rows_list(table, NO_ENCODE_NO_ESCAPE, NULLVAL)
    ok = False
    try:
        csvs[table_name].writer.write_csv(rows)
    except UnicodeEncodeError:
        ok = True
    assert(ok == True)

def test_csvManager():
    import tempfile
    import shutil
    tmppath = tempfile.mkdtemp()
    # pollute folder
    csm = CsvWriteManager(['a'], tmppath, 5)
    csm.write_csv('a', [('a', 'b'), ('c', 'd')])
    csm.finalize()
    del csm
    # continue test
    csm = CsvWriteManager(['a', 'b'], tmppath, 30)
    written =  csm.write_csv('a', [('abracadabra', 'abcdefgeh'),
                                   ('foo', 'something')])
    assert(written!=0)
    written =  csm.write_csv('a', [('abracadabra', 'abcdefgeh'),
                                   ('foo', 'something')])
    assert(written!=0)
    written = csm.write_csv('b', [('ABRACADABRA', 'ABCDEFGEH'),
                                  ('FOO', 'SOMETHING')])
    assert(written!=0)
    try:
        csm.finalize()
    except:
        shutil.rmtree(tmppath)
        raise
    del csm
    shutil.rmtree(tmppath)

def test_csv1():
    collection_name = 'a_inserts'
    tables = collection_tables(collection_name).tables
    csvs = {}
    CsvStruct = collections.namedtuple('CsvStruct', ['output', 'writer'])
    for table_name, table in tables.iteritems():
        output = BytesIO()
        if table_name not in csvs.keys():
            csvs[table_name] = CsvStruct(output = output, 
                                         writer=CsvWriter(output, False))
        rows = table_rows_list(table, ENCODE_ESCAPE, NULLVAL)
        csvs[table_name].writer.write_csv(rows)
        print "output", output.getvalue()
        csvs[table_name].output.seek(0)

    table1_name = collection_name
    tz = tables[table1_name].sql_columns['created_at'].values[0].tzinfo
    table1_data_row_0 = row_by_idx(tables[table1_name], 0)
    print "table1_data_row_0", table1_data_row_0
    csv_reader1 = CsvReader(csvs[table1_name].output, NULLVAL)
    table1_csv_row_0 = csv_reader1.read_record()
    no_more_records = csv_reader1.read_record()
    csv_reader1.close()
    #only one record is expected in table: a_inserts
    print "table1_csv_row_0", table1_csv_row_0
    print "null record", no_more_records
    assert(no_more_records == None)
    assert(table1_data_row_0[0] == table1_csv_row_0[0])
    assert(table1_data_row_0[6] == table1_csv_row_0[6])
    assert(len(table1_data_row_0) == len(table1_csv_row_0))

    table2_name = collection_name[:-1]+'_comments'
    table2_data_row_0 = row_by_idx(tables[table2_name], 0)
    table2_data = csvs[table2_name].output.getvalue()
    print "table2_data_row_0", table2_data_row_0
    csv_reader2 = CsvReader(csvs[table2_name].output, NULLVAL)
    table2_csv_row_0 = csv_reader2.read_record()
    print "table2_csv_row_0", table2_csv_row_0
    # Null is expected.
    assert(table2_csv_row_0[1] == None)
    assert(table2_data_row_0[1] == table2_csv_row_0[1])
    assert(table2_csv_row_0[4] == '56b8f05cf9fcee1b00000110')
    assert(len(table2_data_row_0) == len(table2_csv_row_0))

    for table_name in tables:
        csvs[table_name].writer.close()


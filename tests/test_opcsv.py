#!/usr/bin/env python

import os
from mongo_to_hive_mapping.test_schema_engine import get_schema_engine, get_schema_tables
from gizer.opcsv import CsvWriter
from mongo_to_hive_mapping import schema_engine
import StringIO

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

def test_csv1():
    collection_name = 'a_inserts'
    schema_engine = get_schema_engine(collection_name)
    tables = get_schema_tables(schema_engine)
    csvs = {}
    for table_name, table in tables.tables.iteritems():
        output = StringIO.StringIO()
        if table_name not in csvs.keys():
            csvs[table_name] = [output, CsvWriter(output, null_val_as='\\N')]
        csvs[table_name][1].write_csv(table)

    table2_name = collection_name[:-1]+'_comments'
    table2_data = csvs[table2_name][0].getvalue()
    assert(table2_data.split('\t')[1] == '\\N')
    assert(len(table2_data.splitlines(False)) == 2)

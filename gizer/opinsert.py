#!/usr/bin/env python

import json
import bson
from bson.json_util import loads

def generate_insert_queries(table, psql_schema_name, initial_indexes = {}):
    """ get insert queries as 
    tuple: (format string, [(list,of,values,as,tuples)])
    @param table object schema_engine.SqlTable
    @param initial_indexes dict of indexes from db tables"""
    queries = []
    if len(psql_schema_name):
        psql_schema_name += '.'
    fmt_string = "INSERT INTO %s%s (%s) VALUES(%s);" \
        % (psql_schema_name, \
               table.table_name, \
               ', '.join(table.sql_column_names), \
               ', '.join(['%s' for i in table.sql_column_names]))
    firstcolname = table.sql_column_names[0]
    reccount = len(table.sql_columns[firstcolname].values)
    for val_i in xrange(reccount):
        values = []
        for column_name in table.sql_column_names:
            col = table.sql_columns[column_name]
            index_key = col.index_key()
            if index_key and index_key in initial_indexes:
                values.append(initial_indexes[index_key] + col.values[val_i])
            else:
                values.append(col.values[val_i])
        queries.append( tuple(values) )
    return (fmt_string, queries)

def opinsert_oplog_handler_callback(ns, schema, objdata):
    return schema_engine.create_schema_engine(collection_name, schema_path)
    pass


if __name__ == "__main__":
    from test_opinsert import test_insert1
    test_insert1()

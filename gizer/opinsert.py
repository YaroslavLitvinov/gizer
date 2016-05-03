#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import json
import bson
from bson.json_util import loads

def format_string_insert_query(table, psql_schema_name, table_prefix):
    """ format string to be used with execute
    @param table object schema_engine.SqlTable
    @param psql_schema_name
    @param table_prefix
    """
    if len(psql_schema_name):
        psql_schema_name += '.'
    table_name = table.table_name
    if len(table_prefix):
        table_name = table_prefix + table_name
    fmt_string = 'INSERT INTO %s"%s" (%s) VALUES(%s);' \
        % (psql_schema_name, table_name, \
               ', '.join(['"'+i+'"' for i in table.sql_column_names]), \
               ', '.join(['%s' for i in table.sql_column_names]))
    return fmt_string

def escape_val(val, escape):
    """ @param escape if True then escape special character"""
    if type(val) is str or type(val) is unicode:
        if escape is True:
            return val.encode('unicode-escape').encode('utf-8')
        else:
            return val
    else:
        return val

def prepare_csv_data(row, psql_copy = False):
    """ @param psql_copy if True then create data consumable by psql_copy"""
    for row_i in xrange(len(row)):
        val = row[row_i]
        if val is not None:
            val = escape_val(val, not psql_copy)
        else:
            val = self.null_val
        row[row_i] = val
    return row

def index_columns_as_dict(table):
    """ get dict with index columns, value is column index in row
    @param table object schema_engine.SqlTable"""
    res = {}
    for col_i in xrange(len(table.sql_column_names)):
        column_name = table.sql_column_names[col_i]
        col = table.sql_columns[column_name]
        if col.index_key():
            res[col.index_key()] = col_i
    return res

def apply_indexes_to_table_rows(rows, index_keys, initial_indexes = {}):
    """ get list of rows, every row is values list
    @param index_keys {'index_name': 'column index'}
    @param initial_indexes dict of indexes from db tables"""
    for index_key in index_keys:
        if index_key and index_key in initial_indexes:
            col_i = index_keys[index_key]
            # adjust all column's indexes 
            for row_i in xrange(len(rows)):
                rows[row_i][col_i] = initial_indexes[index_key] + rows[row_i][col_i]
    return rows

def table_rows_list(table, escape, null_value = None):
    """ get list of rows, every row is values list
    @param table object schema_engine.SqlTable"""
    res = []
    firstcolname = table.sql_column_names[0]
    reccount = len(table.sql_columns[firstcolname].values)
    for val_i in xrange(reccount):
        values = []
        for column_name in table.sql_column_names:
            col = table.sql_columns[column_name]
            if col.values[val_i] is not None:
                val = escape_val(col.values[val_i], escape)
            else:
                val = null_value
            values.append(val)
        res.append( values )
    return res



def generate_insert_queries(table, psql_schema_name, table_prefix, 
                            initial_indexes = {}):
    """ get insert queries as 
    tuple: (format string, [(list,of,values,as,tuples)])
    @param table object schema_engine.SqlTable
    @param initial_indexes dict of indexes from db tables"""
    fmt_string = format_string_insert_query(table, psql_schema_name, table_prefix)
    index_keys = index_columns_as_dict(table)
    rows = apply_indexes_to_table_rows(table_rows_list(table, False),
                                       index_keys, initial_indexes)
    queries = []
    for row in rows:
        queries.append( tuple(row) )
    return (fmt_string, queries)

def opinsert_oplog_handler_callback(ns, schema, objdata):
    return schema_engine.create_schema_engine(collection_name, schema_path)
    pass


if __name__ == "__main__":
    from test_opinsert import test_insert1
    test_insert1()

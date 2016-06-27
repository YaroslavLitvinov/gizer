#!/usr/bin/env python

""" Drop, create table statements generators from 'SqlTable' objects. """

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

def generate_drop_table_statement(table, psql_schema_name, table_name_prefix):
    """ return drop table statement.
    params:
    table -- 'SqlTable' object
    psql_schema_name -- schema name in postgres where to drop table
    table_name_prefix -- table prefix, like 'yyyxxx_'"""
    if len(psql_schema_name):
        psql_schema_name += '.'
    query = 'DROP TABLE IF EXISTS %s"%s%s";' \
        % (psql_schema_name, table_name_prefix, table.table_name)
    return query


def generate_create_table_statement(table, psql_schema_name, table_name_prefix):
    """ return create table statement.
    params:
    table -- 'SqlTable' object
    psql_schema_name -- schema name in postgres where to create table
    table_name_prefix -- table prefix, like 'yyyxxx_'"""
    override_types = {'INT': 'INTEGER',
                      'STRING': 'TEXT',
                      'TIMESTAMP': 'TIMESTAMP WITH TIME ZONE',
                      'DOUBLE': 'FLOAT8',
                      'TINYINT': 'INT2'}
    cols = []
    for colname in table.sql_column_names:
        sqlcol = table.sql_columns[colname]
        if sqlcol.typo in override_types.keys():
            typo = override_types[sqlcol.typo]
        else:
            typo = sqlcol.typo
        cols.append('"%s" %s' % (sqlcol.name, typo))
    if len(psql_schema_name) and psql_schema_name.find('.') == -1:
        psql_schema_name += '.'
    query = 'CREATE TABLE IF NOT EXISTS %s"%s%s" (%s);' \
        % (psql_schema_name,
           table_name_prefix,
           table.table_name,
           ', '.join(cols))
    return query

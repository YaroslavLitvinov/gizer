#!/usr/bin/env python

""" Drop, create table statements generators from 'SqlTable' objects. """

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

INDEX_ID_IDXS = 'a'
INDEX_ID_PARENT_IDXS = 'b'
INDEX_ID_ONLY = 'c'

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


def generate_create_index_statement(table, 
                                    psql_schema_name, 
                                    table_name_prefix,
                                    index_type):
    """ return create table index statement.
    params:
    table -- 'SqlTable' object
    psql_schema_name -- schema name in postgres where to create table
    table_name_prefix -- table prefix, like 'yyyxxx_'"""
    psql_index_columns = []
    # add 'table internal indexes' to postgres indexes list
    # if table is not a root table
    if table.root.parent:
        for parent_idx_node in table.idx_nodes():
            # if idx is related to own table
            if table.root.long_alias() == parent_idx_node.long_alias():
                if index_type is INDEX_ID_IDXS:
                    psql_index_columns.append('"idx"')
            else:
                if index_type is INDEX_ID_IDXS or \
                        index_type is INDEX_ID_PARENT_IDXS:
                    alias = parent_idx_node.long_alias()
                    psql_index_columns.append('"'+alias+'_idx"')
    # add super parent id
    id_node = table.root.super_parent().get_id_node()
    if index_type is INDEX_ID_IDXS or \
            index_type is INDEX_ID_ONLY or \
            index_type is INDEX_ID_PARENT_IDXS:
        if table.root.parent:
            psql_index_columns.append('"'+id_node.long_alias()+'"')
        else:
            psql_index_columns.append('"'+id_node.short_alias()+'"')
    # formate query
    if len(psql_schema_name) and psql_schema_name.find('.') == -1:
        psql_schema_name += '.'
    psql_index_columns.sort() # to have a determenistic order
    query = 'CREATE INDEX "i{type}_{prefix}{table}" ON {schema}"{prefix}{table}"\
 ({index_columns});'.format( schema=psql_schema_name,
                             prefix=table_name_prefix,
                             table=table.table_name,
                             index_columns=', '.join(psql_index_columns),
                             type=str(index_type))
    return query
    

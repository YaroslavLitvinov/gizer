#!/usr/bin/env python


def generate_create_table_statement(table):
    """ return string
    @param table object schema_engine.SqlTable"""
    override_types = {'INT': 'INTEGER',
                      'STRING': 'TEXT',
                      'DOUBLE': 'FLOAT8',
                      'TINYINT': 'INT2'}
    cols = []
    for colname in table.sql_column_names:
        sqlcol = table.sql_columns[colname]
        if sqlcol.typo in override_types.keys():
            typo = override_types[sqlcol.typo]
        else:
            typo = sqlcol.typo
        cols.append( '"%s" %s' % (sqlcol.name, \
                                typo ) )
    query = 'CREATE TABLE IF NOT EXISTS "%s" (%s);' % (table.table_name, ', '.join(cols))
    return query

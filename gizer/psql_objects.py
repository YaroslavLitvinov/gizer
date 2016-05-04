#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from gizer.opcreate import generate_create_table_statement
from gizer.opinsert import generate_insert_queries
from mongo_schema.schema_engine import create_tables_load_bson_data

def parent_id_name_for_table(sqltable):
    id_name = None
    for colname, sqlcol in sqltable.sql_columns.iteritems():
        # root table
        if not sqltable.root.parent and \
                sqlcol.node == sqltable.root.get_id_node():
            id_name = colname
            break
        else: # nested table       
            if sqlcol.node.reference:
                id_name = colname
                break
    return id_name

def load_single_rec_into_tables_obj(src_dbreq, 
                                    schema_engine, 
                                    psql_schema, 
                                    rec_id):
    if len(psql_schema):
        psql_schema += '.'
    tables = create_tables_load_bson_data(schema_engine, None)

    # fetch mongo rec by id from source psql
    ext_tables_data = {}
    for table_name, table in tables.tables.iteritems():
        id_name = parent_id_name_for_table(table)
        select_fmt = 'SELECT * FROM {schema}"{table}" \
WHERE {id_name}={id_val};'
        select_req = select_fmt.format(schema = psql_schema,
                                       table = table_name,
                                       id_name = id_name,
                                       id_val = rec_id)
        src_dbreq.cursor.execute(select_req)
        ext_tables_data[table_name] = []
        for record in src_dbreq.cursor:
            ext_tables_data[table_name].append(record)

    # set external tables data to Tables
    tables.load_external_tables_data(ext_tables_data)
    return tables


def insert_tables_data_into_dst_psql(dst_dbreq, 
                                     tables_to_save,
                                     dst_schema_name, 
                                     dst_table_prefix):
    """ Do every insert as separate transaction, very slow approach """
    # insert fetched mongo rec into destination psql
    for table_name, table in tables_to_save.tables.iteritems():
        # create table if not exist
        create_query = generate_create_table_statement(table, 
                                                       dst_schema_name, 
                                                       dst_table_prefix)
        dst_dbreq.cursor.execute(create_query)
        insert_query = generate_insert_queries(table, 
                                               dst_schema_name, 
                                               dst_table_prefix)
        for insert_data in insert_query[1]:
            print "insert", table_name
            dst_dbreq.cursor.execute(insert_query[0],
                                     insert_data)
    # commit 
    dst_dbreq.cursor.execute('COMMIT')


def insert_rec_from_one_tables_set_to_another(dbreq, 
                                              rec_id,
                                              tables_structure,
                                              src_schema_name,
                                              dst_schema_name):
    """ Just execute psql requests in the same DB, fast approach """
    if len(src_schema_name):
        src_schema_name += '.'
    if len(dst_schema_name):
        dst_schema_name += '.'

    for table_name, table in tables_structure.tables.iteritems():
        # create table if not exist
        create_query = generate_create_table_statement(table,
                                                       dst_schema_name, '')
        dbreq.cursor.execute(create_query)
        # print create_query
        id_name = parent_id_name_for_table(table)
        insert_fmt = 'INSERT INTO {dst_schema}{dst_table} \
SELECT * FROM {src_schema}{src_table} WHERE {id_name}={id_val};'
        insert_query = insert_fmt.format(dst_schema = dst_schema_name,
                                         dst_table = table_name,
                                         src_schema = src_schema_name,
                                         src_table = table_name,
                                         id_name = id_name,
                                         id_val = rec_id)
        # print insert_query
        dbreq.cursor.execute(insert_query)
    dbreq.cursor.execute('COMMIT')


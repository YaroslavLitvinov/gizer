#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from logging import getLogger
from gizer.opcreate import generate_create_table_statement
from gizer.opinsert import generate_insert_queries
from gizer.opcreate import generate_drop_table_statement
from gizer.opcreate import generate_create_table_statement
from gizer.all_schema_engines import get_schema_engines_as_dict
from mongo_schema.schema_engine import create_tables_load_bson_data

def parent_id_name_and_quotes_for_table(sqltable):
    id_name = None
    quotes = False
    for colname, sqlcol in sqltable.sql_columns.iteritems():
        # root table
        if not sqltable.root.parent and \
                sqlcol.node == sqltable.root.get_id_node():
            id_name = colname
            if sqlcol.typo == "STRING":
                quotes = True
            break
        else: # nested table       
            if sqlcol.node.reference:
                id_name = colname
                if sqlcol.typo == "STRING":
                    quotes = True
                break
    return (id_name, quotes)

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
        id_name, quotes = parent_id_name_and_quotes_for_table(table)
        if quotes:
            id_val = "'" + str(rec_id) + "'"
        else:
            id_val = rec_id
        select_fmt = 'SELECT * FROM {schema}"{table}" \
WHERE {id_name}={id_val};'
        select_req = select_fmt.format(schema = psql_schema,
                                       table = table_name,
                                       id_name = id_name,
                                       id_val = id_val)
        src_dbreq.cursor.execute(select_req)
        getLogger(__name__).debug("Get psql data: "+select_req)
        ext_tables_data[table_name] = []
        idx=0
        for record in src_dbreq.cursor:
            getLogger(__name__).debug("result[%d]=%s" % (idx, str(record)))
            ext_tables_data[table_name].append(record)
            idx += 1

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
            getLogger(__name__).debug("insert=%s" % table_name)
            dst_dbreq.cursor.execute(insert_query[0],
                                     insert_data)
            if '56b8f05cf9fcee1b00000010' in insert_data:
                dst_dbreq.cursor.execute('select * from operational.posts')
                getLogger(__name__).debug("insert table: %s" % table_name)
                getLogger(__name__).debug("data: %s, %s" % (insert_query[0],
                                                            insert_data))

def insert_rec_from_one_tables_set_to_another(dbreq, 
                                              rec_id,
                                              tables_structure,
                                              src_schema_name,
                                              dst_schema_name):
    """ Just execute psql requests in the same DB, fast approach """
    if len(src_schema_name) and src_schema_name.find('.') == -1:
        src_schema_name += '.'
    if len(dst_schema_name) and dst_schema_name.find('.') == -1:
        dst_schema_name += '.'

    for table_name, table in tables_structure.tables.iteritems():
        # create table if not exist
        create_query = generate_create_table_statement(table,
                                                       dst_schema_name, '')
        dbreq.cursor.execute(create_query)
        id_name, quotes = parent_id_name_and_quotes_for_table(table)
        if quotes:
            id_val = "'" + str(rec_id) + "'"
        else:
            id_val = rec_id
        insert_fmt = 'INSERT INTO {dst_schema}{dst_table} \
SELECT * FROM {src_schema}{src_table} WHERE {id_name}={id_val};'
        insert_query = insert_fmt.format(dst_schema = dst_schema_name,
                                         dst_table = table_name,
                                         src_schema = src_schema_name,
                                         src_table = table_name,
                                         id_name = id_name,
                                         id_val = id_val)
        getLogger(__name__).debug(insert_query)
        dbreq.cursor.execute(insert_query)

def create_psql_table(table, dbreq, psql_schema, prefix, drop):
    if drop:
        query1 = generate_drop_table_statement(table, 
                                               psql_schema, 
                                               prefix)
        getLogger(__name__).info("EXECUTE: " + query1)
        dbreq.cursor.execute(query1)
    query = generate_create_table_statement(table, 
                                            psql_schema, 
                                            prefix)
    getLogger(__name__).info("EXECUTE: " + query)
    dbreq.cursor.execute(query)
    

def create_psql_tables(tables_obj, dbreq, psql_schema, prefix, drop):
    for table_name, table in tables_obj.tables.iteritems():
        create_psql_table(table, dbreq, psql_schema, prefix, drop)

def create_truncate_psql_objects(dbreq, schemas_path, psql_schema):
    """ drop and create tables for all collections """
    schema_engines = get_schema_engines_as_dict(schemas_path)
    for schema_name, schema in schema_engines.iteritems():
        tables_obj = create_tables_load_bson_data(schema, None)
        drop = True
        create_psql_tables(tables_obj, dbreq, psql_schema, '', drop)

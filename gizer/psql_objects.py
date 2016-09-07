#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from logging import getLogger
from gizer.opinsert import generate_insert_queries
from gizer.opcreate import generate_drop_table_statement
from gizer.opcreate import generate_create_table_statement
from gizer.opcreate import generate_create_index_statement
from gizer.opcreate import INDEX_ID_IDXS
from gizer.opcreate import INDEX_ID_PARENT_IDXS
from gizer.opcreate import INDEX_ID_ONLY
from gizer.all_schema_engines import get_schema_engines_as_dict
from mongo_schema.schema_engine import create_tables_load_bson_data
from mongo_schema.schema_engine import log_table_errors


def cmp_psql_mongo_tables(rec_id, mongo_tables_obj, psql_tables_obj):
    """ Return True/False. Compare actual mongo record with record's relational
    model from operational tables. Comparison of non existing objects gets True.
    psql_tables_obj -- Tables obj loaded from postgres;
    mongo_tables_obj -- Tables obj loaded from mongodb. """
    res = None
    if psql_tables_obj.is_empty() and mongo_tables_obj.is_empty():
        # comparison of non existing objects gets True
        res = True
    else:
        compare_res = mongo_tables_obj.compare(psql_tables_obj)
        if not compare_res:
            collection_name = mongo_tables_obj.schema_engine.root_node.name
            log_table_errors("%s's MONGO rec load warns:" % collection_name,
                             mongo_tables_obj.errors)
            getLogger(__name__).debug('cmp rec=%s res=False mongo arg[1] data:',
                                      rec_id)
            for line in str(mongo_tables_obj.tables).splitlines():
                getLogger(__name__).debug(line)
            getLogger(__name__).debug('cmp rec=%s res=False psql arg[2] data:',
                                      rec_id)
            for line in str(psql_tables_obj.tables).splitlines():
                getLogger(__name__).debug(line)

        # save result of comparison
        res = compare_res
    return res

def parent_id_name_and_quotes_for_table(sqltable):
    """ Return tuple with 2 items (nameof_field_of_parent_id, Boolean)
    True - if field data type id string and must be quoted), False if else """
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
    """ Return Tables obj loaded from postgres. """
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
        indexes = [name \
                       for name in table.sql_column_names \
                       if table.sql_columns[name].index_key()]
        idx_order_by = ''
        if len(indexes):
            idx_order_by = "ORDER BY " + ','.join(indexes)
        select_fmt = 'SELECT * FROM {schema}"{table}" \
WHERE {id_name}={id_val} {idx_order_by};'
        select_req = select_fmt.format(schema=psql_schema,
                                       table=table_name,
                                       id_name=id_name,
                                       id_val=id_val,
                                       idx_order_by=idx_order_by)
        getLogger(__name__).debug("Get psql data: "+select_req)
        src_dbreq.cursor.execute(select_req)
        ext_tables_data[table_name] = []
        idx = 0
        for record in src_dbreq.cursor:
            record_decoded = []
            if type(record) is tuple:
                for titem in record:
                    if type(titem) is str:
                        record_decoded.append(titem.decode('utf-8'))
                    else:
                        record_decoded.append(titem)
                record = tuple(record_decoded)
            getLogger(__name__).debug("result[%d]=%s", idx, record)
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
    for _, table in tables_to_save.tables.iteritems():
        create_psql_table(table, dst_dbreq, dst_schema_name,
                          dst_table_prefix, False)
        insert_query = generate_insert_queries(table,
                                               dst_schema_name,
                                               dst_table_prefix)
        for insert_data in insert_query[1]:
            dst_dbreq.cursor.execute(insert_query[0],
                                     insert_data)

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

def create_psql_index(table, dbreq, psql_schema, prefix):
    """ Create postgresql indexes for table """
    #index 1
    query = generate_create_index_statement(table,
                                            psql_schema,
                                            prefix,
                                            INDEX_ID_IDXS)
    getLogger(__name__).info("EXECUTE: " + query)
    dbreq.cursor.execute(query)
    # index 2
    query = generate_create_index_statement(table,
                                            psql_schema,
                                            prefix,
                                            INDEX_ID_ONLY)
    getLogger(__name__).info("EXECUTE: " + query)
    dbreq.cursor.execute(query)
    # index 3
    query = generate_create_index_statement(table,
                                            psql_schema,
                                            prefix,
                                            INDEX_ID_PARENT_IDXS)
    getLogger(__name__).info("EXECUTE: " + query)
    dbreq.cursor.execute(query)



def create_psql_tables(tables_obj, dbreq, psql_schema, prefix, drop):
    """ drop and create tables related to Tables obj """
    for _, table in tables_obj.tables.iteritems():
        create_psql_table(table, dbreq, psql_schema, prefix, drop)

def create_truncate_psql_objects(dbreq, schemas_path, psql_schema):
    """ drop and create tables for all collections """
    schema_engines = get_schema_engines_as_dict(schemas_path)
    for _, schema in schema_engines.iteritems():
        tables_obj = create_tables_load_bson_data(schema, None)
        drop = True
        create_psql_tables(tables_obj, dbreq, psql_schema, '', drop)

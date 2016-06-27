#!/usr/bin/env python

__copyright__ = "Copyright 2016, Rackspace Inc."

from collections import namedtuple
from gizer.opinsert import generate_insert_queries
from mongo_schema.schema_engine import create_tables_load_bson_data
from mongo_schema.schema_engine import log_table_errors
from gizer.opdelete import op_delete_stmts
from gizer.opupdate import update

OplogQuery = namedtuple('OplogQuery', ['op', 'query'])

def cb_insert(psql_schema, ts, ns, schema_engine, bson_data):
    tables = create_tables_load_bson_data(schema_engine, bson_data)
    collection_name = tables.schema_engine.root_node.name
    log_table_errors("collection: %s data for opinsert load from MONGO OPLOG \
with errors:" % collection_name, tables.errors)
    res = []
    for name, table in tables.tables.iteritems():
        res.append(OplogQuery("i", generate_insert_queries(table,
                                                           psql_schema,
                                                           "")))
    return res

def cb_update(ext_arg, schema_engine, bson_data):
    # for set.name = "comments" don't neeed max indexes at all,
    # just use default indexes to add data to parent (parent_id)
    # for set.name = "comments.2" use provided index=2
    dbreq = ext_arg[0]
    psql_schema = ext_arg[1]
    res = []
    cb_res = update(dbreq, schema_engine, bson_data, '', psql_schema)
    for it in cb_res:
        for op in it:
            res.append(OplogQuery('u', (op, it[op])))
    return res


def cb_delete(ext_arg, ts, ns, schema, bson_data):
    dbreq = ext_arg[0]
    psql_schema = ext_arg[1]
    id_str = str(bson_data['_id'])
    cb_res = op_delete_stmts(dbreq, schema.schema,ns.split('.')[-1],id_str, '', psql_schema)
    res = []
    for oper in cb_res:
        for stmnt in cb_res[oper]:
            res.append(OplogQuery('d', (stmnt, [tuple(cb_res[oper][stmnt])])))
    return res


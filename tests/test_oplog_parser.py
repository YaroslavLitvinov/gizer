#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import psycopg2
import bson
from collections import namedtuple
from bson.json_util import loads
from gizer.psql_requests import PsqlRequests
from gizer.oplog_parser import OplogParser
from gizer.oplog_parser import OplogQuery
from gizer.oplog_parser import Callback
from gizer.opinsert import generate_insert_queries
from gizer.oppartial_record import get_tables_data_from_oplog_set_command
from gizer.psql_objects import load_single_rec_into_tables_obj
from gizer.psql_objects import insert_rec_from_one_tables_set_to_another
from gizer.psql_objects import create_psql_tables
from gizer.all_schema_engines import get_schema_engines_as_dict
from mongo_schema.schema_engine import create_tables_load_bson_data
from mock_mongo_reader import MongoReaderMock
from gizer.opdelete import op_delete_stmts
from gizer.opupdate import update

# THis schema must be precreated before running tests
TMP_SCHEMA_NAME = 'operational'

def exec_insert(dbreq, oplog_query):
    # create new connection and cursor
    query = oplog_query.query
    fmt_string = query[0]
    for sqlparams in query[1]:
        dbreq.cursor.execute(fmt_string, sqlparams)

def cb_insert(psql_schema, ts, ns, schema_engine, bson_data):
    tables = create_tables_load_bson_data(schema_engine, bson_data)
    res = []
    for name, table in tables.tables.iteritems():
        res.append(OplogQuery("i", generate_insert_queries(table, 
                                                           psql_schema, 
                                                           "")))
    return res

def cb_update(psql_schema, schema_engine, bson_data):
    # for set.name = "comments" don't neeed max indexes at all,
    # just use default indexes to add data to parent (parent_id)
    # for set.name = "comments.2" use provided index=2
    if bson_data['ts'] == '6249012828238249985' or \
            bson_data ['ts'] == '6249012068029138593':
        tables, initial_indexes \
            = get_tables_data_from_oplog_set_command(schema_engine, 
                                                     bson_data['o']['$set'],
                                                     bson_data['o2'])
        res = []
        for name, table in tables.iteritems():
            res.append(OplogQuery("ui", \
                generate_insert_queries(table, psql_schema, 
                                        "", initial_indexes)))
        return res
    else:
        res = []
        cb_res = update(schema_engine, bson_data)
        for it in cb_res:
            for op in it:
                res.append(OplogQuery('u', (op, it[op])))
        return res
     
def cb_delete(psql_schema, ts, ns, schema, bson_data):
    id_str = str(bson_data['_id'])
    cb_res = op_delete_stmts(schema.schema,ns.split('.')[-1],id_str)
    res = []
    for op in cb_res:
        for stmnt in cb_res[op]:
            res.append(OplogQuery('d', (stmnt, [tuple(cb_res[op][stmnt])])))
    return res

def cb_before(ext_arg, schema_engine, item):
    """ Needed for oplog syncing.
    When handling oplog records during oplog sync,
    it's can be needed at first to copy data into 
    operational database. Hadnled oplog records must exec
    queries in operational database also."""
    if not hasattr(cb_before, "ids"):
        cb_before.ids = []
    dbreq = ext_arg[0]
    src_schema_name = ext_arg[1]
    dst_schema_name = ext_arg[2]

    tables_obj = create_tables_load_bson_data(schema_engine,
                                              None)
    try:
        if item["op"] == "u":
            rec_id = str(item['o2'].values()[0])
            if rec_id not in cb_before.ids:
                # copy record from main tables to operational
                insert_rec_from_one_tables_set_to_another(dbreq, 
                                                          rec_id,
                                                          tables_obj,
                                                          src_schema_name,
                                                          dst_schema_name)
                cb_before.ids.append(rec_id)
        elif item["op"] == "d":
            rec_id = str(item['o'].values()[0])
            if rec_id not in cb_before.ids:
                # copy record from main tables to operational
                insert_rec_from_one_tables_set_to_another(dbreq, 
                                                          rec_id,
                                                          tables_obj,
                                                          src_schema_name,
                                                          dst_schema_name)
                cb_before.ids.append(rec_id)
        elif item["op"] == "i":
            # do not prepare
            pass
    except:
        # create skeleton of original psql tables as initial load 
        # was not executed previously.
        drop = True
        create_psql_tables(tables_obj, dbreq, src_schema_name, '', drop)

def record_status(dbreq, mongo_reader, schema_engine, rec_id,
                  dst_schema_name):
    """ Return True/False
    Compare actual mongo record with record's relational model 
    from operational tables."""
    res = None
    tables_obj = create_tables_load_bson_data(schema_engine,
                                              None)
    # retrieve actual mongo record and transform it to relational data
    mongo_reader.make_new_request(rec_id)
    rec = mongo_reader.next()
    if not rec:
        res = False
    else:
        mongo_tables_obj = create_tables_load_bson_data(schema_engine, 
                                                        [rec])
        psql_tables_obj = load_single_rec_into_tables_obj(dbreq, 
                                                          schema_engine,
                                                          dst_schema_name,
                                                          rec_id)
        compare_res = mongo_tables_obj.compare(psql_tables_obj)
        # save result of comparison
        res = compare_res
        print "rec_id=", rec_id, "compare res=", res
    return res

def apply_oplog_recs_after_ts(start_ts, psql, mongo, oplog,
                              schemas_path, psql_schema):
    """
    @param start_ts Timestamp of record in oplog db which should be
    applied first or next available
    @param psql Postgres cursor wrapper
    @param mongo Mongo cursor wrappper
    @param oplog Mongo oplog cursor wrappper
    @param schemas_path Path with js schemas representing mongo collections
    @param psql_schema schema name in psql db where psql tables are wating 
    for oplog data to apply
    """
    handled_mongo_rec_ids = {} # {collection: [rec list]}
    # create oplog parser
    parser = OplogParser(oplog, start_ts, schemas_path,
                         Callback(cb_before, 
                                  ext_arg = (psql, '', psql_schema)),
                         Callback(cb_insert, ext_arg = psql_schema),
                         Callback(cb_update, ext_arg = psql_schema),
                         Callback(cb_delete, ext_arg = psql_schema))
    # go over oplog, and apply all oplog pacthes starting from start_ts
    oplog_queries = parser.next()
    while oplog_queries != None:
        for oplog_query in oplog_queries:
            if oplog_query.op == "u":
                exec_insert(psql, oplog_query)
            elif oplog_query.op == "d":
                exec_insert(psql, oplog_query)
            elif oplog_query.op == "i" or oplog_query.op == "ui":
                exec_insert(psql, oplog_query)
        collection_name = parser.item_info.schema_name
        rec_id = parser.item_info.rec_id
        if collection_name not in handled_mongo_rec_ids:
            handled_mongo_rec_ids[collection_name] = []
        if rec_id not in handled_mongo_rec_ids[collection_name]:
            handled_mongo_rec_ids[collection_name].append(rec_id)
        oplog_queries = parser.next()
    sync_ok = True
    # compare mongo data & psql data after oplog records applied
    for collection_name, recs in handled_mongo_rec_ids.iteritems():
        schema_engine = parser.schema_engines[collection_name]
        for rec_id in recs:
            rec_stat = record_status(psql, mongo, schema_engine, 
                                     rec_id, psql_schema)
            if rec_stat == False:
                sync_ok = False
                break
    if parser.first_handled_ts:
        return (parser.first_handled_ts, sync_ok)
    else:
        return (None, False)

def create_truncate_psql_objects(dbreq, schemas_path, psql_schema):
    schema_engines = get_schema_engines_as_dict(schemas_path)
    for schema_name, schema in schema_engines.iteritems():
        tables_obj = create_tables_load_bson_data(schema, None)
        drop = True
        create_psql_tables(tables_obj, dbreq, psql_schema, '', drop)

def sync_oplog(test_ts, dbreq, mongo, oplog, schemas_path, psql_schema):
    # create/truncate psql operational tables
    # which are using during oplog tail lookup
    create_truncate_psql_objects(dbreq, schemas_path, psql_schema)
    ts_sync = apply_oplog_recs_after_ts(test_ts, dbreq, mongo, oplog,
                                        schemas_path, psql_schema)
    if ts_sync[1] == True:
        # sync succesfull if sync ok and was handled non null ts
        dbreq.cursor.execute('COMMIT')
        return True
    elif ts_sync[0]:
        print "sync oplog failed, tried to do starting from ts=", test_ts
        # nest sync iteration, starting from ts_sync[0]
        return ts_sync[0]
    else:
        return False

def initial_load(dbreq, schemas_path, psql_schema):
    create_truncate_psql_objects(dbreq, schemas_path, psql_schema)    

def check_oplog_sync(oplog_ts_to_test):
    connstr = os.environ['TEST_PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))

    #create test mongo reader
    mongo_reader = None
    with open('test_data/posts_data_target_oplog_sync.js') as opfile:
         posts_data = opfile.read()
         mongo_reader = MongoReaderMock(posts_data)
         opfile.close()

    # create test olpog reader
    oplog_reader = None
    with open('test_data/test_oplog.js') as opfile:
         oplog_data = opfile.read()
         oplog_reader = MongoReaderMock(oplog_data)
         opfile.close()

    # oplog_ts_to_test is timestamp starting from which oplog records 
    # should be applied to psql tables to locate ts which corresponds to 
    # initially loaded psql data; 
    # None - means oplog records should be tested starting from beginning 
    schemas_path = "./test_data/schemas/rails4_mongoid_development"
    psql_schema = TMP_SCHEMA_NAME
    initial_psql_schema = ''
    initial_load(dbreq, schemas_path, initial_psql_schema)
    sync_res = sync_oplog(oplog_ts_to_test, 
                          dbreq, mongo_reader, oplog_reader,
                          schemas_path, psql_schema)
    while True:
        if sync_res is False or sync_res is True:
            break
        else:
            oplog_ts_to_test = sync_res
        sync_res = sync_oplog(oplog_ts_to_test, 
                          dbreq, mongo_reader, oplog_reader,
                          schemas_path, psql_schema)
    return sync_res

def test_oplog_sync():
    res = check_oplog_sync(None)
    assert(res == True)
    res = check_oplog_sync('6249008760904220673')
    assert(res == True)
    res = check_oplog_sync('6249012068029138000')
    assert(res == False)


if __name__ == '__main__':
    test_oplog_sync()

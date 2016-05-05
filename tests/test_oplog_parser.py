#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import psycopg2
from collections import namedtuple
from bson.json_util import loads
from gizer.psql_requests import PsqlRequests
from gizer.oplog_parser import OplogParser
from gizer.oplog_parser import OplogQuery
from gizer.oplog_parser import Callback
from gizer.opinsert import generate_insert_queries
from gizer.opcreate import generate_drop_table_statement
from gizer.opcreate import generate_create_table_statement
from gizer.oppartial_record import get_tables_data_from_oplog_set_command
from gizer.psql_objects import load_single_rec_into_tables_obj
from gizer.psql_objects import insert_rec_from_one_tables_set_to_another
from gizer.all_schema_engines import get_schema_engines_as_dict
from mongo_schema.schema_engine import create_tables_load_bson_data
from mock_mongo_reader import MongoReaderMock
from gizer.opdelete import op_delete_stmts
from gizer.opupdate import update

# THis schema must be precreated before running tests
TMP_SCHEMA_NAME = 'operational'

def exec_insert(dbreq, oplog_query):
    query = oplog_query.query
    fmt_string = query[0]
    for sqlparams in query[1]:
        dbreq.cursor.execute(fmt_string, sqlparams)

def cb_insert(ts, ns, schema_engine, bson_data):
    tables = create_tables_load_bson_data(schema_engine, bson_data)
    posts_table = tables.tables['posts']
    assert(posts_table)
    assert(posts_table.sql_column_names == [u'body',
                                            u'created_at', 
                                            u'id_bsontype', 
                                            u'id_oid', 
                                            u'title', 
                                            u'updated_at', 
                                            u'user_id'])
    res = []
    for name, table in tables.tables.iteritems():
        res.append(OplogQuery("i", generate_insert_queries(table, "", "")))
    return res

def cb_update(schema_engine, bson_data):
    # for set.name = "comments" don't neeed max indexes at all,
    # just use default indexes to add data to parent (parent_id)
    # for set.name = "comments.2" use provided index=2
    if bson_data['ts'] == '6249012828238249985' or bson_data ['ts'] == '6249012068029138593':
        tables, initial_indexes \
            = get_tables_data_from_oplog_set_command(schema_engine, 
                                                     bson_data['o']['$set'],
                                                     bson_data['o2'])
        res = []
        for name, table in tables.iteritems():
            res.append(OplogQuery("ui", \
                generate_insert_queries(table, "", "", initial_indexes)))
        assert(res!=[])
        return res
    else:
        res = []
        cb_res = update(schema_engine.schema, bson_data)
        for it in cb_res:
            for op in it:
                res.append(OplogQuery('u', (op, it[op])))
        return res
     
def cb_delete(ts, ns, schema, bson_data):
    id_str = str(bson_data['_id'])
    cb_res = op_delete_stmts(schema.schema,ns.split('.')[-1],id_str)
    res = []
    for op in cb_res:
        for stmnt in cb_res[op]:
            res.append(OplogQuery('d', (stmnt, [tuple(cb_res[op][stmnt])])))
    return res

def cb_before(dbreq, schema_engine, item):
    """ Needed for oplog syncing.
    When handling oplog records during oplog sync,
    it's can be needed at first to copy data into 
    operational database. Hadnled oplog records must exec
    queries in operational database also."""
    if not hasattr(cb_before, "ids"):
        cb_before.ids = []
    tables_obj = create_tables_load_bson_data(schema_engine,
                                               None)
    src_schema_name = ''
    dst_schema_name = TMP_SCHEMA_NAME
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

def cb_after(ext_arg, schema_engine, item):
    """ Needed for oplog syncing.
    Compare actual mongo record with record's relational model 
    from operational tables."""
    dbreq = ext_arg[0]
    mongo_reader = ext_arg[1]
    # when after callback coming with item's value=None then 
    # no more records in oplog and need to return either 
    # get resulted oplog's tail or start new iteration
    if not item:
        is_bad = False
        for rec_id in cb_after.ids_res:
            id_results = cb_after.ids_res[rec_id]
            for id_res in id_results:
                ts = id_res[0]
                res = id_res[1]
                if res is False:
                    is_bad = True
                    print "ts", ts, "res=", res
        return not is_bad
               
    ts = item['ts']
    if not hasattr(cb_after, "ids_res"):
        cb_after.ids_res = {}
    tables_obj = create_tables_load_bson_data(schema_engine,
                                               None)
    src_schema_name = ''
    dst_schema_name = TMP_SCHEMA_NAME
    rec_id = str(item['o'].values()[0])
    if item["op"] == "u":
        rec_id = str(item['o2'].values()[0])
    if rec_id not in cb_after.ids_res:
        cb_after.ids_res[rec_id] = []
    # retrieve actual mongo record and transform it to relational data
    mongo_reader.make_new_request(rec_id)
    rec = mongo_reader.next()
    if not rec:
        cb_after.ids_res[rec_id].append((ts, False))
    else:
        mongo_tables_obj = create_tables_load_bson_data(schema_engine, 
                                                        rec)
        psql_tables_obj = load_single_rec_into_tables_obj(dbreq, 
                                                          schema_engine,
                                                          TMP_SCHEMA_NAME,
                                                          rec_id)
        compare_res = mongo_tables_obj.compare(psql_tables_obj)
        # save result of comparison
        cb_after.ids_res[rec_id].append((ts, compare_res))
    return True


def sync_oplog(dbreq, test_ts):
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
    # create oplog parser
    schemas_path = "./test_data/schemas/rails4_mongoid_development"
    parser = OplogParser(oplog_reader, test_ts, schemas_path,
                         Callback(cb_before, 
                                  ext_arg = dbreq),
                         Callback(cb_after, 
                                  ext_arg = (dbreq, mongo_reader)),
                         Callback(cb_insert, ext_arg = None),
                         Callback(cb_update, ext_arg = None),
                         Callback(cb_delete, ext_arg = None))

    # create/truncate psql operational tables
    # which are using during oplog tail lookup
    if not hasattr(cb_before, "ids") or not len(cb_before.ids):
        posts_no_data = create_tables_load_bson_data(\
             parser.schema_engines['posts'], 
             None)
        for table_name, table in posts_no_data.tables.iteritems():
            drop_table = generate_drop_table_statement(table, 
                                                       TMP_SCHEMA_NAME, '')
            create_table = generate_create_table_statement(table, 
                                                           TMP_SCHEMA_NAME, '')
            dbreq.cursor.execute(drop_table)
            dbreq.cursor.execute(create_table)
        dbreq.cursor.execute('COMMIT')

    # go over oplog, handle oplog records and execute final queries
    # oplog_queries it's result of handling of single oplog record
    # and all these queries should be executed in psql
    oplog_queries = parser.next()
    while oplog_queries != None:
         for oplog_query in oplog_queries:
             print oplog_query
             if oplog_query.op == "u":
                 exec_insert(dbreq, oplog_query)
             elif oplog_query.op == "d":
                 exec_insert(dbreq, oplog_query)
             elif oplog_query.op == "i" or oplog_query.op == "ui":
                 exec_insert(dbreq, oplog_query)
         oplog_queries = parser.next()
         
    if parser.status is False:
         # start test from next ts after test_ts
         if parser.first_handled_ts:
              # next ts to try for sync
              return parser.first_handled_ts 
         else:
              # if no more tss then it's fail, not able to sync psql & oplog
              return None
    elif not parser.first_handled_ts:
         # sync succesfull
         dbreq.cursor.execute('COMMIT')
         return True
    

def test_oplog_sync():
    connstr = os.environ['TEST_PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))

    # create skeleton or psql tables as initial load was not
    # executed previously.
    schemas_path = "./test_data/schemas/rails4_mongoid_development"
    schema_engines = get_schema_engines_as_dict(schemas_path)
    for schema_name, schema in schema_engines.iteritems():
        tables_obj = create_tables_load_bson_data(schema,
                                                  None)
        for table_name, table in tables_obj.tables.iteritems():
            query = generate_create_table_statement(table, '', '')
            dbreq.cursor.execute(query)

    # oplog_ts_to_test is timestamp starting from which oplog records 
    # should be applied to psql tables to locate ts which corresponds to 
    # initially loaded psql data; 
    # None - means oplog records should be tested starting from beginning 
    oplog_ts_to_test = None
    res = sync_oplog(dbreq, oplog_ts_to_test)
    while True:
        print 'final res', res
        if res is None:
            print "Not able to sync psql & oplog"
            assert(0)
            break
        elif res is True:
            # sync is ok
            break
        else:
            oplog_ts_to_test = res
        res = sync_oplog(dbreq, oplog_ts_to_test)
         

if __name__ == '__main__':
    test_oplog_sync()

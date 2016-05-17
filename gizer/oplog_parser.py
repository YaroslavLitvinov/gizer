#!/usr/bin/env python

""" Oplog parser, and patcher of end data by oplog operations.
Oplog synchronization with initially loaded data stored in psql.
OplogParser -- class for basic oplog parsing
apply_oplog_recs_after_ts -- handling oplog and applying oplog ops func
sync_oplog -- find syncronization point in oplog for initially loaded data."""

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import bson
from collections import namedtuple
from gizer.oppartial_record import get_tables_data_from_oplog_set_command
from gizer.psql_objects import load_single_rec_into_tables_obj
from gizer.psql_objects import insert_rec_from_one_tables_set_to_another
from gizer.psql_objects import create_psql_tables
from gizer.oplog_handlers import cb_insert
from gizer.oplog_handlers import cb_update
from gizer.oplog_handlers import cb_delete
from gizer.all_schema_engines import get_schema_engines_as_dict
from mongo_schema.schema_engine import create_tables_load_bson_data

Callback = namedtuple('Callback', ['cb', 'ext_arg'])
ItemInfo = namedtuple('ItemInfo', ['schema_name',
                                   'schema_engine',
                                   'ts',
                                   'rec_id'])

class OplogParser:
    """ parse oplog data, apply oplog operations, execute resulted queries
    and verify patched results """
    def __init__(self, reader, start_after_ts, schemas_path,
                 cb_bef, cb_ins, cb_upd, cb_del):
        self.reader = reader
        self.start_after_ts = start_after_ts
        self.first_handled_ts = None
        self.schema_engines = get_schema_engines_as_dict(schemas_path)
        self.item_info = None
        self.cb_before = cb_bef
        self.cb_insert = cb_ins
        self.cb_update = cb_upd
        self.cb_delete = cb_del

    def next_verified(self):
        """ next oplog records for one of ops=u,i,d """
        item = self.reader.next()
        while item:
            if item['op'] == 'i' or item['op'] == 'u' or item['op'] == 'd':
                if item and item['ts'] > self.start_after_ts:
                    return item
            item = self.reader.next()
        return None

    def next(self):
        item = self.next_verified()
        res = None
        if item:
            if self.first_handled_ts is None:
                self.first_handled_ts = item['ts']
            ts_field = item["ts"]
            ns_field = item["ns"]
            o_field = item["o"]
            # get rec_id
            rec_id = None
            if item["op"] == "u":
                rec_id = str(item['o2'].values()[0])
            else:
                if '_id' in item['o']:
                    rec_id = item['o']['_id']
                elif 'id' in item['o']:
                    rec_id = item['o']['id']
                else:
                    assert(0)
            if type(rec_id) is bson.objectid.ObjectId:
                rec_id = str(rec_id)

            db_and_collection = item["ns"].split('.')
            # dbname = db_and_collection[0]
            schema_name = db_and_collection[1]
            schema = self.schema_engines[schema_name]
            # save rec_id
            self.item_info = ItemInfo(schema_name,
                                      schema,
                                      item['ts'],
                                      rec_id)

            if self.cb_before:
                self.cb_before.cb(self.cb_before.ext_arg,
                                  schema,
                                  item)
            if item["op"] == "i":
                # insert is ALWAYS expects array of records
                res = self.cb_insert.cb(self.cb_insert.ext_arg,
                                        ts_field, ns_field, schema,
                                        [o_field])
            elif item["op"] == "u":
                o2_id = item["o2"]
                res = self.cb_update.cb(self.cb_update.ext_arg,
                                        schema, item)
            elif item["op"] == "d":
                res = self.cb_delete.cb(self.cb_delete.ext_arg,
                                        ts_field, ns_field, schema,
                                        o_field)
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


def exec_insert(dbreq, oplog_query):
    # create new connection and cursor
    query = oplog_query.query
    fmt_string = query[0]
    for sqlparams in query[1]:
        dbreq.cursor.execute(fmt_string, sqlparams)

def compare_psql_and_mongo_records(dbreq, mongo_reader, schema_engine, rec_id,
                                   dst_schema_name):
    """ Return True/False. Compare actual mongo record with record's relational
    model from operational tables. Comparison of non existing objects gets True.
    dbreq -- psql cursor wrapper
    mongo_reader - mongo cursor wrapper
    schema_engine -- 'SchemaEngine' object
    rec_id - record id to compare
    dst_schema_name -- psql schema name where psql tables store that record"""
    res = None
    psql_tables_obj = load_single_rec_into_tables_obj(dbreq,
                                                      schema_engine,
                                                      dst_schema_name,
                                                      rec_id)
    mongo_tables_obj = None
    # retrieve actual mongo record and transform it to relational data
    mongo_reader.make_new_request(rec_id)
    rec = mongo_reader.next()
    if not rec:
        if psql_tables_obj.is_empty():
            # comparison of non existing objects gets True
            res= True
        else:
            res = False
    else:
        mongo_tables_obj = create_tables_load_bson_data(schema_engine,
                                                        [rec])
        compare_res = mongo_tables_obj.compare(psql_tables_obj)
        # save result of comparison
        res = compare_res
    print "rec_id=", rec_id, "compare res=", res
    if not res:
        print "psql_tables_obj", psql_tables_obj.tables
        try:
            print "mongo_tables_obj", mongo_tables_obj.tables
        except:
            print "mongo_tables_obj", None
    return res

def create_truncate_psql_objects(dbreq, schemas_path, psql_schema):
    """ drop and create tables for all collections """
    schema_engines = get_schema_engines_as_dict(schemas_path)
    for schema_name, schema in schema_engines.iteritems():
        tables_obj = create_tables_load_bson_data(schema, None)
        drop = True
        create_psql_tables(tables_obj, dbreq, psql_schema, '', drop)

def apply_oplog_recs_after_ts(start_ts, psql, mongo, oplog, schemas_path,
                              psql_schema_to_apply_ops,
                              psql_schema_initial_load):
    """ Read oplog operations starting from timestamp start_ts. Then do copy
    of records mentioned in these oplog operations, from main psql schema
    to operational psql schema. Then apply oplog operations to psql data
    and compare results with latest data directly taken from mongo db.
    Return True if oplog operations applied succesfully, or False if
    verification failed. This function is using OplogParser itself.
    params:
    start_ts -- Timestamp of record in oplog db which should be
    applied first or next available
    psql -- Postgres cursor wrapper
    mongo -- Mongo cursor wrappper
    oplog -- Mongo oplog cursor wrappper
    schemas_path -- Path with js schemas representing mongo collections
    psql_schema_to_apply_ops -- psql schema which tables data will be patched.
    psql_schema_initial_load -- optional param, psql schema which data is
    using as source data for copying into tables of psql_schema_to_apply_ops
    where data will be pacthed by oplog operations. If not specified then data
    in psql_schema_to_apply_ops will be patched directly without preparing."""
    handled_mongo_rec_ids = {} # {collection: [rec list]}
    if psql_schema_initial_load is None:
        callback_before = None
    else:
        callback_before = cb_before
    # create oplog parser
    parser = OplogParser(oplog, start_ts, schemas_path, \
                Callback(callback_before,
                         ext_arg=(psql,
                                  psql_schema_initial_load,
                                  psql_schema_to_apply_ops)),
                Callback(cb_insert, ext_arg=psql_schema_to_apply_ops),
                Callback(cb_update, ext_arg=(psql,
                                             psql_schema_to_apply_ops)),
                Callback(cb_delete, ext_arg=(psql,
                                             psql_schema_to_apply_ops)))
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
            equal = compare_psql_and_mongo_records(psql, mongo,
                                                   schema_engine,
                                                   rec_id,
                                                   psql_schema_to_apply_ops)
            if not equal:
                sync_ok = False
                break
    if parser.first_handled_ts:
        return (parser.first_handled_ts, sync_ok)
    else:
        return (None, False)

def sync_oplog(test_ts, dbreq, mongo, oplog, schemas_path,
               psql_schema_to_apply_ops, psql_schema_initial_load):
    """ Return True if able to locate and synchronize initially loaded data
    with oplog data. Find syncronization point in oplog for initially 
    loaded data. 
    start_ts -- Timestamp of oplog record to start sync tests
    dbreq -- Postgres cursor wrapper
    mongo -- Mongo cursor wrappper
    oplog -- Mongo oplog cursor wrappper
    schemas_path -- Path with js schemas representing mongo collections
    psql_schema_to_apply_ops -- psql schema which tables data will be patched.
    psql_schema_initial_load -- psql schema which data is
    using as source data for copying into tables of psql_schema_to_apply_ops
    where data will be pacthed by oplog operations."""
    # create/truncate psql operational tables
    # which are using during oplog tail lookup
    create_truncate_psql_objects(dbreq, schemas_path, psql_schema_to_apply_ops)
    ts_sync = apply_oplog_recs_after_ts(test_ts, dbreq, mongo, oplog,
                                        schemas_path,
                                        psql_schema_to_apply_ops,
                                        psql_schema_initial_load)
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

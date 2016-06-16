#!/usr/bin/env python

""" Oplog parser, and patcher of end data by oplog operations.
Oplog synchronization with initially loaded data stored in psql.
OplogParser -- class for basic oplog parsing
do_oplog_apply -- handling oplog and applying oplog ops func
sync_oplog -- find syncronization point in oplog for initially loaded data."""

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import bson
import sys
import pprint
from logging import getLogger
from os import environ
from bson.json_util import loads
from collections import namedtuple
from gizer.oppartial_record import get_tables_data_from_oplog_set_command
from gizer.psql_objects import load_single_rec_into_tables_obj
from gizer.psql_objects import insert_rec_from_one_tables_set_to_another
from gizer.psql_objects import create_psql_tables
from gizer.oplog_parser import OplogParser
from gizer.oplog_parser import exec_insert
from gizer.oplog_handlers import cb_insert
from gizer.oplog_handlers import cb_update
from gizer.oplog_handlers import cb_delete
from gizer.etlstatus_table import timestamp_str_to_object
from gizer.all_schema_engines import get_schema_engines_as_dict
from mongo_reader.prepare_mongo_request import prepare_mongo_request
from mongo_reader.prepare_mongo_request import prepare_oplog_request
from mongo_schema.schema_engine import create_tables_load_bson_data
from mongo_schema.schema_engine import log_table_errors

Callback = namedtuple('Callback', ['cb', 'ext_arg'])
OplogApplyRes = namedtuple('OplogApplyRes', 
                           ['ts', # oplog timestamp
                            'res' # True/False res
                            ])

def compare_psql_and_mongo_records(psql, mongo_reader, schema_engine, rec_id,
                                   dst_schema_name):
    """ Return True/False. Compare actual mongo record with record's relational
    model from operational tables. Comparison of non existing objects gets True.
    psql -- psql cursor wrapper
    mongo_reader - mongo cursor wrapper tied to specific collection
    schema_engine -- 'SchemaEngine' object
    rec_id - record id to compare
    dst_schema_name -- psql schema name where psql tables store that record"""
    res = None
    mongo_tables_obj = None
    psql_tables_obj = load_single_rec_into_tables_obj(psql,
                                                      schema_engine,
                                                      dst_schema_name,
                                                      rec_id)
    # retrieve actual mongo record and transform it to relational data
    query = prepare_mongo_request(schema_engine, rec_id)
    mongo_reader.make_new_request(query)
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
        if not compare_res:
            collection_name = mongo_tables_obj.schema_engine.root_node.name
            log_table_errors("collection: %s data load from MONGO with errors:" \
                                 % collection_name,
                             mongo_tables_obj.errors)
            log_table_errors("collection: %s data load from PSQL with errors:" \
                                 % collection_name, 
                             psql_tables_obj.errors)
            getLogger(__name__).debug('cmp rec=%s res=False mongo arg[1] data:' % 
                                      str(rec_id))
            for line in str(mongo_tables_obj.tables).splitlines():
                getLogger(__name__).debug(line)
            getLogger(__name__).debug('cmp rec=%s res=False psql arg[2] data:' % 
                                      str(rec_id))
            for line in str(psql_tables_obj.tables).splitlines():
                getLogger(__name__).debug(line)

        # save result of comparison
        res = compare_res
    return res


class OplogHighLevel:
    def __init__(self, psql, mongo_readers, oplog,
                 schemas_path, schema_engines, psql_schema):
        """ params:
        psql -- Postgres cursor wrapper
        mongo_readers -- dict of mongo readers, one per collection
        oplog -- Mongo oplog cursor wrappper
        schemas_path -- Path with js schemas representing mongo collections
        psql_schema -- psql schema whose tables data to patch."""
        self.psql = psql
        self.mongo_readers = mongo_readers
        self.oplog = oplog
        self.schemas_path = schemas_path
        self.schema_engines = schema_engines
        self.psql_schema = psql_schema
        self.comparator = ComparatorMongoPsql(schema_engines,
                                              mongo_readers,
                                              psql,
                                              psql_schema)

    def do_oplog_apply(self, start_ts, doing_sync):
        """ Read oplog operations starting just after timestamp start_ts.
        Apply oplog operations to psql db.
        Compare source (mongo) and dest(psql) records.
        Return named tuple - Self.OplogApplyRes. Where:
        Self.OplogApplyRes.ts is ts to apply operations.
        Self.OplogApplyRes.res is result of applying self.oplog operations.
        False - apply failed.
        This function is using Self.OplogParser itself.
        params:
        start_ts -- Timestamp of record in self.oplog db which should be
        applied first read ts or next available
        doing_sync -- if True then don't commit/rollback, 
        if False do commit on success, rollback on fail"""

        getLogger(__name__).\
                info('Apply oplog operation ts:%s' % str(start_ts))
        compare_rec_ids = {} # {collection: {rec_ids: bool}}
        self.oplog_query = prepare_oplog_request(start_ts)
        self.oplog.make_new_request(self.oplog_query)
        # create oplog parser. note: cb_insert doesn't need psql object
        parser = OplogParser(self.oplog, self.schemas_path, \
                    Callback(cb_insert, ext_arg=self.psql_schema),
                    Callback(cb_update, ext_arg=(self.psql, self.psql_schema)),
                    Callback(cb_delete, ext_arg=(self.psql, self.psql_schema)))
        # go over oplog, and apply all oplog pacthes starting from start_ts
        last_ts = None
        oplog_queries = parser.next()
        while oplog_queries != None:
            for oplog_query in oplog_queries:
                if oplog_query.op == "u" or \
                   oplog_query.op == "d" or \
                   oplog_query.op == "i" or oplog_query.op == "ui":
                    exec_insert(self.psql, oplog_query)
                    collection_name = parser.item_info.schema_name
                    rec_id = parser.item_info.rec_id
                    last_ts = parser.item_info.ts
                    self.comparator.add_to_compare(collection_name, rec_id)
            oplog_queries = parser.next()
        # compare mongo data & psql data after self.oplog records applied
        compare_res = self.comparator.compare_src_dest()
        getLogger(__name__).\
                info('oplog ts:%s operation applied res=%s' % (str(start_ts),
                                                               str(compare_res)))
        if not doing_sync:
            if compare_res:
                getLogger(__name__).info('COMMIT')
                self.psql.conn.commit()
            else:
                getLogger(__name__).info('ROLLBACK')
                self.psql.conn.rollback()

        if compare_res and not parser.first_handled_ts:
            # no oplog records to apply
            return OplogApplyRes(start_ts, True)
        else:
            if compare_res:
                # oplog apply ok, return last appliued ts
                return OplogApplyRes(last_ts, True)
            else:
                # oplog apply error, return next ts candidate
                return OplogApplyRes(parser.first_handled_ts, False)

    def do_oplog_sync(self, ts):
        """ Oplog sync is using local psql database with all data from main psql db
        for applying test patches from mongodb oplog. It's expected high intensive
        queries execution flow. The result of synchronization would be a single
        timestamp from oplog which is last operation applied to data which resides
        in main psql database. If TS is not located then synchronization failed.
        do oplog sync, return ts - last ts which is part of initilly loaded data
        params:
        ts -- oplog timestamp which is start point to locate sync point"""
    
        getLogger(__name__).\
                info('Start oplog synchronising from ts:%s' % str(ts))
        schema_engines = get_schema_engines_as_dict(self.schemas_path)
    
        # oplog_ts_to_test is timestamp starting from which oplog records
        # should be applied to psql tables to locate ts which corresponds to
        # initially loaded psql data;
        # None - means oplog records should be tested starting from beginning
        oplog_ts_to_test = ts
        sync_res = self._sync_oplog(oplog_ts_to_test)
        while True:
            if sync_res is False or sync_res is True:
                break
            else:
                oplog_ts_to_test = sync_res
            sync_res = self._sync_oplog(oplog_ts_to_test)
        getLogger(__name__).\
            info('oplog ts:%s sync res=%s' % (str(ts),
                                              str(sync_res)))

        if sync_res:
            # if oplog sync point is located at None, then all oplog ops
            # must be applied starting from first ever ts
            if not oplog_ts_to_test:
                getLogger(__name__).\
                    info('Already synchronised ts:%s' % str(ts))
                return True
            else:
                getLogger(__name__).\
                    info('Sync point located ts:%s' % str(oplog_ts_to_test))
                return oplog_ts_to_test
        else:
            getLogger(__name__).error('Sync failed.')
            return None
    

    def _sync_oplog(self, test_ts):
        """ Find syncronization point of oplog and psql data
        (which usually is initially loaded data.)
        Return True if able to locate and synchronize initially loaded data
        with oplog data, or return next ts candidate for syncing.
        start_ts -- Timestamp of oplog record to start sync tests"""
        ts_sync = self.do_oplog_apply(test_ts, doing_sync=True)
        if ts_sync.res == True:
            # sync succesfull
            getLogger(__name__).info('COMMIT')
            self.psql.conn.commit()
            return True
        else:
            # continue syncing, revert to original data
            getLogger(__name__).info('ROLLBACK')
            self.psql.conn.rollback()
            if ts_sync.ts:
                getLogger(__name__).info("Bad sync candidate ts:" + str(test_ts) +
                                         ", try next ts=" + str(ts_sync.ts))
                # next sync iteration, must start after ts_sync.ts
                return ts_sync.ts
            else:
                return False

class ComparatorMongoPsql:

    def __init__(self, schema_engines, mongo_readers, psql, psql_schema):
        self.schema_engines = schema_engines
        self.mongo_readers = mongo_readers
        self.psql = psql
        self.psql_schema = psql_schema
        self.recs_to_compare = {}

    def add_to_compare(self, collection_name, rec_id):
        if collection_name not in self.recs_to_compare:
            self.recs_to_compare[collection_name] = {}
        if rec_id not in self.recs_to_compare[collection_name]:
            self.recs_to_compare[collection_name][rec_id] = False # by default

    def compare_one_src_dest(self, collection_name, rec_id):
        schema_engine = self.schema_engines[collection_name]
        mongo_reader = self.mongo_readers[collection_name]
        getLogger(__name__).info("comparing... rec_id=" + str(rec_id))
        equal = compare_psql_and_mongo_records(self.psql,
                                               mongo_reader,
                                               schema_engine,
                                               rec_id,
                                               self.psql_schema)
        getLogger(__name__).info("compare res=" + str(equal) + 
                                 " for rec_id=" + str(rec_id))
        return equal

    def compare_src_dest(self):
        getLogger(__name__).info('Oplog ops applied. Compare following recs: %s'\
                                     % self.recs_to_compare )
        # compare mongo data & psql data after self.oplog records applied
        for collection_name, recs in self.recs_to_compare.iteritems():
            for rec_id in recs:
                equal = self.compare_one_src_dest(collection_name, rec_id)
                self.recs_to_compare[collection_name][rec_id] = equal
                if not equal:
                    return False
        return True

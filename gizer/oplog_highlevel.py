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
from gizer.psql_objects import load_single_rec_into_tables_obj
from gizer.psql_objects import insert_rec_from_one_tables_set_to_another
from gizer.oplog_parser import OplogParser
from gizer.oplog_parser import exec_insert
from gizer.oplog_handlers import cb_insert
from gizer.oplog_handlers import cb_update
from gizer.oplog_handlers import cb_delete
from gizer.etlstatus_table import timestamp_str_to_object
from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.opmultiprocessing import FastQueueProcessor
from gizer.etl_mongo_reader import EtlMongoReader
from mongo_reader.prepare_mongo_request import prepare_mongo_request
from mongo_reader.prepare_mongo_request import prepare_oplog_request
from mongo_reader.prepare_mongo_request import prepare_mongo_request_for_list
from mongo_schema.schema_engine import create_tables_load_bson_data
from mongo_schema.schema_engine import log_table_errors

DO_OPLOG_READ_ATTEMPTS_COUNT = 100
MONGO_REC_PROCESSING_PROCESS_COUNT = 8
FAST_QUEUE_SIZE = MONGO_REC_PROCESSING_PROCESS_COUNT*2

CompareRes = namedtuple('CompareRes', ['rec_id', 'flag', 'attempt'])
Callback = namedtuple('Callback', ['cb', 'ext_arg'])
OplogApplyRes = namedtuple('OplogApplyRes', 
                           ['handled_count', # handled oplog records (ops=u,i,d)
                            'queries_count', # sql queries executed
                            'ts', # oplog timestamp
                            'res' # True/False res
                            ])

def async_worker_handle_mongo_rec(schema_engines,
                                  rec_data_and_collection):
    """ function intended to call by FastQueueProcessor.
    process mongo record / bson data in separate process.
    schema_engines -- dict {'collection name': SchemaEngine}. Here is
    many schema engines to use every queue to handle items from any collection;
    rec_data_and_collection - tuple('collection name', bson record)"""
    rec = rec_data_and_collection[0]
    collection = rec_data_and_collection[1]
    return create_tables_load_bson_data(schema_engines[collection],
                                        [rec])

def cmp_psql_mongo_tables(rec_id, mongo_tables_obj, psql_tables_obj):
    """ Return True/False. Compare actual mongo record with record's relational
    model from operational tables. Comparison of non existing objects gets True.
    psql_tables_obj -- 
    mongo_tables_obj -- """
    res = None
    if psql_tables_obj.is_empty() and mongo_tables_obj.is_empty():
        # comparison of non existing objects gets True
        res= True
    else:
        compare_res = mongo_tables_obj.compare(psql_tables_obj)
        if not compare_res:
            collection_name = mongo_tables_obj.schema_engine.root_node.name
            log_table_errors("collection: %s data load from MONGO with errors:" \
                                 % collection_name, mongo_tables_obj.errors)
            log_table_errors("collection: %s data load from PSQL with errors:" \
                                 % collection_name, psql_tables_obj.errors)
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
    def __init__(self, psql, mongo_readers, oplog_readers,
                 schemas_path, schema_engines, psql_schema):
        """ params:
        psql -- Postgres cursor wrapper
        mongo_readers -- dict of mongo readers, one per collection
        oplog -- Mongo oplog cursor wrappper
        schemas_path -- Path with js schemas representing mongo collections
        psql_schema -- psql schema whose tables data to patch."""
        self.psql = psql
        self.mongo_readers = mongo_readers
        self.oplog_readers = oplog_readers
        self.schemas_path = schemas_path
        self.schema_engines = schema_engines
        self.psql_schema = psql_schema
        self.comparator = ComparatorMongoPsql(schema_engines,
                                              mongo_readers,
                                              psql,
                                              psql_schema)

    def get_ts_rec_ids(self, start_ts):
        getLogger(__name__).\
            info('get_ts_rec_ids from timestamps located after ts:%s' \
                     % str(start_ts))
        ts_rec_ids = {}
        js_oplog_query = prepare_oplog_request(start_ts)
        for name in self.oplog_readers:
            self.oplog_readers[name].make_new_request(js_oplog_query)
        # create oplog parser. note: cb_insert doesn't need psql object
        parser = OplogParser(self.oplog_readers, self.schemas_path,
                             Callback(cb_insert, ext_arg=self.psql_schema),
                             Callback(cb_update, 
                                      ext_arg=(self.psql, self.psql_schema)),
                             Callback(cb_delete, 
                                      ext_arg=(self.psql, self.psql_schema)),
                             dry_run = True)
        # go over oplog get just rec ids for timestamps that can be handled
        oplog_queries = parser.next()
        while oplog_queries != None:
            collection_name = parser.item_info.schema_name
            rec_id = parser.item_info.rec_id
            if collection_name not in ts_rec_ids:
                ts_rec_ids[collection_name] = []
            if rec_id not in ts_rec_ids[collection_name]:
                ts_rec_ids[collection_name].append(rec_id)
            oplog_queries = parser.next()
        return ts_rec_ids

    def _sync_single_rec_id(self, start_ts, collection, rec_id):
        """ do sync for single rec id, startinf just after start_ts.
        all timestamps not related to specified rec_id will be skipped """
        getLogger(__name__).info("sync single %s rec_id=%s"
                                 % (collection, rec_id))
        test_ts = start_ts
        for name in self.oplog_readers:
            # will affect only mock test reader
            self.oplog_readers[name].reset_dataset() 
        ts_sync = self.do_oplog_apply(test_ts, collection, rec_id,
                                      doing_sync=True)
        while True:
            if not ts_sync.res and not ts_sync.ts:
                break
            if ts_sync.res:
                break
            getLogger(__name__).info("sync single next iteration")
            self.psql.conn.rollback()
            test_ts = ts_sync.ts
            for name in self.oplog_readers:
                # will affect only mock test reader
                self.oplog_readers[name].reset_dataset() 
            ts_sync = self.do_oplog_apply(test_ts, collection, rec_id,
                                          doing_sync=True)
        res = None
        if ts_sync.res:
            res = test_ts
        getLogger(__name__).info("sync single res %s, ts:%s"
                                 % (str(ts_sync.res), str(res)))

        return res

    def fast_sync_oplog(self, start_ts):
        """ do sync sequentually for every rec id in separate"""
        def recs_count(dict_list):
            count = 0
            for key in dict_list:
                count += len(dict_list[key])
            return count

        min_ts = None
        ts_rec_ids = self.get_ts_rec_ids(start_ts)
        total = recs_count( ts_rec_ids )
        getLogger(__name__).info("all rec ids to sync: %s" % str(ts_rec_ids))
        for collection in ts_rec_ids:
            while ts_rec_ids[collection]:
                rec_id = ts_rec_ids[collection].pop()
                ts = self._sync_single_rec_id(start_ts, collection, rec_id)
                rest = recs_count( ts_rec_ids )
                getLogger(__name__).info("sync single progress %d / %d" %
                                         (total - rest, total))
                if not min_ts or (ts and ts < min_ts):
                    getLogger(__name__).info("fast: min_ts %s < ts %s" %
                                             (ts, min_ts))
                    min_ts = ts
        # certainly located either sync point or much closest point to sync
        return min_ts

    def do_oplog_apply(self, start_ts, 
                       filter_collection, 
                       filter_rec_id, 
                       doing_sync):
        """ Read oplog operations starting just after timestamp start_ts.
        Apply oplog operations to psql db. After all records are applied do
        consistency check by comparing source (mongo) and dest(psql) records.
        If doing_sync is false and consistency check is successful then
        do commit, else do failover on fail. Failover here means - delete
        corrupted records from psql, insert their relational model into psql
        and do consistency check again. If after that check fails then
        do rollback of psql data and get error.
        Return named tuple - OplogApplyRes. Where:
        OplogApplyRes.ts is ts to apply operations.
        OplogApplyRes.res is result of applying oplog operations.
        False - apply failed.
        The function is using OplogParser itself.
        params:
        start_ts -- Timestamp of record in oplog db which should be
        applied first read ts or next available
        doing_sync -- if True then operate as part of sync stage and
        don't do commits/rollbacks; if False then operate in acc to desc."""

        getLogger(__name__).\
            info('Apply oplog operation going after ts:%s' % str(start_ts))
        start_ts_backup = start_ts

        # fetch parser.first_handled_ts and save to local first_handled_ts
        # to be able to return it as parser will miss that value at recreating
        first_handled_ts = None
        do_again_counter = 0
        temp_data = self.oplog_readers[self.oplog_readers.keys()[0]]
        do_again = True
        while do_again:
            # reset 'apply again', it's will be enabled again if needed
            do_again = False
            if filter_collection and filter_rec_id and temp_data.real_transport():
                # section not for mock
                dbname = temp_data.settings_list[0].dbname
                js_oplog_query = prepare_oplog_request_filter(start_ts, 
                                                              dbname, 
                                                              filter_collection, 
                                                              filter_rec_id)
            else:
                js_oplog_query = prepare_oplog_request(start_ts)
            for name in self.oplog_readers:
                self.oplog_readers[name].make_new_request(js_oplog_query)
            # create oplog parser. note: cb_insert doesn't need psql object
            parser = OplogParser(self.oplog_readers, self.schemas_path,
                                 Callback(cb_insert, ext_arg=self.psql_schema),
                                 Callback(cb_update, 
                                          ext_arg=(self.psql, self.psql_schema)),
                                 Callback(cb_delete, 
                                          ext_arg=(self.psql, self.psql_schema)),
                                 dry_run = False)
            # go over oplog, and apply all oplog pacthes starting from start_ts
            last_ts = None
            queries_counter = 0
            oplog_rec_counter = 0
            oplog_queries = parser.next()
            while oplog_queries != None:
                oplog_rec_counter += 1
                collection_name = parser.item_info.schema_name
                rec_id = parser.item_info.rec_id
                last_ts = parser.item_info.ts
                # filter out not matched recs
                if filter_collection and filter_rec_id:
                    if collection_name == filter_collection \
                            and rec_id == filter_rec_id:
                        if not first_handled_ts:
                            first_handled_ts = parser.item_info.ts
                        getLogger(__name__).info("fast first_handled_ts %s %s" 
                                                 % (str(parser.item_info.ts),
                                                    str(first_handled_ts)))
                    else:
                        oplog_queries = parser.next()
                        continue
                for oplog_query in oplog_queries:
                    queries_counter += 1
                    exec_insert(self.psql, oplog_query)
                    #last_ts = parser.item_info.ts
                    self.comparator.add_to_compare(collection_name, rec_id,
                                                   do_again_counter)
                oplog_queries = parser.next()
            getLogger(__name__).info("handled %d oplog records/ %d queries" % \
                                         (oplog_rec_counter, queries_counter))
            if not first_handled_ts:
                first_handled_ts = parser.first_handled_ts
            # compare mongo data & psql data after oplog records applied
            compare_res = self.comparator.compare_src_dest()
            # result of comparison can be negative if new oplog item had received
            # during checking results (comparing all records) then do double checks
            # so handle that case:
            if not compare_res and last_ts:
                if do_again_counter < DO_OPLOG_READ_ATTEMPTS_COUNT:
                    do_again = True
                    do_again_counter += 1
                    start_ts = last_ts
                    getLogger(__name__).warning('Do handling of newly added\
 oplog records, attempt=%d' % do_again_counter)
                else:
                    getLogger(__name__).warning('max attempt reads exceded %d' %
                                                DO_OPLOG_READ_ATTEMPTS_COUNT)
                    failed_attempts = self.comparator.get_failed_cmp_attempts()
                    getLogger(__name__).warning('failed attempts= %s' %
                                                str(failed_attempts))
                    # if compare failed just for most recent oplog data
                    # and for any other attempts compare is successfull
                    # then we can suppose that next cmp is also will be good
                    if len(failed_attempts) == 1 \
                            and do_again_counter in failed_attempts:
                        compare_res = True
                        getLogger(__name__).warning('Finish endless comparisons.\
 Force compare_res to be True.')
        getLogger(__name__).\
                info('Last oplog rec applied is ts:%s with res=%s' % 
                     (str(start_ts), str(compare_res)))
        if not doing_sync:
            if compare_res:
                getLogger(__name__).info('COMMIT')
                self.psql.conn.commit()
            else:
                getLogger(__name__).error('ROLLBACK')
                self.psql.conn.rollback()

        if compare_res and not first_handled_ts:
            # no oplog records to apply
            getLogger(__name__).info("do_oplog_apply: Nothing applied, \
no records after ts: %s" \
                                         % str(start_ts_backup))
            return OplogApplyRes(handled_count=oplog_rec_counter,
                                 queries_count=queries_counter,
                                 ts=start_ts,
                                 res=True)
        else:
            if compare_res:
                # oplog apply ok, return last applied ts
                getLogger(__name__).info("do_oplog_apply: Applied start_ts: %s, \
last_ts: %s" \
                                             % (str(start_ts_backup), 
                                                str(last_ts)))
                return OplogApplyRes(handled_count=oplog_rec_counter,
                                     queries_count=queries_counter,
                                     ts=last_ts,
                                     res=True)
            else:
                # if transport returned an error then keep the same ts_start
                # and return True, as nothing applied
                readers_failed = [(k, v.failed) for k,v in \
                                    self.comparator.mongo_readers.iteritems() \
                                    if v.failed]
                if parser.is_failed() or len(readers_failed):
                    if parser.is_failed():
                        getLogger(__name__).warning("do_oplog_apply: \
oplog transport failed")
                    if len(readers_failed):
                        getLogger(__name__).warning("do_oplog_apply: \
following mongo transports failed: %s" % (str(readers_failed)))
                        Exception('test')

                    getLogger(__name__).info('do_oplog_apply: Keep the same ts')
                    return OplogApplyRes(handled_count=oplog_rec_counter,
                                         queries_count=queries_counter,
                                         ts=start_ts_backup,
                                         res=True)
                else:
                    # oplog apply error, return next ts candidate
                    getLogger(__name__).info("do_oplog_apply: \
Bad apply for start_ts: %s, next candidate ts: %s"
                                             % (str(start_ts_backup), 
                                                str(first_handled_ts)))
                    return OplogApplyRes(handled_count=oplog_rec_counter,
                                         queries_count=queries_counter,
                                         ts=first_handled_ts,
                                         res=False)

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
        # oplog_ts_to_test is timestamp starting from which oplog records
        # should be applied to psql tables to locate ts which corresponds to
        # initially loaded psql data;
        # None - means oplog records should be tested starting from beginning
        oplog_ts_to_test = ts
        fast_ts = self.fast_sync_oplog(ts)
        getLogger(__name__).info("fast_sync ts is: %s" % fast_ts)
        if fast_ts:
            oplog_ts_to_test = fast_ts
        # with high probability ts is already located now
        # but to be sure run slow sync
        sync_res = self._sync_oplog(oplog_ts_to_test)
        while True:
            if sync_res is False or sync_res is True:
                break
            else:
                oplog_ts_to_test = sync_res
            sync_res = self._sync_oplog(oplog_ts_to_test)
        getLogger(__name__).\
            info('Located sync ts:%s (fast ts:%s) sync res=%s' 
                 % (str(oplog_ts_to_test), str(fast_ts), str(sync_res)))
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
        for name in self.oplog_readers:
            self.oplog_readers[name].reset_dataset() # will affect only mock test reader
        ts_sync = self.do_oplog_apply(test_ts, None, None, doing_sync=True)
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
        self.recs_to_compare_copy = {}
        self.etl_mongo_reader = EtlMongoReader(MONGO_REC_PROCESSING_PROCESS_COUNT,
                                               FAST_QUEUE_SIZE,
                                               async_worker_handle_mongo_rec,
                                               #1st worker param
                                               self.schema_engines, 
                                               self.mongo_readers)

    def __del__(self):
        del self.etl_mongo_reader

    def add_to_compare(self, collection_name, rec_id, attempt):
        if collection_name not in self.recs_to_compare:
            self.recs_to_compare[collection_name] = {}
        # every time item adding to compare list will reset old state
        # distinguish dict key and rec_id as rec_id can be a mongo object
        getLogger(__name__).info("%s is rec_id attempt=%d" % (str(rec_id), attempt))
        self.recs_to_compare[collection_name][str(rec_id)] \
            = CompareRes(rec_id, False, attempt)

    def compare_one_src_dest(self, collection_name, rec_id, 
                             mongo_tables_obj, psql_tables_obj):
        schema_engine = self.schema_engines[collection_name]
        mongo_reader = self.mongo_readers[collection_name]
        getLogger(__name__).info("comparing... rec_id=" + str(rec_id))
        equal = cmp_psql_mongo_tables(rec_id, mongo_tables_obj, psql_tables_obj)
        getLogger(__name__).info("compare res=" + str(equal) + 
                                 " for rec_id=" + str(rec_id))
        return equal

    def compare_src_dest(self):
        getLogger(__name__).info('Oplog ops applied. Compare following recs: %s'\
                                     % self.recs_to_compare )
        cmp_res = True
        # iterate mongo items belong to one collection
        for collection, recs in self.recs_to_compare.iteritems():
            # comparison strategy: filter out previously compared recs;
            # so will be compared only that items which never compared or
            # prev comparison gave False
            filtered_recs_list_cmp = []
            for rec_id, compare_res in recs.iteritems():
                if not compare_res.flag:
                    filtered_recs_list_cmp.append(compare_res.rec_id)
            # if nothing to compare just skip current collection
            if not filtered_recs_list_cmp:
                continue
            # prepare query
            mongo_query = prepare_mongo_request_for_list(
                self.schema_engines[collection], 
                filtered_recs_list_cmp)
            getLogger(__name__).info('mongo query to fetch recs to compare: %s',
                                     mongo_query)
            self.etl_mongo_reader.execute_query(collection, mongo_query)
            received_list = []
            # get and process records to compare
            processed_recs = self.etl_mongo_reader.next_processed()
            while processed_recs is not None:
                # do cmp for every returned obj
                for mongo_tables_obj in processed_recs:
                    rec_id = mongo_tables_obj.rec_id()
                    received_list.append(rec_id)
                    psql_tables_obj = load_single_rec_into_tables_obj(
                        self.psql,
                        self.schema_engines[collection],
                        self.psql_schema,
                        rec_id )
                    # this check makes sence ony for mock transport as it 
                    # will return all records and not only requested
                    key = str(rec_id)
                    if key in self.recs_to_compare[collection] and \
                            not self.recs_to_compare[collection][key].flag:
                        equal = self.compare_one_src_dest(collection,
                                                          rec_id,
                                                          mongo_tables_obj,
                                                          psql_tables_obj)
                        if not equal:
                            cmp_res = False
                    else:
                        continue
                    # update cmp result in main dict
                    attempt = self.recs_to_compare[collection][key].attempt
                    # update cmp result in main dict
                    self.recs_to_compare[collection][key] = \
                        CompareRes(rec_id, equal, attempt)
                processed_recs = self.etl_mongo_reader.next_processed()
            # should return True for deleted items (non existing items)
            for rec_id in filtered_recs_list_cmp:
                if rec_id not in received_list:
                    psql_tables_obj = load_single_rec_into_tables_obj(
                        self.psql,
                        self.schema_engines[collection],
                        self.psql_schema,
                        rec_id )
                    # if psql data also doesn't exist
                    if psql_tables_obj.is_empty():
                        key = str(rec_id)
                        attempt = self.recs_to_compare[collection][key].attempt
                        self.recs_to_compare[collection][key] = \
                            CompareRes(rec_id, True, attempt)                    
                        getLogger(__name__).info("cmp non existing rec_id %s."
                                                 % (str(rec_id)))

        return cmp_res

    def get_failed_cmp_attempts(self):
        failed_attempts_list = []
        for collection, recs in self.recs_to_compare.iteritems():
            for rec_id, compare_res in recs.iteritems():
                if not compare_res.flag and \
                        compare_res.attempt not in failed_attempts_list:
                    failed_attempts_list.append(compare_res.attempt)
        failed_attempts_list.sort()
        return failed_attempts_list
    

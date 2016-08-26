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
from mongo_reader.prepare_mongo_request import prepare_oplog_request_filter
from mongo_reader.prepare_mongo_request import prepare_mongo_request_for_list
from mongo_schema.schema_engine import create_tables_load_bson_data
from mongo_schema.schema_engine import log_table_errors

DO_OPLOG_READ_ATTEMPTS_COUNT = 100
MONGO_REC_PROCESSING_PROCESS_COUNT = 8
FAST_QUEUE_SIZE = MONGO_REC_PROCESSING_PROCESS_COUNT*2
SYNC_REC_COUNT_IN_PARALLEL = 100
SYNC_WORKERS_COUNT = 4


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
            log_table_errors("%s's MONGO rec load warns:" % collection_name,
                             mongo_tables_obj.errors)
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

def mock_transports_reset(oplog_readers):
    for name in oplog_readers:
        # will affect only mock test reader
        oplog_readers[name].reset_dataset() 

class TsData:
    def __init__(self, oplog_name, ts, queries):
        self.oplog_name = oplog_name
        self.ts = ts
        self.queries = queries
        self.sync_start = None

class PsqlCacheTable:
    PsqlCacheData = namedtuple('PsqlCacheData', ['ts', 'oplog', 'collection',
                                                 'queries',  'rec_id',
                                                 'sync_point'])
    def __init__(self, cursor, schema_name):
        self.cursor = cursor
        if len(schema_name):
            self.schema_name = schema_name + '.'
        else:
            self.schema_name = ''
        self.drop_table()
        self.create_table()
    
    def drop_table(self):
        fmt = 'DROP TABLE IF EXISTS {schema}qmetlcache;'
        self.cursor.execute( fmt.format(schema=self.schema_name) )
        
    def create_table(self):
        fmt = 'CREATE TABLE IF NOT EXISTS {schema}qmetlcache (\
        "ts" TEXT, "oplog" TEXT, "collection" TEXT, "queries" TEXT, \
        "rec_id" TEXT, "sync_point" BOOLEAN);'
        self.cursor.execute( fmt.format(schema=self.schema_name) )

    def insert(self, psql_cache_data):
        fmt = 'INSERT INTO {schema}qmetlcache VALUES(\
%s, %s, %s, %s, %s, %s, %s, %s);'
        operation_str = fmt.format(schema=self.schema_name)
        self.cursor.execute( operation_str,
                             (str(psql_cache_data.ts),
                              psql_cache_data.oplog,
                              psql_cache_data.collection,
                              repr(psql_cache_data.queries),
                              repr(psql_cache_data.rec_id),
                              psql_cache_data.sync_point) )
        self.cursor.execute('COMMIT;')

    def select_max_synced_ts_at_shard(self, oplog_name):
        max_sync_start_fmt="SELECT * from {schema}qmetlcache WHERE \
ts=MAX(ts) and oplog='{oplog}' and sync_point=TRUE;"
        select_query = max_sync_start_fmt.format(schema=self.schema_name,
                                                 oplog=oplog_name)
        self.cursor.execute(select_query)
        row = self.cursor.fetchone()
        res = self.convert_row_to_psql_cache_data(row)
        return res

    def select_ts_related_to_rec_id(self, collection, rec_id):
        res = []
        rec_tss_fmt="SELECT * from {schema}qmetlcache WHERE \
collection='{collection}' and rec_id='{rec_id}' ORDER BY ts;"
        select_query = rec_tss_fmt.format(schema=self.schema_name,
                                          collection=collection,
                                          rec_id=repr(rec_id))
        rows = self.cursor.fetchall()
        for row in rows:
            res.append(self.convert_row_to_psql_cache_data(row))
        return res

    def convert_row_to_psql_cache_data(self, row):
        import ast
        if row:
            return PsqlCacheTable.PsqlCacheData(
                ts=timestamp_str_to_object(row[0]),
                oplog=row[1],
                collection=row[2],
                queries=ast.literal_eval(row[3]),
                rec_id=ast.literal_eval(row[4]),
                sync_point=row[5])
        else:
            return None

class OplogHighLevel:
    def __init__(self, psql_etl, psql, mongo_readers, oplog_readers,
                 schemas_path, schema_engines, psql_schema):
        """ params:
        psql -- Postgres cursor wrapper
        mongo_readers -- dict of mongo readers, one per collection
        oplog -- Mongo oplog cursor wrappper
        schemas_path -- Path with js schemas representing mongo collections
        psql_schema -- psql schema whose tables data to patch."""
        self.psql_etl = psql_etl
        self.psql = psql
        self.mongo_readers = mongo_readers
        self.oplog_readers = oplog_readers
        self.schemas_path = schemas_path
        self.schema_engines = schema_engines
        self.psql_schema = psql_schema
        self.last_oplog_ts = {}
        self.comparator = ComparatorMongoPsql(schema_engines,
                                              mongo_readers,
                                              psql,
                                              psql_schema)

    def __del__(self):
        del self.comparator

    def get_rec_ids(self, start_ts_dict):
        """Return {collection_name: {rec_id: bool}} """
        res = {}
        getLogger(__name__).\
            info('get_rec_ids - locate timestamps after ts:%s' % start_ts_dict)
        projection = {"ts":1, "ns":1, "op":1, 
                      "o2._id":1, "o._id":1, "o2.id":1, "o.id":1}
        # use projection with query as parser is running in dryrun mode and
        # just need to get a list of ids related to timestamps
        for name in self.oplog_readers:
            # separate query for every shard
            js_oplog_query = prepare_oplog_request(start_ts_dict[name])
            self.oplog_readers[name].make_new_request(js_oplog_query,
                                                      projection)
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
            if collection_name not in res:
                res[collection_name] = {}
            res[collection_name][rec_id] = None
            oplog_queries = parser.next()
        return res

    def _sync_rec_list(self, start_ts, collection, rec_ids):
        """ do sync for single rec id, startinf just after start_ts.
        all timestamps not related to specified rec_ids will be skipped """
        getLogger(__name__).info("sync %s rec_ids=%s"
                                 % (collection, rec_ids))
        test_ts = start_ts
        for name in self.oplog_readers:
            # will affect only mock test reader
            self.oplog_readers[name].reset_dataset() 
        ts_sync = self.do_oplog_apply(test_ts, collection, rec_ids,
                                      doing_sync=True)
        self.psql.conn.rollback()
        while True:
            if not ts_sync.res and not ts_sync.ts:
                break
            if ts_sync.res:
                break
            getLogger(__name__).info("sync single next iteration")
            test_ts = ts_sync.ts
            for name in self.oplog_readers:
                # will affect only mock test reader
                self.oplog_readers[name].reset_dataset() 
            ts_sync = self.do_oplog_apply(test_ts, collection, rec_ids,
                                          doing_sync=True)
            self.psql.conn.rollback()
        # This rollback can guarantie that sync will not affect db data 
        self.psql.conn.rollback()
        res = None
        if ts_sync.res:
            res = test_ts
        getLogger(__name__).info("sync single res %s, ts:%s"
                                 % (str(ts_sync.res), str(res)))

        return res

    def fast_sync_oplog(self, start_ts_dict):
        """ Get syncronization point for every record of oplog and psql data
        (which usually is initially loaded data). 
        Return dict with timestamps if synchronization successfull, or None if not.
        params:
        start_ts_dict -- Dict with initial timestamps"""
        # create table to save map of syncronization for all records
        psql_cache_table = PsqlCacheTable(self.psql_etl.cursor,
                                          self.psql_schema)
        collection_rec_ids_dict = self.get_rec_ids(start_ts_dict)
        # get sync map. Every collection items have differetn sync point
        # and should be aligned
        for collection, rec_ids_dict in collection_rec_ids_dict.iteritems():
            sync_engine = OplogSyncEngine(start_ts_dict,
                                          collection, 
                                          self.schema_engines[collection],
                                          self.mongo_readers[collection],
                                          self.oplog_readers,
                                          self.psql,
                                          self.psql_schema,
                                          psql_cache_table)
            res = sync_engine.sync_map_for_collection(sync_engine, rec_ids_dict)
            if not res:
                return None
        # get max sync_start timestamp for every shard to align sync_point
        max_sync_ts = {}
        for oplog_name in self.oplog_readers:
            res = psql_cache_table.select_max_synced_ts_at_shard(oplog_name)
            if res:
                max_sync_ts[oplog_name] = res.ts
                getLogger(__name__).info("Max ts to apply for oplog=%s is %s" % 
                                         (max_ts_data_to_apply.oplog,
                                          max_ts_data_to_apply.ts))
            else:
                max_sync_ts[oplog_name] = None
        # iterate over all collections/rec_ids and execute rec related queries
        # up to speific alignment timestamp 
        for collection, rec_ids_dict in collection_rec_ids_dict.iteritems():
            query_exec_progress = 0
            for rec_id in rec_ids_dict:
                query_exec_progress += 1
                getLogger(__name__).info("exec query progress for %s is: %d / %d" %
                                         (collection,
                                          query_exec_progress,
                                          len(rec_ids_dict)))
                ts_cache = psql_cache_table.select_ts_related_to_rec_id(
                    collection, rec_id)
                if ts_cache.ts <= max_sync_ts[ts_cache.oplog]:
                    for oplog_query in ts_cache.queries:
                        exec_insert(self.psql, oplog_query)
        getLogger(__name__).info('COMMIT')
        self.psql.conn.commit()
        return max_sync_ts

    def prepare_oplog_request(self, ts, oplog_name, filter_collection, filter_rec_ids):
        collection_transport = self.mongo_readers[self.mongo_readers.keys()[0]]
        if filter_collection and filter_rec_ids \
                and collection_transport.real_transport():
            # section not for mock
            dbname = collection_transport.settings_list[0].dbname
            js_oplog_query = prepare_oplog_request_filter(ts, 
                                                          dbname, 
                                                          filter_collection, 
                                                          filter_rec_ids)
        else:
            # Use as ts last handled ts, as can't use the same ts for all readers
            if oplog_name in self.last_oplog_ts and self.last_oplog_ts[oplog_name]:
                ts = self.last_oplog_ts[oplog_name]
            js_oplog_query = prepare_oplog_request(ts)
        getLogger(__name__).info("Oplog request [%s]: %s",
                                 oplog_name, js_oplog_query)
        return js_oplog_query

    def do_oplog_apply(self, start_ts, 
                       filter_collection, 
                       filter_rec_ids, 
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
        do_again = True
        while do_again:
            # reset 'apply again', it's will be enabled again if needed
            do_again = False
            for name in self.oplog_readers:
                js_oplog_query = self.prepare_oplog_request(start_ts, 
                                                            name, 
                                                            filter_collection, 
                                                            filter_rec_ids)
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
                if filter_collection and filter_rec_ids:
                    if collection_name == filter_collection \
                            and rec_id in filter_rec_ids:
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
                    self.comparator.add_to_compare(collection_name, rec_id,
                                                   do_again_counter)
                oplog_queries = parser.next()
            getLogger(__name__).info("handled %d oplog records/ %d queries" % \
                                         (oplog_rec_counter, queries_counter))
            if not filter_collection:
                self.last_oplog_ts = parser.last_oplog_ts
            # save timestamps from temp OplogParser to self
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

    def do_oplog_sync(self, ts_start_dict):
        """ Sync oplog and postgres. The result of synchronization is a single
        timestamp from oplog. So if do apply to psql all timestamps going after
        that sync point and then compare affected psql and mongo records 
        they should be equal.  If TS is not located then synchronization failed.
        params:
        ts_start_dict -- dict contains one timestamp for every oplog shard
        which is start pos to find sync point"""
        getLogger(__name__).\
                info('Start oplog sync. Default ts:%s' % str(ts_start_dict))
        # ts_start is timestamp starting from which oplog records
        # should be applied to psql tables to locate ts which corresponds to
        # initially loaded psql data;
        # None - means oplog records should be tested starting from beginning
        if ts_start_dict is None:
            ts_start_dict = {}
            for oplog_name in self.oplog_readers:
                ts_start_dict[oplog_name] = None
        sync_ts_dict = self.fast_sync_oplog(ts_start_dict)
        getLogger(__name__).info("sync ts is: %s" % sync_ts_dict)
        if sync_ts_dict:
            getLogger(__name__).info('Synced at ts:%s' % sync_ts_dict)
            return sync_ts_dict
        else:
            getLogger(__name__).error('Sync failed.')
            return None

class CollectionReader:
    def __init__(self, collection_name, schema_engine, mongo_reader):
        self.collection_name = collection_name
        self.schema_engine = schema_engine
        self.mongo_reader = mongo_reader
        self.etl_mongo_reader = EtlMongoReader(MONGO_REC_PROCESSING_PROCESS_COUNT,
                                               FAST_QUEUE_SIZE,
                                               async_worker_handle_mongo_rec,
                                               #1st worker param
                                               {collection_name: schema_engine}, 
                                               {collection_name: mongo_reader})

    def __del__(self):
        del self.etl_mongo_reader

    def get_mongo_table_objs_by_ids(self, rec_ids):
        res = {}
        # prepare query
        mongo_query = prepare_mongo_request_for_list(self.schema_engine, rec_ids)
        getLogger(__name__).info('Exec mongo query: %s', mongo_query)
        self.etl_mongo_reader.execute_query(self.collection_name, mongo_query)
        # get and process records
        processed_recs = self.etl_mongo_reader.next_processed()
        while processed_recs is not None:
            for mongo_tables_obj in processed_recs:
                rec_id = mongo_tables_obj.rec_id()
                res[str(rec_id)] = mongo_tables_obj
            processed_recs = self.etl_mongo_reader.next_processed()
        return res


class OplogSyncEngine:

    def __init__(self, start_ts_dict, collection_name, schema_engine, 
                 mongo_reader, oplog_readers, psql, psql_schema, psql_cache_table):
        self.cache = None
        self.start_ts_dict = start_ts_dict
        self.collection_name = collection_name
        self.schema_engine = schema_engine
        self.mongo_reader = mongo_reader
        self.oplog_readers = oplog_readers
        self.psql = psql
        self.psql_schema = psql_schema
        self.psql_cache_table = psql_cache_table
        self.collection_reader = CollectionReader(collection_name, 
                                                  schema_engine, mongo_reader)


    def sync_map_for_collection(self, sync_engine, collection_rec_ids):
        """ Sync all the ollection' sitems. Return True / False """
        count_synced = 0
        sync_attempt = 0
        while sync_attempt < DO_OPLOG_READ_ATTEMPTS_COUNT \
                and len(collection_rec_ids) != count_synced :
            sync_attempt += 1
            rec_ids = []
            for rec_id, sync in collection_rec_ids.iteritems():
                if sync:
                    continue
                rec_ids.append(rec_id)
                if len(rec_ids) < SYNC_REC_COUNT_IN_PARALLEL:
                    # collect rec_ids for processing
                    continue
                synced_recs = \
                    sync_engine.sync_recs_save_tsdata_to_psql_cache(rec_ids)
                del rec_ids[:]
                for synced_rec_id in synced_recs:
                    collection_rec_ids_dict[synced_rec_id] = True # synced
                getLogger(__name__).info("sync map progress for %s is: %d / %d" %
                                         (self.collection_name,
                                          count_synced,
                                          len(collection_rec_ids)))
                count_synced += len(synced_recs)
            # after loop ends check if there items left to be handled
            if rec_ids:
                synced_recs = \
                    sync_engine.sync_recs_save_tsdata_to_psql_cache(rec_ids)
                for synced_rec_id in synced_recs:
                    collection_rec_ids_dict[synced_rec_id] = True # synced
            count_synced += len(synced_recs)
        return len(collection_rec_ids) == count_synced

    def sync_recs_save_tsdata_to_psql_cache(self, rec_ids):
        """ Return list of recs wor which sync_start is located"""
        synced_recs = []
        recid_tsdata_dict = self.locate_recs_sync_points(rec_ids)
        for rec_id, ts_data_list in recid_tsdata_dict.iteritems():
            if type(ts_data_list) is bool:
                # no need to sync
                psql_data = PsqlCacheTable.PsqlCacheData(None, None, collection,
                                                         None, rec_id, True)
                self.psql_cache_table.insert(psql_data)
                synced_recs.append(rec_id)
                continue
            for ts_data in ts_data_list:
                psql_data = PsqlCacheTable.PsqlCacheData(
                    ts_data.ts, ts_data.oplog_name, self.collection_name,
                    ts_data.queries, rec_id, ts_data.sync_start)
                self.psql_cache_table.insert(psql_data)
                if ts_data.sync_start:
                    synced_recs.append(rec_id)
        return synced_recs

    def locate_recs_sync_points(self, rec_ids):
        res = {}
        mongo_objects = \
            self.collection_reader.get_mongo_table_objs_by_ids(rec_ids)
	recid_ts_queries = self.get_tsdata_for_recs(rec_ids)
        for rec_id, ts_data_list in recid_ts_queries.iteritems():
            if cmp_psql_mongo_tables(rec_id, 
                                     mongo_objects[rec_id], psql_tables_obj):
                # rec_id not require to sync, set flag it's already synced
                # overwrite list of ts by assigning True 
                recid_ts_queries[rec_id] = True
                continue
            for query_idx in xrange(len(ts_data_list)):
                cur_idx = query_idx
                while cur_idx < len(ts_data_list):
                    ts_data = ts_data_list[cur_idx]
                    for single_query in ts_data.queries:
                        exec_insert(self.psql, single_query)
                    cur_idx += 1
                psql_tables_obj = load_single_rec_into_tables_obj(
                    self.psql, self.schema_engine, self.psql_schema, rec_id)
                equal = cmp_psql_mongo_tables(rec_id, 
                                              mongo_objects[rec_id],
                                              psql_tables_obj)
                if equal:
                    if cur_idx:
                        # mark ts to apply it first when do sync
                        res[rec_id][cur_idx - 1].sync_start = True
                    else:
                        # not need sync, overwrite list of ts by assigning True 
                        res[rec_id] = True
                self.psql.conn.rollback()
                if equal:
                    break
        return res

    def get_tsdata_for_recs(self, rec_ids):
        res = {}
        mock_transports_reset(self.oplog_readers)
        for oplog_name in self.oplog_readers:
            js_oplog_query = self.prepare_oplog_request(
                self.start_ts_dict[oplog_name], 
                oplog_name, 
                self.collection_name, 
                rec_ids)
            self.oplog_readers[name].make_new_request(js_oplog_query)
        # create oplog parser. note: cb_insert doesn't need psql object
        parser = OplogParser(self.oplog_readers, self.schemas_path,
                             Callback(cb_insert, ext_arg=self.psql_schema),
                             Callback(cb_update, 
                                      ext_arg=(self.psql, self.psql_schema)),
                             Callback(cb_delete, 
                                      ext_arg=(self.psql, self.psql_schema)),
                             dry_run = False)
        oplog_queries = parser.next()
        while oplog_queries != None:
            collection_name = parser.item_info.schema_name
            oplog_name = parser.item_info.oplog_name
            rec_id = parser.item_info.rec_id
            last_ts = parser.item_info.ts
            if rec_id not in res:
                res[rec_id] = []
            res[rec_id].append( TsData(oplog_name, last_ts, oplog_queries) )
            #### mock transports support /BEGIN/: filter out results
            if collection_name != self.collection_name or rec_id not in rec_ids:
                oplog_queries = parser.next()
                continue
            #### mock transports support /END/
            oplog_queries = parser.next()
        return res


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
        cmp_res = True
        # iterate mongo items belong to one collection
        for collection, recs in self.recs_to_compare.iteritems():
            # comparison strategy: filter out previously compared recs;
            # so will be compared only that items which never compared or
            # prev comparison gave False
            recs_list_cmp = []
            max_recs_in_list = 1000
            for rec_id, compare_res in recs.iteritems():
                if not compare_res.flag:
                    recs_list_cmp.append(compare_res.rec_id)
            # if nothing to compare just skip current collection
            if not recs_list_cmp:
                continue

            maxs = 1000
            lst = recs_list_cmp
            splitted = [lst[i:i + maxs] for i in xrange(0, len(lst), maxs)]
            for chunk in splitted:
                res = self.compare_src_dest_portion(collection, chunk)
                if not res:
                    cmp_res = res
        return cmp_res

    def compare_src_dest_portion(self, collection, recs):
        getLogger(__name__).info('Oplog ops applied. Compare following recs: %s'\
                                     % self.recs_to_compare )
        cmp_res = True
        # prepare query
        mongo_query = prepare_mongo_request_for_list(
            self.schema_engines[collection], recs)
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
        for rec_id in recs:
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
    

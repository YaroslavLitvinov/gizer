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
import ast
import pickle
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
from gizer.etlstatus_table import timestamp_str_to_object as ts_obj
from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.opmultiprocessing import FastQueueProcessor
from gizer.etl_mongo_reader import EtlMongoReader
from mongo_reader.prepare_mongo_request import prepare_mongo_request
from mongo_reader.prepare_mongo_request import prepare_oplog_request
from mongo_reader.prepare_mongo_request import prepare_oplog_request_filter
from mongo_reader.prepare_mongo_request import prepare_mongo_request_for_list
from mongo_schema.schema_engine import create_tables_load_bson_data
from mongo_schema.schema_engine import log_table_errors

MAX_REQCOUNT_FOR_SHARD = 10000
DO_OPLOG_READ_ATTEMPTS_COUNT = 10

# collection reader is just yet another reader using by syncronizer
COLLECTION_READER_BSON_WORKERS_COUNT = 8
COLLECTION_READER_QUEUE_SIZE = COLLECTION_READER_BSON_WORKERS_COUNT*2

# comparator is using for data integrity verification
MONGO_PSQL_CMP_BSON_WORKERS_COUNT = 8
MONGO_PSQL_CMP_QUEUE_SIZE = MONGO_PSQL_CMP_BSON_WORKERS_COUNT*2

SYNC_REC_COUNT_IN_ONE_BATCH = 100

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

class TsData:
    def __init__(self, oplog_name, ts, queries):
        self.oplog_name = oplog_name
        self.ts = ts
        self.queries = queries
        self.sync_start = None

class PsqlCacheTable:
    PsqlCacheData = namedtuple('PsqlCacheData', ['ts', 'oplog', 'collection',
                                                 'queries',  'rec_id',
                                                 'sync_start'])
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
        "rec_id" TEXT, "sync_start" BOOLEAN);'
        self.cursor.execute( fmt.format(schema=self.schema_name) )

    def insert(self, psql_cache_data):
        fmt = 'INSERT INTO {schema}qmetlcache VALUES(\
%s, %s, %s, %s, %s, %s);'
        operation_str = fmt.format(schema=self.schema_name)
        if psql_cache_data.ts:
            ts_str = str(psql_cache_data.ts)
        else:
            ts_str = None
        self.cursor.execute( operation_str,
                             (ts_str,
                              psql_cache_data.oplog,
                              psql_cache_data.collection,
                              # use pickle for non trivial data
                              pickle.dumps(psql_cache_data.queries),
                              str(psql_cache_data.rec_id),
                              psql_cache_data.sync_start) )

    def commit(self):
        self.cursor.execute('COMMIT')
        getLogger(__name__).info("qmetlcache COMMIT")

    def select_max_synced_ts_at_shard(self, oplog_name):
        max_sync_start_fmt= \
            "SELECT max(a.ts) FROM (SELECT ts from {schema}qmetlcache \
WHERE oplog='{oplog}' and sync_start=TRUE) as a;"
        select_query = max_sync_start_fmt.format(schema=self.schema_name,
                                                 oplog=oplog_name)
        self.cursor.execute(select_query)
        row = self.cursor.fetchone()
        if row:
            return ts_obj(row[0])
        else:
            return None

    def select_ts_related_to_rec_id(self, collection, rec_id):
        res = []
        rec_tss_fmt="SELECT * from {schema}qmetlcache WHERE \
collection='{collection}' and rec_id='{rec_id}' ORDER BY ts;"
        select_query = rec_tss_fmt.format(schema=self.schema_name,
                                          collection=collection,
                                          rec_id=str(rec_id))
        self.cursor.execute(select_query)
        rows = self.cursor.fetchall()
        for row in rows:
            res.append(self.convert_row_to_psql_cache_data(row))
        return res

    def convert_row_to_psql_cache_data(self, row):
        if row:
            return PsqlCacheTable.PsqlCacheData(
                ts=ts_obj(row[0]),
                oplog=row[1],
                collection=row[2],
                # use pickle for non trivial data
                queries=pickle.loads(row[3]),
                rec_id=row[4],
                sync_start=row[5])
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
        self.queries_counter = 0
        self.oplog_rec_counter = 0
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

    def _sync_oplog(self, start_ts_dict):
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
                                          self.schemas_path,
                                          self.schema_engines[collection],
                                          self.mongo_readers[collection],
                                          self.oplog_readers,
                                          self.psql,
                                          self.psql_schema,
                                          psql_cache_table)
            res = sync_engine.sync_collection_timestamps(sync_engine, rec_ids_dict)
            if not res:
                return None
        # get max sync_start timestamp for every shard to align sync_point
        max_sync_ts = {}
        for oplog_name in self.oplog_readers:
            sync_ts = psql_cache_table.select_max_synced_ts_at_shard(oplog_name)
            if sync_ts:
                max_sync_ts[oplog_name] = sync_ts
            else:
                max_sync_ts[oplog_name] = start_ts_dict
        # iterate over all collections/rec_ids and execute rec related queries
        # up to speific alignment timestamp 
        getLogger(__name__).info("Sync query exec")
        for collection, rec_ids_dict in collection_rec_ids_dict.iteritems():
            query_exec_progress = 0
            for rec_id in rec_ids_dict:
                query_exec_progress += 1
                if not query_exec_progress % 100:
                    getLogger(__name__).info( \
                        "Sync query exec progress for %s: %d / %d" %
                        (collection,
                         query_exec_progress,
                         len(rec_ids_dict)))
                ts_cache = psql_cache_table.select_ts_related_to_rec_id(
                    collection, rec_id)
                flag = False
                for ts_data in ts_cache:
                    # Start execute queries from sync_start
                    if ts_data.sync_start:
                        flag = True
                    if not flag:
                        continue
                    # if timestamp is exist then need to syncronise
                    if ts_data.ts:
                        if ts_data.ts <= max_sync_ts[ts_data.oplog]:
                            getLogger(__name__).debug("Sync ts op %s" %
                                                      ts_data.ts)
                            self.oplog_rec_counter += 1
                            for oplog_query in ts_data.queries:
                                self.queries_counter += 1
                                exec_insert(self.psql, oplog_query)
        getLogger(__name__).info('COMMIT')
        self.psql.conn.commit()
        getLogger(__name__).info("Synchronization finished ts=%s" % max_sync_ts)
        return max_sync_ts

    def do_oplog_apply(self, start_ts_dict):
        """ Read oplog operations starting just after timestamp start_ts_dict
        by gathering timestamps from all configured shards.
        Apply oplog operations to psql db. After all records are applied do
        consistency check by comparing source (mongo) and dest(psql) records.
        Return False and do rollback if timestamps are applied but consistency 
        checks are failed.
        Return named tuple - OplogApplyRes. Where: OplogApplyRes.ts is dict 
        with a new sync_points (last applied timestamps from every shard).
        OplogApplyRes.res True/False consistency check result after ts applied.
        The function is using OplogParser itself.
        params:
        start_ts_dict -- dict with Timestamp for every shard. """

        new_ts_dict = start_ts_dict
        do_again_counter = 0
        do_again = True
        while do_again:
            do_again = False
            new_ts_dict = self.read_oplog_apply_ops(new_ts_dict, do_again_counter)
            compare_res = self.comparator.compare_src_dest()
            failed_attempts = self.comparator.get_failed_cmp_attempts()
            last_portion_failed = False
            if len(failed_attempts) == 1 and do_again_counter in failed_attempts:
                last_portion_failed = True
            if not compare_res:
                if last_portion_failed:
                    if do_again_counter < DO_OPLOG_READ_ATTEMPTS_COUNT:
                        do_again = True
                        do_again_counter += 1
                    else: # Attempts count exceeded
                        getLogger(__name__).warning('Attempts count exceeded.\
Force assigning compare_res to True.')
                        compare_res = True
                else:
                    getLogger(__name__).warning("Recs cmp failed. %s" 
                                                % failed_attempts)
        if compare_res:
            getLogger(__name__).info('COMMIT')
            self.psql.conn.commit()
            return new_ts_dict
        else:
            getLogger(__name__).error('ROLLBACK')
            self.psql.conn.rollback()
            return None

    def read_oplog_apply_ops(self, start_ts_dict, attempt):
        """ Apply ops going after specified timestamps.
        params:
        start_ts_dict -- dict with Timestamp for every shard.
        Return updated sync points and dict containing affected record ids """
        # derive new sync point from starting points and update it on the go
        for name in self.oplog_readers:
            # get new timestamps greater than sync point
            js_oplog_query = prepare_oplog_request(start_ts_dict[name])
            self.oplog_readers[name].make_new_request(js_oplog_query)
            if self.oplog_readers[name].real_transport():
                self.oplog_readers[name].cursor.limit(MAX_REQCOUNT_FOR_SHARD)
        # create oplog parser. note: cb_insert doesn't need psql object
        parser = OplogParser(self.oplog_readers, self.schemas_path,
                             Callback(cb_insert, ext_arg=self.psql_schema),
                             Callback(cb_update, 
                                      ext_arg=(self.psql, self.psql_schema)),
                             Callback(cb_delete, 
                                      ext_arg=(self.psql, self.psql_schema)),
                             dry_run = False)
        # go over oplog, and apply oplog ops for every timestamp
        oplog_queries = parser.next()
        while oplog_queries != None:
            collection_name = parser.item_info.schema_name
            rec_id = parser.item_info.rec_id
            self.oplog_rec_counter += 1
            for oplog_query in oplog_queries:
                self.queries_counter += 1
                exec_insert(self.psql, oplog_query)
                self.comparator.add_to_compare(collection_name, rec_id, attempt)
            oplog_queries = parser.next()
        getLogger(__name__).info(\
            "Handled oplog records/psql queries: %d/%d" %
            (self.oplog_rec_counter, self.queries_counter))
        res = {}
        for shard in start_ts_dict:
            if shard in parser.last_oplog_ts:
                # ts updated for this shard
                res[shard] = parser.last_oplog_ts[shard]
            else:
                # nothing received from this shard
                res[shard] = start_ts_dict[shard]
        return res

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
        sync_ts_dict = self._sync_oplog(ts_start_dict)
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
        self.etl_mongo_reader = EtlMongoReader(COLLECTION_READER_BSON_WORKERS_COUNT,
                                               COLLECTION_READER_QUEUE_SIZE,
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

    def __init__(self, start_ts_dict, collection_name,
                 schemas_path, schema_engine,
                 mongo_reader, oplog_readers,
                 psql, psql_schema, psql_cache_table):
        self.cache = None
        self.start_ts_dict = start_ts_dict
        self.collection_name = collection_name
        self.schemas_path = schemas_path
        self.schema_engine = schema_engine
        self.mongo_reader = mongo_reader
        self.oplog_readers = oplog_readers
        self.psql = psql
        self.psql_schema = psql_schema
        self.psql_cache_table = psql_cache_table
        self.collection_reader = CollectionReader(collection_name, 
                                                  schema_engine, mongo_reader)


    def sync_collection_timestamps(self, sync_engine, rec_ids_dict):
        """ Sync all the ollection' sitems. Return True / False """
        count_synced = 0
        sync_attempt = 0
        while sync_attempt < DO_OPLOG_READ_ATTEMPTS_COUNT \
                and len(rec_ids_dict) != count_synced :
            sync_attempt += 1
            getLogger(__name__).info("Sync collection:%s attempt # %d/%d" % 
                                     (self.collection_name, 
                                      sync_attempt, DO_OPLOG_READ_ATTEMPTS_COUNT))
            rec_ids = []
            for rec_id, sync in rec_ids_dict.iteritems():
                if sync:
                    continue
                rec_ids.append(rec_id)
                if len(rec_ids) < SYNC_REC_COUNT_IN_ONE_BATCH:
                    # collect rec_ids for processing
                    continue
                synced_recs = \
                    sync_engine.sync_recs_save_tsdata_to_psql_cache(rec_ids)
                del rec_ids[:]
                for synced_rec_id in synced_recs:
                    rec_ids_dict[synced_rec_id] = True # synced
                getLogger(__name__).info("sync map progress for %s is: %d / %d" %
                                         (self.collection_name,
                                          count_synced,
                                          len(rec_ids_dict)))
                count_synced += len(synced_recs)
            # after loop ends check if there items left to be handled
            if rec_ids:
                synced_recs = \
                    sync_engine.sync_recs_save_tsdata_to_psql_cache(rec_ids)
                for synced_rec_id in synced_recs:
                    rec_ids_dict[synced_rec_id] = True # synced
            count_synced += len(synced_recs)
            getLogger(__name__).info("Synced %s %d of %d recs." % 
                                     (self.collection_name,
                                      count_synced, len(rec_ids_dict)))
        return len(rec_ids_dict) == count_synced

    def sync_recs_save_tsdata_to_psql_cache(self, rec_ids):
        """ Return list of recs wor which sync_start is located"""
        synced_recs = []
        recid_tsdata_dict = self.locate_recs_sync_points(rec_ids)
        for rec_id, ts_data_list in recid_tsdata_dict.iteritems():
            if type(ts_data_list) is bool:
                # this rec is already synced, has no timestamps list
                psql_data = PsqlCacheTable.PsqlCacheData(
                    None, None, self.collection_name, None, rec_id, True)
                self.psql_cache_table.insert(psql_data)
                synced_recs.append(rec_id)
                continue
            # cache timestamps data in psql
            for ts_data in ts_data_list:
                psql_data = PsqlCacheTable.PsqlCacheData(
                    ts_data.ts, ts_data.oplog_name, self.collection_name,
                    ts_data.queries, rec_id, ts_data.sync_start)
                self.psql_cache_table.insert(psql_data)
                if ts_data.sync_start:
                    synced_recs.append(rec_id)
        self.psql_cache_table.commit()
        return synced_recs

    def locate_recs_sync_points(self, rec_ids):
        """ Return timestamps data and recs sync points in following format:
        res = { rec_id: [ TsData list ] }"""
        res = {}
        mongo_objects = \
            self.collection_reader.get_mongo_table_objs_by_ids(rec_ids)
	recid_ts_queries = self.get_tsdata_for_recs(rec_ids)
        for rec_id, ts_data_list in recid_ts_queries.iteritems():
            if str(rec_id) in mongo_objects:
                mongo_obj = mongo_objects[str(rec_id)]
            else:
                # get empty object as doesn't exists in mongo
                mongo_obj = create_tables_load_bson_data(self.schema_engine, {})
            psql_obj = load_single_rec_into_tables_obj(
                self.psql, self.schema_engine, self.psql_schema, rec_id)
            # cmp mongo and psql records before applying timestamps
            # just to check if it is already synced
            if cmp_psql_mongo_tables(rec_id, mongo_obj, psql_obj):
                # rec_id not require to sync, set flag it's already synced
                # overwrite list of ts by assigning True 
                res[rec_id] = True
                continue
            # Apply timestamps and then cmp resulting psql object and mongo obj
            # If not equal then shift to next timestamp and do it again
            for query_idx in xrange(len(ts_data_list)):
                cur_idx = query_idx
                while cur_idx < len(ts_data_list):
                    ts_data = ts_data_list[cur_idx]
                    for single_query in ts_data.queries:
                        exec_insert(self.psql, single_query)
                    cur_idx += 1
                psql_obj = load_single_rec_into_tables_obj(
                    self.psql, self.schema_engine, self.psql_schema, rec_id)
                equal = cmp_psql_mongo_tables(rec_id, mongo_obj, psql_obj)
                # check if located ts, to be used as start point to apply ts
                if equal:
                    if rec_id not in res:
                        res[rec_id] = ts_data_list
                    res[rec_id][query_idx].sync_start = True
                    getLogger(__name__).info("rec_id sync point %s : [%s]->%s" % 
                                             (rec_id, 
                                              ts_data_list[query_idx].oplog_name,
                                              ts_data_list[query_idx].ts))
                self.psql.conn.rollback()
                if equal:
                    break
        return res

    def get_tsdata_for_recs(self, rec_ids):
        res = {}
        # issue separate requests to oplog readers
        for oplog_name in self.oplog_readers:
            if self.oplog_readers[oplog_name].real_transport():
                dbname = self.mongo_reader.settings_list[0].dbname
                js_query = prepare_oplog_request_filter(
                    self.start_ts_dict[oplog_name],
                    dbname,
                    self.collection_name,
                    rec_ids)
            else:
                js_query = prepare_oplog_request(self.start_ts_dict[oplog_name])
            self.oplog_readers[oplog_name].make_new_request(js_query)
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
            #### mock transports support /BEGIN/: filter out results
            if collection_name != self.collection_name or rec_id not in rec_ids:
                oplog_queries = parser.next()
                continue
            #### mock transports support /END/
            if rec_id not in res:
                res[rec_id] = []
            res[rec_id].append( TsData(oplog_name, last_ts, oplog_queries) )
            oplog_queries = parser.next()
        return res


class ComparatorMongoPsql:

    def __init__(self, schema_engines, mongo_readers, psql, psql_schema):
        self.schema_engines = schema_engines
        self.mongo_readers = mongo_readers
        self.psql = psql
        self.psql_schema = psql_schema
        self.recs_to_compare = {}
        self.etl_mongo_reader = EtlMongoReader(MONGO_PSQL_CMP_BSON_WORKERS_COUNT,
                                               MONGO_PSQL_CMP_QUEUE_SIZE,
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
    

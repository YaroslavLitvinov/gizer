#!/usr/bin/env python

""" Oplog parser, and patcher of end data by oplog operations.
Oplog synchronization with initially loaded data stored in psql.
do_oplog_apply -- handling oplog and applying oplog ops func
sync_oplog -- find syncronization point in oplog for initially loaded data."""

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import traceback
import logging
from logging import getLogger
from gizer.collection_reader import CollectionReader
from gizer.psql_objects import load_single_rec_into_tables_obj
from gizer.psql_objects import cmp_psql_mongo_tables
from gizer.oplog_parser import exec_insert
from gizer.psql_cache import PsqlCacheTable
from gizer.oplog_sync_base import OplogSyncBase
from gizer.oplog_sync_base import DO_OPLOG_REREAD_MAXCOUNT
from gizer.log import logless, logmore
from mongo_reader.prepare_mongo_request import prepare_oplog_request

SYNC_REC_COUNT_IN_ONE_BATCH = 100

class OplogSyncUnallignedData(OplogSyncBase):
    """ This syncronizer inteneded to syncronize unalligned data, which
    produced by init load. In opposite to OplogSyncAllignedData this
    implementation is slower as it using deep syncronization algorithm
    and doing more comparison checks and caching intermediate data in postgres.
    Unalligned data that synced at once can be in further be syncronized by
    OplogSyncAllignedData."""

    def __init__(self, psql_etl, psql, mongo_readers, oplog_readers,
                 schemas_path, schema_engines, psql_schema, attempt):
        """ params:
        psql_etl -- Postgres cursor wrapper
        psql -- Postgres cursor wrapper. Separate cursor.
        mongo_readers -- dict of mongo readers, one per collection
        oplog -- Mongo oplog cursor wrappper
        schemas_path -- Path with js schemas representing mongo collections
        psql_schema -- psql schema whose tables data to patch."""
        super(OplogSyncUnallignedData, self).\
            __init__(psql, mongo_readers, oplog_readers,
                     schemas_path, schema_engines, psql_schema, attempt)
        self.psql_etl = psql_etl

    def get_rec_ids(self, start_ts_dict):
        """Return {collection_name: {rec_id: bool}} """
        res = {}
        getLogger(__name__).\
            info('Start getting rec ids for sync going after ts:%s',
                 start_ts_dict)
        saved_logging_level = logging.getLogger().getEffectiveLevel()
        logging.getLogger().setLevel(logging.ERROR)
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
        parser = self.new_oplog_parser(dry_run=True)
        # go over oplog get just rec ids for timestamps that can be handled
        oplog_queries = parser.next()
        while oplog_queries != None:
            collection_name = parser.item_info.schema_name
            rec_id = parser.item_info.rec_id
            if collection_name not in res:
                res[collection_name] = {}
            res[collection_name][rec_id] = None
            oplog_queries = parser.next()
        logging.getLogger().setLevel(saved_logging_level)
        getLogger(__name__).info('Finish getting rec ids')
        return res

    def _sync_oplog(self, start_ts_dict):
        """ Get syncronization point for every record of oplog and psql data
        (which usually is initially loaded data).
        Return dict with timestamps if synchronization successfull,
        or None if not.
        params:
        start_ts_dict -- Dict with initial timestamps"""
        # create table to save map of syncronization for all records
        psql_cache_table = PsqlCacheTable(self.psql_etl.cursor,
                                          self.psql_schema)
        collection_rec_ids_dict = self.get_rec_ids(start_ts_dict)
        for collection, rec_ids_dict in collection_rec_ids_dict.iteritems():
            getLogger(__name__).info("Collection %s count to sync: %s",
                                     collection, len(rec_ids_dict))
        # get sync map. Every collection items have differetn sync point
        # and should be aligned
        OplogSyncEngine.cursor_ts_dict = start_ts_dict
        for collection, rec_ids_dict in collection_rec_ids_dict.iteritems():
            sync_engine = OplogSyncEngine(
                start_ts_dict, collection, self, psql_cache_table)
            try:
                res = sync_engine.sync_collection_timestamps(sync_engine,
                                                             rec_ids_dict)
            except:
                getLogger(__name__).error('Exception caught %s',
                                          traceback.format_exc())
                res = None
            if not res:
                return None
        # get max sync_start timestamp for every shard to align sync_point
        max_sync_ts = {}
        for oplog_name in self.oplog_readers:
            sync_ts = psql_cache_table.select_max_synced_ts_at_shard(oplog_name)
            if sync_ts:
                max_sync_ts[oplog_name] = sync_ts
            else:
                max_sync_ts[oplog_name] = start_ts_dict[oplog_name]
        # iterate over all collections/rec_ids and execute rec related queries
        # up to speific alignment timestamp
        getLogger(__name__).info("Sync query exec")
        for collection, rec_ids_dict in collection_rec_ids_dict.iteritems():
            query_exec_progress = 0
            for rec_id in rec_ids_dict:
                query_exec_progress += 1
                if not query_exec_progress % 100:
                    getLogger(__name__).info(
                        "Sync query exec progress for %s: %d / %d",
                        collection, query_exec_progress, len(rec_ids_dict))
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
                            getLogger(__name__).debug("Synced ts %s : %s",
                                                      rec_id, ts_data.ts)
                            self.oplog_rec_counter += 1
                            for oplog_query in ts_data.queries:
                                self.queries_counter += 1
                                logless()
                                exec_insert(self.psql, oplog_query)
                                logmore()
        getLogger(__name__).info('COMMIT')
        self.psql.conn.commit()
        getLogger(__name__).info("Synchronization finished ts=%s", max_sync_ts)
        return max_sync_ts

    def sync(self, ts_start_dict):
        """ Sync oplog and postgres. The result of synchronization is a single
        timestamp from oplog. So if do apply to psql all timestamps going after
        that sync point and then compare affected psql and mongo records
        they should be equal.  If TS is not located then synchronization failed.
        params:
        ts_start_dict -- dict contains one timestamp for every oplog shard
        which is start pos to find sync point"""
        getLogger(__name__).info('Start oplog sync. Default ts:%s',
                                 ts_start_dict)
        # ts_start is timestamp starting from which oplog records
        # should be applied to psql tables to locate ts which corresponds to
        # initially loaded psql data;
        # None - means oplog records should be tested starting from beginning
        if ts_start_dict is None:
            ts_start_dict = {}
            for oplog_name in self.oplog_readers:
                ts_start_dict[oplog_name] = None
        sync_ts_dict = self._sync_oplog(ts_start_dict)
        getLogger(__name__).info("sync ts is: %s", sync_ts_dict)
        if sync_ts_dict:
            getLogger(__name__).info('Synced at ts:%s', sync_ts_dict)
            return sync_ts_dict
        else:
            getLogger(__name__).error('Sync failed.')
            return None

class OplogSyncEngine(object):
    cursor_ts_dict = None

    """ Synchronize items across single collection.
    Using postgres table for caching purposes. """
    def __init__(self, start_ts_dict, collection_name,
                 sync_base, psql_cache_table):
        self.cache = None
        self.start_ts_dict = start_ts_dict
        if not OplogSyncEngine.cursor_ts_dict:
            OplogSyncEngine.cursor_ts_dict = start_ts_dict
        self.collection_name = collection_name
        self.sync_base = sync_base
        self.schema_engine = sync_base.schema_engines[collection_name]
        self.mongo_reader = sync_base.mongo_readers[collection_name]
        self.mongo_reader.reset_dataset() # for mock compatibility
        self.psql_cache_table = psql_cache_table
        self.collection_reader = CollectionReader(self.collection_name,
                                                  self.schema_engine,
                                                  self.mongo_reader)
        self.cantsync = {}

    def __del__(self):
        del self.collection_reader

    def sync_collection_timestamps(self, sync_engine, rec_ids_dict):
        """ Sync all the collection's items. Return True / False """
        count_synced = 0
        sync_try = 0
        self.cantsync = {}
        while sync_try < DO_OPLOG_REREAD_MAXCOUNT \
                and len(rec_ids_dict) != count_synced:
            sync_try += 1
            getLogger(__name__).info("%s collection sync_try # %d/%d",
                                     self.collection_name, sync_try,
                                     DO_OPLOG_REREAD_MAXCOUNT)
            ids = []
            for rec_id, sync in rec_ids_dict.iteritems():
                if not sync:
                    ids.append(rec_id)
            maxs = SYNC_REC_COUNT_IN_ONE_BATCH
            splitted = [ids[i:i + maxs] for i in xrange(0, len(ids), maxs)]
            for rec_ids in splitted:
                synced_recs = sync_engine.sync_and_cache_tsdata(rec_ids)
                count_synced += len(synced_recs)
                for synced_rec_id in synced_recs:
                    rec_ids_dict[synced_rec_id] = True # synced
                getLogger(__name__).info("sync progress for %s is: %d / %d",
                                         self.collection_name,
                                         count_synced,
                                         len(rec_ids_dict))
        return len(rec_ids_dict) == count_synced

    def sync_and_cache_tsdata(self, rec_ids):
        """ Return list of recs wor which sync_start is located"""
        synced_recs = []
        failed_to_sync_rec_ids = []
        # check if sync failed, and log all related data
        for rec_id in rec_ids:
            # if same timestamps count during more than one sync_try
            if str(rec_id) in self.cantsync and \
                    len(self.cantsync[str(rec_id)]) > 1 \
                    and len(self.cantsync[str(rec_id)]) != \
                    len(set(self.cantsync[str(rec_id)])):
                failed_to_sync_rec_ids.append(rec_id)
        # if sync failed
        if failed_to_sync_rec_ids:
            self.log_sync_error(failed_to_sync_rec_ids)
            return []

        recid_tsdata_dict = self.locate_recs_sync_points(rec_ids)
        for rec_id, ts_data_list in recid_tsdata_dict.iteritems():
            if type(ts_data_list) is bool:
                synced_recs.append(rec_id)
                continue
            for ts_data in ts_data_list:
                if ts_data.sync_start:
                    synced_recs.append(rec_id)

        return synced_recs

    def locate_recs_sync_points(self, rec_ids):
        """ Return timestamps data and recs sync points in following format:
        res = { rec_id: [ TsData list ] }"""
        res = {}
        getLogger(__name__).info("Start fetching object")
        logless(logging.ERROR)
        mongo_objects = \
            self.collection_reader.get_mongo_table_objs_by_ids(rec_ids)
        logmore()
        getLogger(__name__).info("Getting ts for rec_ids: [%s]%s",
                                 self.collection_name, rec_ids)
        logless()
        recid_ts_queries = self.get_tsdata_for_recs(rec_ids)
        logmore()
        for rec_id, ts_data_list in recid_ts_queries.iteritems():
            getLogger(__name__).info("Count of timestamps for rec_id: %s : %d",
                                     rec_id, len(ts_data_list))
            logless()
            mongo_obj = None
            if str(rec_id) in mongo_objects:
                mongo_obj = mongo_objects[str(rec_id)]
            psql_obj = load_single_rec_into_tables_obj(
                self.sync_base.psql, self.schema_engine,
                self.sync_base.psql_schema, rec_id)
            # cmp mongo and psql records before applying timestamps
            # just to check if it is already synced
            logmore()
            equal = cmp_psql_mongo_tables(rec_id, mongo_obj, psql_obj)
            if equal:
                # rec_id not require to sync, set flag it's already synced
                # overwrite list of ts by assigning True
                res[rec_id] = True
                psql_data = PsqlCacheTable.PsqlCacheData(
                    None, None, self.collection_name, None, rec_id, True)
                self.psql_cache_table.insert(psql_data)
                getLogger(__name__).info("%s already synced", rec_id)
                continue
            # Apply timestamps and then cmp resulting psql object and mongo obj
            # If not equal then shift to next timestamp and do it again
            equal = True
            for query_idx in xrange(len(ts_data_list)):
                cur_idx = query_idx
                logless()
                while cur_idx < len(ts_data_list):
                    ts_data = ts_data_list[cur_idx]
                    logmore()
                    getLogger(__name__).info("Exec queries [%s]ts: %s",
                                             ts_data.oplog, ts_data.ts)
                    logless()
                    for single_query in ts_data.queries:
                        exec_insert(self.sync_base.psql, single_query)
                    cur_idx += 1
                psql_obj = load_single_rec_into_tables_obj(
                    self.sync_base.psql, self.schema_engine,
                    self.sync_base.psql_schema, rec_id)
                logmore()
                equal = cmp_psql_mongo_tables(rec_id, mongo_obj, psql_obj)
                # make start point from located ts
                if equal:
                    if rec_id not in res:
                        res[rec_id] = ts_data_list
                    ts_data_cache = res[rec_id][query_idx]
                    ts_data = PsqlCacheTable.PsqlCacheData(
                        ts_data_cache.ts, ts_data.oplog, ts_data.collection,
                        ts_data.queries, rec_id, True)
                    res[rec_id][query_idx] = ts_data
                    self.psql_cache_table.update_ts_sync(ts_data.oplog,
                                                         ts_data.ts)
                    getLogger(__name__).info("rec_id sync point %s : [%s]->%s",
                                             rec_id,
                                             ts_data_list[query_idx].oplog,
                                             ts_data_list[query_idx].ts)
                self.sync_base.psql.conn.rollback()
                if equal:
                    break

            if not equal:
                # not synced, yet
                if str(rec_id) not in self.cantsync:
                    self.cantsync[str(rec_id)] = []
                self.cantsync[str(rec_id)].append(len(ts_data_list))
                getLogger(__name__).warning(\
                    "can't sync now ts_count %s:, rec_id %s",
                    len(ts_data_list), rec_id)
        self.psql_cache_table.commit()
        return res

    def get_tsdata_for_recs(self, rec_ids):
        res = {}
        # issue separate requests to oplog readers
        for oplog_name in self.sync_base.oplog_readers:
            # get all timestamps
            js_query = prepare_oplog_request(
                OplogSyncEngine.cursor_ts_dict[oplog_name])
            self.sync_base.oplog_readers[oplog_name].make_new_request(js_query)
        parser = self.sync_base.new_oplog_parser(dry_run=False)
        oplog_queries = parser.next()
        while oplog_queries != None:
            collection_name = parser.item_info.schema_name
            oplog_name = parser.item_info.oplog_name
            rec_id = parser.item_info.rec_id
            last_ts = parser.item_info.ts
            OplogSyncEngine.cursor_ts_dict[oplog_name] = last_ts
            # cache all timestamps data in psql
            psql_data = PsqlCacheTable.PsqlCacheData(
                last_ts, oplog_name, collection_name,
                oplog_queries, rec_id, None)
            self.psql_cache_table.insert(psql_data)
            oplog_queries = parser.next()

        self.psql_cache_table.commit()
        # fetch all ts data related to specified rec ids
        res = self.fecth_ts_data_from_cache(rec_ids)
        return res

    def fecth_ts_data_from_cache(self, rec_ids_list):
        res = {}
        # fetch all ts data related to specified rec ids
        for rec_id in rec_ids_list:
            if rec_id not in res:
                res[rec_id] = []
            ts_data_list = self.psql_cache_table.select_ts_related_to_rec_id(
                self.collection_name, rec_id)
            for ts_data in ts_data_list:
                res[rec_id].append(ts_data)
        return res

    def log_sync_error(self, rec_ids):
        getLogger(__name__).error('Failed to sync %s rec_ids %s',
                                  self.collection_name, rec_ids)
        logless(logging.ERROR)
        mongo_objects = \
            self.collection_reader.get_mongo_table_objs_by_ids(rec_ids)
        recid_ts_data_dict = self.fecth_ts_data_from_cache(rec_ids)
        logmore()
        for rec_id, ts_data_list in recid_ts_data_dict.iteritems():
            # log mongo record
            mongo_obj = mongo_objects[str(rec_id)]
            for tblname, sqltable in mongo_obj.tables.iteritems():
                getLogger(__name__).warning("mongo rec_id %s [%s] %s",
                                            rec_id, tblname, sqltable)
                getLogger(__name__).warning(sqltable)

            # log psql record
            psql_obj = load_single_rec_into_tables_obj(
                self.sync_base.psql, self.schema_engine,
                self.sync_base.psql_schema, rec_id)
            for tblname, sqltable in psql_obj.tables.iteritems():
                getLogger(__name__).warning("psql rec_id %s [%s] %s",
                                            rec_id, tblname, sqltable)
            # log timestamps
            for ts_data in ts_data_list:
                getLogger(__name__).warning("ts of rec_id: [%s]%s -> [%s]%s",
                                            ts_data.collection, rec_id,
                                            ts_data.oplog, ts_data.ts)
            # log queries data
            getLogger(__name__).warning("--------Queries of rec_id %s:", rec_id)
            for ts_data in ts_data_list:
                for query in ts_data.queries:
                    fmt_string = query.query[0]
                    for sqlparams in query.query[1]:
                        getLogger(__name__).warning("rec_id: %s -> %s %s",
                                                    ts_data.rec_id,
                                                    fmt_string, sqlparams)

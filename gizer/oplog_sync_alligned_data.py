#!/usr/bin/env python

""" Simplified version of synchronizer that is working with alligned data. """

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from logging import getLogger
from collections import namedtuple
from gizer.oplog_parser import exec_insert
from gizer.batch_comparator import ComparatorMongoPsql
from gizer.oplog_sync_base import OplogSyncBase
from gizer.oplog_sync_base import DO_OPLOG_READ_ATTEMPTS_COUNT
from gizer.psql_objects import remove_rec_from_psqldb
from gizer.psql_objects import insert_tables_data_into_dst_psql
from gizer.collection_reader import CollectionReader
from mongo_reader.prepare_mongo_request import prepare_oplog_request

# usefull for testing recovering (it helps simulate bad records)
#MAX_REQCOUNT_FOR_SHARD = 100

class OplogSyncAllignedData(OplogSyncBase):
    """ Simplified version of synchronizer that is working with alligned data.
        As init load produces unalligned data this syncronizer should not be
        used just after init load finishes. Instead OplogSyncUnallignedData
        must be used. """

    def __init__(self, psql, mongo_readers, oplog_readers,
                 schemas_path, schema_engines, psql_schema):
        """ params:
        psql -- Postgres cursor wrapper
        mongo_readers -- dict of mongo readers, one per collection
        oplog -- Mongo oplog cursor wrappper
        schemas_path -- Path with js schemas representing mongo collections
        psql_schema -- psql schema whose tables data to patch."""
        super(OplogSyncAllignedData, self).\
            __init__(psql, mongo_readers, oplog_readers,
                     schemas_path, schema_engines, psql_schema)
        self.comparator = ComparatorMongoPsql(schema_engines,
                                              mongo_readers,
                                              psql,
                                              psql_schema)
        self.failed = False

    def __del__(self):
        del self.comparator

    def sync(self, start_ts_dict):
        """ Read oplog operations starting just after timestamp start_ts_dict
        by gathering timestamps from all configured shards.
        Apply oplog operations to psql db. After all records are applied do
        consistency check by comparing source (mongo) and dest(psql) records.
        Return False and do rollback if timestamps are applied but consistency 
        checks are failed.
        Return sync points as dict.
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
            getLogger(__name__).warning("Failed cmp attempts %s" % failed_attempts)
            last_portion_failed = False
            recover = False
            # if failed only latest data
            if len(failed_attempts) == 1 and do_again_counter in failed_attempts:
                last_portion_failed = True
            elif len(failed_attempts):
                # recover records whose cmp get negative result
                self.recover_failed_items(failed_attempts)
                recover = True
                compare_res = True
            if not compare_res or not new_ts_dict:
                # if transport returned an error then keep the same ts_start
                # and return True, as nothing applied
                if self.failed or self.comparator.is_failed():
                    return start_ts_dict
                if last_portion_failed:
                    if do_again_counter < DO_OPLOG_READ_ATTEMPTS_COUNT:
                        do_again = True
                        do_again_counter += 1
                    else: # Attempts count exceeded
                        getLogger(__name__).warning('Attempts count exceeded.\
Force assigning compare_res to True.')
                        compare_res = True
                else:
                    getLogger(__name__).warning("Recs cmp failed.")
        if compare_res:
            getLogger(__name__).info('COMMIT')
            self.psql.conn.commit()
            if recover:
                return 'resync' # must be handled specially
            else:
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
            # TODO: comment it
            #if self.oplog_readers[name].real_transport():
            #    self.oplog_readers[name].cursor.limit(MAX_REQCOUNT_FOR_SHARD)
        parser = self.new_oplog_parser(dry_run=False)
        # go over oplog, and apply oplog ops for every timestamp
        oplog_queries = parser.next()
        while oplog_queries != None:
            collection_name = parser.item_info.schema_name
            rec_id = parser.item_info.rec_id
            self.oplog_rec_counter += 1
            if len(oplog_queries):
                getLogger(__name__).info("Exec ts queries [%s] %s:",
                                         parser.item_info.oplog_name,
                                         parser.item_info.ts)
            for oplog_query in oplog_queries:
                self.queries_counter += 1
                exec_insert(self.psql, oplog_query)
                self.comparator.add_to_compare(collection_name, rec_id, attempt)
            oplog_queries = parser.next()
        getLogger(__name__).info(\
            "%d attempt. Handled oplog records/psql queries: %d/%d" %
            (attempt, self.oplog_rec_counter, self.queries_counter))
        res = {}
        for shard in start_ts_dict:
            if shard in parser.last_oplog_ts:
                # ts updated for this shard
                res[shard] = parser.last_oplog_ts[shard]
            else:
                # nothing received from this shard
                res[shard] = start_ts_dict[shard]
        self.failed = parser.is_failed()
        if parser.is_failed():
            res = None
        return res

    def recover_failed_items(self, failed_items):
        getLogger(__name__).warning('start recovery')
        for _, collection_ids in failed_items.iteritems():
            for collection, ids in collection_ids.iteritems():
                self.recover_collection_items(collection, ids)

        # print list of recovered items
        for _, collection_ids in failed_items.iteritems():
            for collection, ids in collection_ids.iteritems():
                for rec_id in ids:
                    getLogger(__name__).info("recovered %s %s",
                                             collection, rec_id)
        getLogger(__name__).info("recover complete")

    def recover_collection_items(self, collection, ids):
        reader = CollectionReader(collection, 
                                  self.schema_engines[collection],
                                  self.mongo_readers[collection])
        maxs = 100
        splitted = [ids[i:i + maxs] for i in xrange(0, len(ids), maxs)]
        for chunk_ids in splitted:
            recs = reader.get_mongo_table_objs_by_ids(chunk_ids)
            for str_rec_id, rec in recs.iteritems():
                # 1. remove from psql
                rec_id_obj = [i for i in ids if str(i) == str(str_rec_id) ][0]
                remove_rec_from_psqldb(self.psql, self.psql_schema,
                                       reader.schema_engine, 
                                       collection, rec, rec_id_obj)
                # 2. add new one to psql
                insert_tables_data_into_dst_psql(self.psql, rec,
                                                 self.psql_schema, '')

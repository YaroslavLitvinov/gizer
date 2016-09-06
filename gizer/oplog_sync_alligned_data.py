#!/usr/bin/env python

""" Simplified version of synchronizer that is working with alligned data. """

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from logging import getLogger
from collections import namedtuple
from gizer.oplog_parser import OplogParser
from gizer.oplog_parser import exec_insert
from gizer.oplog_parser import Callback
from gizer.oplog_handlers import cb_insert
from gizer.oplog_handlers import cb_update
from gizer.oplog_handlers import cb_delete
from gizer.batch_comparator import ComparatorMongoPsql
from gizer.oplog_sync_base import OplogSyncBase
from gizer.oplog_sync_base import DO_OPLOG_READ_ATTEMPTS_COUNT
from mongo_reader.prepare_mongo_request import prepare_oplog_request

MAX_REQCOUNT_FOR_SHARD = 10000

class OplogSyncAllignedData(OplogSyncBase):
    """ Simplified version of synchronizer that is working with alligned data.
        
        As init load produces unalligned data this syncronizer should not be used
        just after init load finishes. Instead OplogSyncUnallignedData must be used.
        
    """

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
            if len(failed_attempts) == 1 and do_again_counter in failed_attempts:
                last_portion_failed = True
            if not compare_res or not new_ts_dict:
                # if transport returned an error then keep the same ts_start
                # and return True, as nothing applied
                readers_failed = [(k, v.failed) for k,v in \
                                    self.comparator.mongo_readers.iteritems() \
                                    if v.failed]
                if not new_ts_dict or len(readers_failed):
                    if len(readers_failed):
                        getLogger(__name__).warning("mongo readers failed: %s" 
                                                    % (str(readers_failed)))
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
        if parser.is_failed():
            res = None
        return res


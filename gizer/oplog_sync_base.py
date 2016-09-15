#!/usr/bin/env python

""" Interface for synchronizer implementations """

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from gizer.oplog_handlers import cb_insert
from gizer.oplog_handlers import cb_update
from gizer.oplog_handlers import cb_delete
from gizer.oplog_parser import OplogParser
from gizer.oplog_parser import Callback

DO_OPLOG_REREAD_MAXCOUNT = 10

class OplogSyncBase(object):
    def __init__(self, psql, mongo_readers, oplog_readers,
                 schemas_path, schema_engines, psql_schema):
        self.psql = psql
        self.mongo_readers = mongo_readers
        self.oplog_readers = oplog_readers
        self.schemas_path = schemas_path
        self.schema_engines = schema_engines
        self.psql_schema = psql_schema
        self.queries_counter = 0
        self.oplog_rec_counter = 0
        super(OplogSyncBase, self).__init__()

    def statistic(self):
        """ Return tuple (timestamps count, queries count) """
        return (self.oplog_rec_counter, self.queries_counter)

    def new_oplog_parser(self, dry_run):
        # create oplog parser. note: cb_insert doesn't need psql object
        return OplogParser(self.oplog_readers, self.schemas_path,
                           Callback(cb_insert, ext_arg=self.psql_schema),
                           Callback(cb_update,
                                    ext_arg=(self.psql, self.psql_schema)),
                           Callback(cb_delete,
                                    ext_arg=(self.psql, self.psql_schema)),
                           dry_run=dry_run)

    def sync(self, start_ts_dict):
        """ Do syncronization
        Return dict with a sync points for every shard or None if sync error.
        params:
        start_ts_dict Initial sync points """
        raise NotImplementedError('You need to define a sync method!')

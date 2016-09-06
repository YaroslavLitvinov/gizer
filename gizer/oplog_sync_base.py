#!/usr/bin/env python

""" """

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

DO_OPLOG_READ_ATTEMPTS_COUNT = 10

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

    def statistic(self):
        """ Return tuple (timestamps count, queries count) """
        return (self.oplog_rec_counter, self.queries_counter)

    def sync(self, start_ts_dict):
        """ Do syncronization 
        Return dict with a sync points for every shard or None if sync error.
        params:
        start_ts_dict Initial sync points """
        raise NotImplementedError('You need to define a sync method!')

#!/usr/bin/env python

""" Implements saving and loading of python object in Postgres db """

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import pickle
import psycopg2
from collections import namedtuple
from logging import getLogger
from gizer.etlstatus_table import timestamp_str_to_object as ts_obj

def convert_row_to_psql_cache_data(row):
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


class PsqlCacheTable(object):
    """ Class for saving and loading timestamps data to/from postgres. """

    PsqlCacheData = namedtuple('PsqlCacheData', ['ts', 'oplog', 'collection',
                                                 'queries', 'rec_id',
                                                 'sync_start'])
    def __init__(self, cursor, schema_name):
        self.cursor = cursor
        if len(schema_name):
            self.schema_name = schema_name + '.'
        else:
            self.schema_name = ''
        self.drop_table()
        self.create_table()
        self.create_index()

    def drop_table(self):
        fmt = 'DROP TABLE IF EXISTS {schema}qmetlcache;'
        self.cursor.execute(fmt.format(schema=self.schema_name))

    def create_table(self):
        fmt = 'CREATE TABLE IF NOT EXISTS {schema}qmetlcache (\
        "ts" TEXT, "oplog" TEXT, "collection" TEXT, "queries" BYTEA, \
        "rec_id" TEXT, "sync_start" BOOLEAN);'
        self.cursor.execute(fmt.format(schema=self.schema_name))

    def create_index(self):
        """ Create postgresql index for table """
        #index 1
        fmt = 'CREATE INDEX "i_{table}" \
ON {schema}"{table}" (collection, rec_id);'
        self.cursor.execute(fmt.format(schema=self.schema_name,
                                       table='qmetlcache'))

    def insert(self, psql_cache_data):
        fmt = 'INSERT INTO {schema}qmetlcache VALUES(\
%s, %s, %s, %s, %s, %s);'
        operation_str = fmt.format(schema=self.schema_name)
        if psql_cache_data.ts:
            ts_str = str(psql_cache_data.ts)
        else:
            ts_str = None
        # use pickle to save python objects
        queries = psycopg2.Binary(pickle.dumps(psql_cache_data.queries))
        self.cursor.execute(operation_str,
                            (ts_str,
                             psql_cache_data.oplog,
                             psql_cache_data.collection,
                             queries,
                             str(psql_cache_data.rec_id),
                             psql_cache_data.sync_start))

    def commit(self):
        self.cursor.execute('COMMIT')
        getLogger(__name__).info("qmetlcache COMMIT")

    def update_ts_sync(self, oplog, ts):
        """ time_end, ts, error """
        fmt1 = "UPDATE {schema}qmetlcache SET sync_start=TRUE \
WHERE oplog='{oplog}' and ts='{ts}';"
        query = fmt1.format(schema=self.schema_name,
                            oplog=oplog, ts=str(ts))
        self.cursor.execute(query)

    def select_max_synced_ts_at_shard(self, oplog_name):
        max_sync_start_fmt = \
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
        """ Run sql query and return list of timestamps related to recid """
        res = []
        rec_tss_fmt = "SELECT * from {schema}qmetlcache WHERE \
collection='{collection}' and rec_id='{rec_id}' ORDER BY ts;"
        select_query = rec_tss_fmt.format(schema=self.schema_name,
                                          collection=collection,
                                          rec_id=str(rec_id))
        self.cursor.execute(select_query)
        rows = self.cursor.fetchall()
        for row in rows:
            res.append(convert_row_to_psql_cache_data(row))
        return res



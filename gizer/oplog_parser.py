#!/usr/bin/env python

""" Oplog parser, and patcher of end data by oplog operations.
Oplog synchronization with initially loaded data stored in psql.
OplogParser -- class for basic oplog parsing"""

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from logging import getLogger
from collections import namedtuple
from gizer.oplog_handlers import OplogQuery
from gizer.all_schema_engines import get_schema_engines_as_dict

EMPTY_TS = 'empty_ts'

Callback = namedtuple('Callback', ['cb', 'ext_arg'])
ItemInfo = namedtuple('ItemInfo', ['schema_name',
                                   'schema_engine',
                                   'oplog_name',
                                   'ts',
                                   'rec_id'])

class OplogParser(object):
    """ parse oplog data, apply oplog operations, execute resulted queries
    and verify patched results """
    def __init__(self, readers, schemas_path,
                 cb_ins, cb_upd, cb_del, dry_run):
        self.readers = readers
        self.schema_engines = get_schema_engines_as_dict(schemas_path)
        self.item_info = None
        self.cb_insert = cb_ins
        self.cb_update = cb_upd
        self.cb_delete = cb_del
        self.dry_run = dry_run
        # init cache by Nones
        self.readers_cache = {}
        self.last_oplog_ts = {}
        self.shard_name_for_last_ts = None
        for name in readers:
            self.readers_cache[name] = None

    def is_failed(self):
        """ Return True if error occured, or False otherwise """
        failed = False
        for name, oplog_reader in self.readers.iteritems():
            if oplog_reader.failed:
                getLogger(__name__).warning("oplog transport %s failed", name)
                failed = True
        return failed

    def next_skip_from_migrate(self, name):
        """ Don't handle item['fromMigrate']=True """
        item = self.readers[name].next()
        while item and \
                ('fromMigrate' in item and item['fromMigrate'] is True):
            item = self.readers[name].next()
        return item

    def next_all_readers(self):
        """ Return record with minimum timestamp from readers set """
        # fill cache if empty
        for name in self.readers:
            if not self.readers_cache[name]:
                self.readers_cache[name] = self.next_skip_from_migrate(name)
        # locate item with min timestamp
        ts_min = ('name', None)
        for name, item in self.readers_cache.iteritems():
            if not ts_min[1] and item:
                ts_min = (name, item['ts'])
            elif item and item['ts'] < ts_min[1]:
                ts_min = (name, item['ts'])
        # return min item, pop it from cache
        if ts_min[1]:
            shard_name = ts_min[0]
            self.shard_name_for_last_ts = shard_name
            getLogger(__name__).debug("%s => ts:%s", shard_name, ts_min[1])
            tmp_ts = self.readers_cache[shard_name]
            # to save last timestamp indepedently for every shard
            self.last_oplog_ts[shard_name] = \
                self.readers_cache[shard_name]["ts"]
            self.readers_cache[shard_name] = None
            return tmp_ts
        else:
            return None

    def next_verified(self):
        """ next oplog records for one of ops=u,i,d """
        item = self.next_all_readers()
        while item:
            if item['op'] == 'i' or item['op'] == 'u' or item['op'] == 'd':
                schema_name = item["ns"].split('.')[1]
                if schema_name in self.schema_engines:
                    return item
            item = self.next_all_readers()
        return None

    def next(self):
        """ Return next OplogQuery or None if no more to iterate """
        res = None
        item = self.next_verified()
        if item:
            oplog_name = self.shard_name_for_last_ts
            ts_field = item["ts"]
            ns_field = item["ns"]
            o_field = item["o"]
            # get rec_id
            rec_id = None
            if item["op"] == "u":
                rec_id = item['o2'].values()[0]
            else:
                if '_id' in item['o']:
                    rec_id = item['o']['_id']
                elif 'id' in item['o']:
                    rec_id = item['o']['id']
                else:
                    assert 0

            db_and_collection = item["ns"].split('.')
            # dbname = db_and_collection[0]
            schema_name = db_and_collection[1]
            schema = self.schema_engines[schema_name]
            # save rec_id
            self.item_info = ItemInfo(schema_name,
                                      schema,
                                      oplog_name,
                                      item['ts'],
                                      rec_id)
            getLogger(__name__).\
                debug("op=%s, ts=%s, collection=%s, rec_id=%s",
                      item["op"], item['ts'], schema_name, str(rec_id))
            if self.dry_run: # dry run will not do actual processing
                res = OplogQuery("i", 'SELECT 1;') # query not for execute
            elif item["op"] == "i":
                # insert is ALWAYS expects array of records
                res = self.cb_insert.cb(self.cb_insert.ext_arg,
                                        ts_field, ns_field, schema,
                                        [o_field])
            elif item["op"] == "u":
                res = self.cb_update.cb(self.cb_update.ext_arg,
                                        schema, item)
            elif item["op"] == "d":
                res = self.cb_delete.cb(self.cb_delete.ext_arg,
                                        ts_field, ns_field, schema,
                                        o_field)
        return res

def exec_insert(psql, oplog_query):
    """ Exec postgres query.
    params:
    psql -- Postgres cursor wrapper
    oplog_query -- query tuple to execute"""
    query = oplog_query.query
    fmt_string = query[0]
    for sqlparams in query[1]:
        getLogger(__name__).info('EXECUTE: %s %s', fmt_string, str(sqlparams))
        psql.cursor.execute(fmt_string, sqlparams)

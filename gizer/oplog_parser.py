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
from logging import getLogger
from bson.json_util import loads
from collections import namedtuple
from gizer.psql_objects import load_single_rec_into_tables_obj
from gizer.psql_objects import insert_rec_from_one_tables_set_to_another
from gizer.oplog_handlers import cb_insert
from gizer.oplog_handlers import cb_update
from gizer.oplog_handlers import cb_delete
from gizer.etlstatus_table import timestamp_str_to_object
from gizer.all_schema_engines import get_schema_engines_as_dict
from mongo_reader.prepare_mongo_request import prepare_mongo_request
from mongo_reader.prepare_mongo_request import prepare_oplog_request
from mongo_schema.schema_engine import create_tables_load_bson_data

EMPTY_TS = 'empty_ts'

ItemInfo = namedtuple('ItemInfo', ['schema_name',
                                   'schema_engine',
                                   'ts',
                                   'rec_id'])

class OplogParser:
    """ parse oplog data, apply oplog operations, execute resulted queries
    and verify patched results """
    def __init__(self, readers, schemas_path,
                 cb_ins, cb_upd, cb_del):
        self.readers = readers
        self.first_handled_ts = None
        self.schema_engines = get_schema_engines_as_dict(schemas_path)
        self.item_info = None
        self.cb_insert = cb_ins
        self.cb_update = cb_upd
        self.cb_delete = cb_del
        # init cache by Nones
        self.readers_cache = {}
        for name in readers:
            self.readers_cache[name] = None

    def is_failed(self):
        failed = False
        for name, oplog_reader in self.readers.iteritems():
            if oplog_reader.failed:
                getLogger(__name__).warning("oplog transport %s failed" % name)
                failed = True
        return failed

    def next_all_readers(self):
        # fill cache if empty
        for name in self.readers:
            if not self.readers_cache[name]:
                self.readers_cache[name] = self.readers[name].next()
        # locate item with min timestamp
        ts_min = ('name', None)
        for name, item in self.readers_cache.iteritems():
            if not ts_min[1] and item:
                ts_min = (name, item['ts'])
            elif item and item['ts'] < ts_min[1]:
                ts_min = (name, item['ts'])
        # return min item, pop it from cache
        if ts_min[1]:
            getLogger(__name__).info("from oplog:%s ts:%s" % 
                                     (ts_min[0], ts_min[1]) )
            tmp_ts = self.readers_cache[ts_min[0]]
            self.readers_cache[ts_min[0]] = None
            return tmp_ts
        else:
            return None

    def next_verified(self):
        """ next oplog records for one of ops=u,i,d """
        item = self.next_all_readers()
        while item:
            if item['op'] == 'i' or item['op'] == 'u' or item['op'] == 'd':
                schema_name = item["ns"].split('.')[1]
                if schema_name not in self.schema_engines:
                    getLogger(__name__).\
                        warning("Unknown collection: " +
                                schema_name + ", skip ts:" + str(item["ts"]))
                else:
                    return item
            item = self.next_all_readers()
        return None

    def next(self):
        item = self.next_verified()
        res = None
        if item:
            if self.first_handled_ts is None:
                self.first_handled_ts = item['ts']
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
                    assert(0)

            db_and_collection = item["ns"].split('.')
            # dbname = db_and_collection[0]
            schema_name = db_and_collection[1]
            schema = self.schema_engines[schema_name]
            # save rec_id
            self.item_info = ItemInfo(schema_name,
                                      schema,
                                      item['ts'],
                                      rec_id)
            getLogger(__name__).\
                info("op=" + item["op"] + ", ts=" + str(item['ts']) +
                    ", name=" + schema_name + ", rec_id=" + str(rec_id))
            if item["op"] == "i":
                # insert is ALWAYS expects array of records
                res = self.cb_insert.cb(self.cb_insert.ext_arg,
                                        ts_field, ns_field, schema,
                                        [o_field])
            elif item["op"] == "u":
                o2_id = item["o2"]
                res = self.cb_update.cb(self.cb_update.ext_arg,
                                        schema, item)
            elif item["op"] == "d":
                res = self.cb_delete.cb(self.cb_delete.ext_arg,
                                        ts_field, ns_field, schema,
                                        o_field)
        return res

def exec_insert(psql, oplog_query):
    # create new connection and cursor
    query = oplog_query.query
    fmt_string = query[0]
    for sqlparams in query[1]:
        getLogger(__name__).debug('EXECUTE: ' + str(fmt_string) + str(sqlparams))
        psql.cursor.execute(fmt_string, sqlparams)
        

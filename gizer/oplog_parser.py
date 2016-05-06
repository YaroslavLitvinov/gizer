#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import json
import bson
from bson.json_util import loads
from collections import namedtuple

from all_schema_engines import get_schema_engines_as_dict

OplogQuery = namedtuple('OplogQuery', ['op', 'query'])
Callback = namedtuple('Callback', ['cb', 'ext_arg'])
ItemInfo = namedtuple('ItemInfo', ['schema_name', 
                                   'schema_engine', 
                                   'ts', 
                                   'rec_id'])


class OplogParser:
    
    def __init__(self, reader, start_after_ts, schemas_path, 
                 cb_before,
                 cb_insert, cb_update, cb_delete):
        self.reader = reader
        self.start_after_ts = start_after_ts
        self.first_handled_ts = None
        self.schema_engines = get_schema_engines_as_dict(schemas_path)
        self.item_info = None
        self.cb_before = cb_before
        self.cb_insert = cb_insert
        self.cb_update = cb_update
        self.cb_delete = cb_delete

    def next_verified(self):
        item = self.reader.next()
        while item:
            if item['op'] == 'i' or item['op'] == 'u' or item['op'] == 'd':
                if item and item['ts'] > self.start_after_ts:
                    return item
            item = self.reader.next()
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
                rec_id = str(item['o2'].values()[0])
            else:
                if '_id' in item['o']:
                    rec_id = item['o']['_id']
                elif 'id' in item['o']:
                    rec_id = item['o']['id']
                else:
                    assert(0)
            if type(rec_id) is bson.objectid.ObjectId:
                rec_id = str(rec_id)

            db_and_collection = item["ns"].split('.')
            dbname = db_and_collection[0]
            schema_name = db_and_collection[1]
            schema = self.schema_engines[schema_name]
            # save rec_id
            self.item_info = ItemInfo(schema_name, 
                                      schema, 
                                      item['ts'], 
                                      rec_id)
            
            if self.cb_before:
                self.cb_before.cb(self.cb_before.ext_arg,
                                  schema,
                                  item)
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

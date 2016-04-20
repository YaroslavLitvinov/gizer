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

class OplogParser:
    
    def __init__(self, schemas_path, cb_insert, cb_update, cb_delete):
        self.schema_engines = get_schema_engines_as_dict(schemas_path)
        self.cb_insert = cb_insert
        self.cb_update = cb_update
        self.cb_delete = cb_delete

    def load_file(self, oplog_file):
        """ Use either load_file or load_data"""
        with open(oplog_file) as opfile:
            self.load_data( opfile.read() )
            opfile.close()

    def load_data(self, oplog_data):
        """ Use either load_file or load_data"""
        self.oplog_data = bson.json_util.loads( oplog_data )
        self.oplog_index = 0

    def next(self):
        if self.oplog_index < len(self.oplog_data):
            item = self.oplog_data[self.oplog_index]
            self.oplog_index += 1
            ts_field = item["ts"]
            ns_field = item["ns"]
            o_field = item["o"]
            db_and_collection = item["ns"].split('.')
            dbname = db_and_collection[0]
            schema_name = db_and_collection[1]
            schema = self.schema_engines[schema_name]
            
            if item["op"] == "i":
                return self.cb_insert(ts_field, ns_field, schema, 
                                      [o_field])
            elif item["op"] == "u":
                o2_id = str(item["o2"]['_id'])
                return self.cb_update(ts_field, ns_field, schema, 
                                      o_field['$set'], o2_id)
            elif item["op"] == "d":
                return self.cb_delete(ts_field, ns_field, schema, 
                                      o_field)
        else:
            return None

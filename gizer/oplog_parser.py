#!/usr/bin/env python

import json
import bson
from bson.json_util import loads

from all_schema_engines import get_schema_engines_as_dict


class OplogParser:
    
    def __init__(self, schemas_path, cb_insert, cb_update, cb_delete):
        self.schema_engines = get_schema_engines_as_dict(schemas_path)
        self.cb_insert = cb_insert
        self.cb_update = cb_update
        self.cb_delete = cb_delete
        self.insert_count = 0
        self.update_count = 0
        self.delete_count = 0

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
            ns_field = item["ns"]
            o_field = item["o"]
            db_and_collection = item["ns"].split('.')
            dbname = db_and_collection[0]
            schema_name = db_and_collection[1]
            schema = self.schema_engines[schema_name]
            
            if item["op"] == "i":
                self.insert_count += 1
                return self.cb_insert(ns_field, schema, [o_field])
            elif item["op"] == "u":
                self.update_count += 1
                return self.cb_update(ns_field, schema, o_field, str(item["o2"]["_id"]))
            elif item["op"] == "d":
                self.delete_count += 1
                return self.cb_delete(ns_field, schema, o_field)
        else:
            return None

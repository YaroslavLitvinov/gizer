#!/usr/bin/env python

import json

import bson
from bson.json_util import loads


class OplogParser:
    
    def __init__(self, schemas_path, cb_insert, cb_update, cb_delete):
        self.schemas_path = schemas_path
        self.schemas = {}
        self.cb_insert = cb_insert
        self.cb_update = cb_update
        self.cb_delete = cb_delete

    def load(self, oplog_file):
        self.oplog_data = bson.json_util.loads( oplog_file.read() )
        self.oplog_index = 0

    def next(self):
        if self.oplog_index < len(self.oplog_data):
            item = self.oplog_data[self.oplog_index]
            self.oplog_index += 1
            ns_field = item["ns"]
            o_field = item["o"]
            l = item["ns"].split('.')
            l.insert(0, self.schemas_path)
            schema_filename = '/'.join(l)+'.js'
            if schema_filename not in self.schemas.keys():
                with open(schema_filename, 'r') as f:
                    self.schemas[schema_filename] = json.load( f )
            schema = self.schemas[schema_filename]
            
            if item["op"] == "i":
                return self.cb_insert(ns_field, schema, o_field)
            elif item["op"] == "u":
                return self.cb_update(ns_field, schema, o_field, str(item["o2"]["_id"]))
            elif item["op"] == "d":
                return self.cb_delete(ns_field, schema, o_field)
        else:
            return None
  

if __name__ == "__main__":
    def test_cb_insert(ns, schema, objdata):
        return "insert"

    def test_cb_update(ns, schema, objdata, parent_id):
        return "update"

    def test_cb_delete(ns, schema, objdata):
        return "delete"

    p = OplogParser("./schemas", test_cb_insert, test_cb_update, test_cb_delete)
    with open("test_data/test_oplog.js", "r") as f:
        p.parse(f)



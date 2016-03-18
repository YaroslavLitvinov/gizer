#!/usr/bin/env python

from oplog_parser import OplogParser
from mongo_to_hive_mapping import schema_engine

def default_cb_update(ns, schema, objdata, parent_id):
    return ["default update", parent_id, objdata]
    
def default_cb_delete(ns, schema, objdata):
    return ["default delete", objdata]


if __name__ == "__main__":
    p = OplogParser("./schemas", opinsert_callback, default_cb_update, default_cb_delete)
    with open("test_data/test_oplog.js", "r") as f:
        p.load(f)
        sqls = p.next()
        while sqls:
            print sqls
            sqls = p.next()

            

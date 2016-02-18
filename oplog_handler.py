#!/usr/bin/env python

from oplog_parser import OplogParser
from opinsert import opinsert_callback

def default_cb_update(ns, schema, objdata, parent_id):
    print "default update", parent_id, objdata
    
def default_cb_delete(ns, schema, objdata):
    print "default delete", objdata


if __name__ == "__main__":
    p = OplogParser("./schemas", opinsert_callback, default_cb_update, default_cb_delete)
    with open("test_data/test_oplog.js", "r") as f:
        p.parse(f)

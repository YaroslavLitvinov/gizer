#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import time
import bson
from mongo_schema.schema_engine import SchemaEngine

def prepare_mongo_request(schema_engine, rec_id):
    node = schema_engine.root_node.get_id_node()
    if type(rec_id) is bson.objectid.ObjectId:
        name = node.parent.name
    else:
        name = node.name
    return {name:rec_id}

def prepare_mongo_request_for_list(schema_engine, recid_list):
    request_list = []
    for rec_id in recid_list:
        request_list.append(prepare_mongo_request(schema_engine, rec_id))
    return {"$or": request_list}

def prepare_oplog_request(ts):
    if not ts:
        return {}
    else:
        return {"ts": {"$gt": ts}}

def list_ofitems_byor(rec_ids):
    ors = []
    for rec_id in rec_ids:
       ors.append( { "o2": {"_id" : rec_id} }, {"o._id" : rec_id} ) 
    return ors

def prepare_oplog_request_filter(ts, dbname, collection, rec_ids):
    ts_query = prepare_oplog_request(ts)
    if collection and rec_ids:
        query = { "$and": [ {"ns": dbname+"."+collection}, 
                            {"$or": list_ofitems_byor(rec_ids) }
                            ]
                 }
        if ts_query:
            query["$and"].append(ts_query)
    else:
        query = ts_query
    return query

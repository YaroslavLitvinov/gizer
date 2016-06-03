#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import time
import bson
from mongo_schema.schema_engine import SchemaEngine

def prepare_mongo_request(collection, schema_engine, rec_id):
    query_fmt = "db.%s.find({'%s': %s})"
    node = schema_engine.root_node.get_id_node()
    if type(rec_id) is bson.objectid.ObjectId:
        id_str = "{ '$oid': '%s' }" % str(rec_id)
        name = node.parent.name
    elif type(rec_id) is str:
        id_str = "'%s'" % rec_id
        name = node.name
    else:
        id_str = str(rec_id)
        name = node.name
    return query_fmt % (collection, name, id_str)


#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import bson
from bson import json_util
from collections import namedtuple
from mongo_schema.schema_engine import Tables
from mongo_schema.schema_engine import create_tables_load_bson_data

PComponent = namedtuple('PComponent', ['name', 'index'])

def get_name_and_path_components(name_and_path):
    """ @return get dict of parsed components with values, value can be None"""
    res = []
    numerics = []
    names = []
    #collection_name = schema_engine.root_node.name
    name_components = name_and_path.split('.')
    for comp in name_components:
        if unicode(comp).isnumeric():
            numerics.append(int(comp))
        else:
            names.append(comp)
    for i in xrange(len(names)):
        name = names[i]
        if i < len(numerics):
            res.append(PComponent(name, numerics[i]))
        else:
            res.append(PComponent(name, None))
    return res

def initial_indexes_from_components(schema_engine, components):
    res = {}
    locate_path = []
    print "locate_path", locate_path
    for comp_i in xrange(len(components)):
        locate_path = [i.name for i in components[:comp_i+1]]
        node = schema_engine.locate(locate_path)
        res[node.long_alias()] = components[comp_i].index
    return res

def node_by_components(schema_engine, components):
    locate_path = [i.name for i in components]
    node = schema_engine.locate(locate_path)
    last_index = components[-1].index
    # if bson_data is considered as array item
    if node.value is node.type_array and last_index:
        node = node.children[0]
    return node

def get_tables_data_from_oplog_set_command(schema_engine, bson_data,
                                           bson_object_id_name_value):
    """ @return ttuple(tables as dict, initial_indexes) """
    all_initial_indexes = {}
    tables = {}
    print "bson_data", bson_data
    for name_and_path, bson_value in bson_data.iteritems():
        components = get_name_and_path_components(name_and_path)
        initial_indexes = initial_indexes_from_components(schema_engine,
                                                          components)
        if initial_indexes:
            all_initial_indexes = dict(all_initial_indexes.items() + 
                                       initial_indexes.items())

        obj_id_name = bson_object_id_name_value.keys()[0]
        obj_id_val = bson_object_id_name_value.values()[0]
        node = node_by_components(schema_engine, components)
        whole_bson = node.json_inject_data(bson_value,
                                           obj_id_name, obj_id_val)
        print "node_by_components", node, node.name
        table_obj = Tables(schema_engine, whole_bson)
        table_obj.load_all()
        # exclude parent tables if they have no data
        parent_tables_to_skip = [i.long_plural_alias()
                                 for i in node.all_parents() 
                                 if i.value == i.type_array][:-1]
        print "len(table_obj.tables)", len(table_obj.tables)
        for table_name, table in table_obj.tables.iteritems():
            # skip parent tables as they have no data
            if not table_name in parent_tables_to_skip:
                tables[table_name] = table
            else:
                print "skip table", table_name
    return (tables, all_initial_indexes)

#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import bson
from bson import json_util
from mongo_schema.schema_engine import create_tables_load_bson_data


def parse_name_and_path(name_and_path):
    """ @return get dict of parsed components with values, value can be None"""
    res = {}
    numerics = []
    names = []
    #collection_name = schema_engine.root_node.name
    name_components = name_and_path.split('.')
    for comp in name_components:
        if unicode(comp).isnumeric():
            numerics.append(comp)
        else:
            names.append(comp)
    for i in xrange(len(names)):
        name = names[i]
        if i < len(numerics):
            res[name] = numerics[i]
        else:
            res[name] = None
    return res


def complete_partial_record(schema_engine, name_and_path, bson_data):
    result = '%s'
    components = parse_name_and_path(name_and_path)
    if len(components.keys()) > 1:
        node = schema_engine.locate(components.keys())
        parents = node.all_parents()[2:] #skip collections array and struct
        for parent in parents:
            if parent.value == node.type_array:
                result = result % '[%s]'
            elif parent.value == node.type_struct:
                result = result % '{%s}'
            else:
                # item
                assert(0)
        result = result % bson_data
    else:
        result = {unicode(name_and_path): bson_data}
    return result

def get_tables_data_from_oplog_set_command(schema_engine, bson_data):
    """ @return tables as dict """
    tables = {}
    print bson_data
    for name_and_path, bson_value in bson_data.iteritems():
        # exclude parent tables as they have no data for inserts
        components = parse_name_and_path(name_and_path)
        node = schema_engine.locate(components.keys())
        parent_tables_to_skip = [i.long_plural_alias()
                                 for i in node.all_parents() 
                                 if i.value == i.type_array][:-1]
        bson_rec = complete_partial_record(schema_engine, name_and_path, bson_value)
        table_obj = create_tables_load_bson_data(schema_engine, [bson_rec])
        for table_name, table in table_obj.tables.iteritems():
            # skip parent tables as they have no data
            if not table_name in parent_tables_to_skip:
                tables[table_name] = table
            else:
                print "skip table", table_name
    return tables

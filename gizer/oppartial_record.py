#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from logging import getLogger
from collections import namedtuple
from mongo_schema.schema_engine import Tables

PComponent = namedtuple('PComponent', ['name', 'index'])
PartialInsert = namedtuple('PartialInsert', ['tables', 'initial_indexes'])

def get_name_and_path_components(name_and_path):
    """ @return get dict of parsed components with values, value can be None"""
    res = []
    numerics = []
    names = []
    name_components = name_and_path.split('.')
    for comp in name_components:
        if unicode(comp).isnumeric():
            numerics.append(int(comp))
        else:
            if len(names) > len(numerics):
                numerics.append(None)
            names.append(comp)
    if len(names) > len(numerics):
        numerics.append(None)
    for i in xrange(len(names)):
        res.append(PComponent(names[i], numerics[i]))
    return res

def initial_indexes_from_components(schema_engine, components):
    res = {}
    locate_path = []
    for comp_i in xrange(len(components)):
        locate_path = [i.name for i in components[:comp_i+1]]
        node = schema_engine.locate(locate_path)
        if not components[comp_i].index:
            res[node.long_alias()] = 0
        else:
            res[node.long_alias()] = components[comp_i].index
    return res

def node_by_components(schema_engine, components):
    locate_path = [i.name for i in components]
    node = schema_engine.locate(locate_path)
    last_index = components[-1].index
    # if bson_data is considered as array item
    if node.value is node.type_array and last_index is not None:
        node = node.children[0]
    return node

def get_tables_data_from_oplog_set_command(schema_engine, bson_data,
                                           bson_object_id_name_value):
    """ @return list of PartianInsert(tables as dict, initial_indexes) """
    getLogger(__name__).debug("collection=%s, bson_data=%s, \
bson_object_id_name_value=%s", schema_engine.root_node.name,
                              str(bson_data), str(bson_object_id_name_value))
    res = []
    for name_and_path, bson_value in bson_data.iteritems():
        tables = {}
        components = get_name_and_path_components(name_and_path)
        initial_indexes = initial_indexes_from_components(schema_engine,
                                                          components)
        getLogger(__name__).debug("components=%s, initial_indexes=%s",
                                  components, initial_indexes)

        obj_id_name = bson_object_id_name_value.keys()[0]
        obj_id_val = bson_object_id_name_value.values()[0]
        node = node_by_components(schema_engine, components)
        whole_partial_bson = node.json_inject_data(
            bson_value, obj_id_name, obj_id_val)
        getLogger(__name__).debug("whole_partial_bson=%s", whole_partial_bson)
        table_obj = Tables(schema_engine, whole_partial_bson)
        table_obj.load_all()
        # exclude parent tables if they have no data
        parent_tables_to_skip = [i.long_plural_alias()
                                 for i in node.all_parents() 
                                 if i.value == i.type_array][:-1]
        for table_name, table in table_obj.tables.iteritems():
            # skip parent tables as they have no data
            if not table_name in parent_tables_to_skip:
                tables[table_name] = table
        getLogger(__name__).debug("whole_partial_bson skip empty tables=%s",
                                  str(parent_tables_to_skip))

        getLogger(__name__).debug("whole_partial_bson value tables=%s",
                                  str(tables.keys()))
        res.append(PartialInsert(tables=tables,
                                 initial_indexes=initial_indexes))
    return res

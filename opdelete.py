__author__ = 'Volodymyr Varchuk'
__email__       = "vladimir.varchuk@rackspace.com"

import json
import bson
from bson.json_util import loads
#from bson_processing import BsonProcessing

# "posts.3.comments.5"

def callback_internal(table, id, schema):
    res = []

    structtables = table.split (".");
    print(structtables)
#    for type_item in schema:
        #if type_item is list:

    condition = ''
    table_name = ''
    res.append("DELETE FROM {table} WHERE {condition};".format(table=table_name, condition=condition))
    return res

def spaces(spaces_count):
    str = '===='
    for i in range(0, spaces_count,1):
        str = str + '===='
    return str + '>'

def print_schema(schema, level):
    i = 0
    for it in schema:
        element = schema[it]
        if type(element) is dict:
            print_schema(element,level+1)
        elif type(element) is list:
            print('')
            for list_item in element:
                #print(list_item)
                if type(list_item) is dict:
                    print_schema(list_item,level+1)
                else:
                    print(spaces(level), 'level', level, type(list_item), list_item )
        else:
            print(spaces(level), 'level', level, type(element), it, element)

def get_list_ids(list, ids):
    sign = False;
    for it in list:
        if it in ['id', 'oid', '_id', '_oid']:
            sign = True
            ids.append(it)
    if not sign:
        ids.append('idx')
    return ids

def add_parental_table_name_to_ids(ids, table_name):
    new_ids =[]
    for id in ids:
        new_ids.append(table_name+'_'+id)
    return new_ids

def add_parental_ids(schema, level, ids, table):
    i = 0
    ids = get_list_ids(schema, ids)

    for it in schema:
        element = schema[it]
        if type(element) is dict:
            ids = get_list_ids(element, ids)
            add_parental_ids(element,level+1,add_parental_table_name_to_ids(ids, table),it)
        elif type(element) is list:
            print('')
            #ids = get_list_ids(element, ids)
            for list_item in element:
                if type(list_item) is dict:
                    add_parental_ids(list_item,level+1,add_parental_table_name_to_ids(ids, table),it)
                else:
                    print(spaces(level), 'level', level, type(list_item), list_item )
        else:
            print(spaces(level), 'level', level, type(element), it, element)
    print(table, ids)

data = open('/home/volodymyr/git/gizer/test_data/test_schema2.txt').read()
schema = json.loads(data)

#print_schema(schema=schema, level=0)
ids = []
table_name = 'main'
print (type(schema))
add_parental_ids(schema=schema, level=0, ids=ids, table=table_name)






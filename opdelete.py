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
        print(spaces(level), 'level', level, type(element), it, element)
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



data = open('/home/volodymyr/git/gizer/test_data/test_schema.txt').read()
schema = json.loads(data)
print_schema(schema=schema[0], level=0)







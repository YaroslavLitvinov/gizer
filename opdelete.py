__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"

import json
import pprint
import bson
from bson.json_util import loads
import collections


# from bson_processing import BsonProcessing

# "posts.3.comments.5"
# "main.relatives.2.contacts" id='asdasdasdaasdas'

# "DELETE FROM main_retaives_contacts where _id='asdasdasdaasdas'"
# "DELETE FROM main_retaives_contacts where main_relatives_contacts__id='asdasdasdaasdas'"
CREATE_SQLs = {}

def callback_internal(table, id, schema):
    res = []

    structtables = table.split(".");
    print(structtables)
    #    for type_item in schema:
    # if type_item is list:

    condition = ''
    table_name = ''
    res.append("DELETE FROM {table} WHERE {condition};".format(table=table_name, condition=condition))
    return res


def spaces(spaces_count):
    str = '===='
    for i in range(0, spaces_count, 1):
        str = str + '===='
    return str + '>'


def get_postgres_type(type_name):
    # TODO should be case insensitive
    return {
        'STRING': 'text',
        'INT': 'integer',
        'BOOL': 'boolean',
        'LONG': 'bigint'
    }[type_name]


def get_list_ids(lst, ids):
    list = lst[0]
    ids_to_add = {}
    for it in list:
        if it in ['id', 'oid', '_id', '_oid']:
            if type(list[it]) is dict:
                for id_item in list[it]:
                    if id_item in ['id', 'oid', '_id', '_oid']:
                        ids_to_add[it + '_' + id_item] = get_postgres_type(list[it][id_item])
            else:
                ids_to_add[it] = get_postgres_type(list[it])
    if len(ids_to_add):
        ids_to_add['idx'] = 'bigint'
    ids.update(ids_to_add)
    return ids


def add_parental_table_name_to_ids(ids, table_name):
    new_ids = {}
    for id in ids:
        new_ids[table_name + '_' + id] = ids[id]
    return new_ids


def get_child_dict_item(dict_items, field_list, parent_field):
    for it in dict_items:
        # TODO add handler when item has list type
        if type(dict_items[it]) is dict:
            field_list = get_child_dict_item(dict_items[it], field_list, parent_field + '_' + it)
        else:
            field_list[parent_field + '_' + it] = get_postgres_type(dict_items[it])
    return field_list


def generate_create_statement(table_name, ids_list, fields_list):
    CREATE_TAMPLATE = 'CREATE {table_name} (\n\t{ids}, \n\t{fields}\n)'
    max_len = max(max(map(len, fields_list)), max(map(len, ids_list)))
    template = '{0:max_len}\t{1}'.replace('max_len', str(max_len))
    ids = ', \n\t'.join([(template.format(key, value)) for (key, value) in ids_list.items()])
    fields = ', \n\t'.join([(template.format(key, value)) for (key, value) in fields_list.items()])
    return CREATE_TAMPLATE.format(table_name=table_name, ids=ids, fields=fields)


def generate_schema_with_parental_ids(schema, ids, table):
    field_list = {}
    ids = get_list_ids(schema, ids)
    new_schema = {}
    for item_list in schema:
        if type(item_list) is dict or type(item_list) is list:
            for it in item_list:
                item_value = item_list[it]
                if type(item_value) is dict:
                    field_list = get_child_dict_item(item_value, field_list, it)
                elif type(item_value) is list:
                    new_schema[it] = generate_schema_with_parental_ids(item_value,
                                                                       add_parental_table_name_to_ids(ids, table),
                                                                       table + '_' + it).copy()
                else:
                    field_list[it] = get_postgres_type(item_value)
                    # else:
                    # print(type(item_list), table, item_list, item_list)
        if len(field_list) == 0:
            field_list['data'] = get_postgres_type(item_list)
        all_fields = ids.copy()
        all_fields.update(field_list)
        print(table)
        new_schema[table] = all_fields
        #print(generate_create_statement(table, ids, field_list))
    CREATE_SQLs[table] = generate_create_statement(table, ids, field_list)
    return new_schema

def get_element(schema, path):
    s_path = path.split('.')
    element = schema
    for table_name in s_path:
        if not table_name.isdigit():
            element = element[table_name]
    return element


data = open('test_data/test_schema3.txt').read()
schema = json.loads(data)
table_name = 'main'

pp = pprint.PrettyPrinter(indent=4)
gen_schema = {}
gen_schema[table_name] = generate_schema_with_parental_ids(schema, {}, table_name)
#pp.pprint(gen_schema)

for item in CREATE_SQLs:
    print(CREATE_SQLs[item])


# "main.relatives.2.contacts" id='asdasdasdaasdas'
path = 'main.relatives.2.contacts'
id = 'asdasdasdaasdas'

pp.pprint(get_element(gen_schema, path))


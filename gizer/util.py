#!/usr/bin/env
"""Common utils."""
__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"

import bson

DELETE_TMPLT = 'DELETE FROM {table} WHERE {conditions};'
UPDATE_TMPLT = 'UPDATE {table} SET {statements} WHERE {conditions};'
INSERT_TMPLT = 'INSERT INTO {table} ({columns}) VALUES({values});'
SELECT_TMPLT = 'SELECT MAX(idx) FROM {table} WHERE {conditions};'

# UPSERT_TMLPT = """\
# LOOP
#     {update}
#     IF found THEN
#         RETURN;
#     END IF;
#     BEGIN
#         {insert}
#         RETURN;
#     EXCEPTION WHEN unique_violation THEN
#     END;
# END LOOP;
# """

UPSERT_TMLPT = 'do $$\
    begin\
    {update}\
    IF FOUND THEN\
        RETURN;\
    END IF;\
    BEGIN\
        {insert}\
        RETURN;\
    EXCEPTION WHEN unique_violation THEN\
    END;\
    end\
    $$'


def get_field_name_without_underscore(field_name):
    for i in range(len(field_name)):
        if field_name[i].isalpha():
            s = field_name[i:]
            break
    return s


def isIdField(field_name):
    if field_name in ['id', 'oid', '_id', '_oid', '_id_oid', 'id_oid']:
        return True
    else:
        return False


def get_table_name_schema(str_list):
    return '.'.join(filter(None,str_list))


def get_postgres_type(type_name):
    #TODO fix types
    return {
        'STRING': 'text',
        'INT': 'integer',
        'BOOLEAN': 'boolean',
        'LONG': 'bigint',
        'TIMESTAMP': 'timestamp',
        'DOUBLE': 'double',
        'TINYINT': 'integer'
    }[type_name.upper()]


def get_table_name_from_list(spath):
    spathl = spath[:]
    for it in spathl:
        if it.isdigit():
            spathl.remove(it)
    if len(spathl) > 1:
        return '_'.join(['_'.join(( el[:-1] if el[-1] == 's' else el) for el in spathl[:-1]), get_field_name_without_underscore(spathl[-1])])
    else:
        return spathl[-1]


def get_idx_column_name_from_list(spath):
    spathl = spath[:]
    for it in spathl:
        if it.isdigit():
            spathl.remove(it)
    if len(spathl) > 1:
        return '_'.join(['_'.join((el) for el in spathl[:-1]), spathl[-1]])
    else:
        return spathl[-1]


def get_root_table_from_path(path):
    spath = path.split('.')
    if len(spath) == 0:
        return path
    else:
        return spath[0]
    return get_table_name_from_list(collection_path)


def get_indexes_dictionary(path):
    index_dict = {}
    spath = path.split('.')
    iter_i = reversed(xrange(len(spath)))
    for i in iter_i:
        if spath[i].isdigit():
            table_name = get_table_name_from_list(spath)
            index_dict[table_name] = str(int(spath[i]) + 1)
            del spath[i]
            del spath[i - 1]
            next(iter_i)
        else:
            del spath[i]
    return index_dict


def get_indexes_dictionary_idx(path):
    index_dict = {}
    spath = path.split('.')
    iter_i = reversed(xrange(len(spath)))
    for i in iter_i:
        if spath[i].isdigit():
            table_name = get_idx_column_name_from_list(spath)
            index_dict[table_name] = str(int(spath[i]) + 1)
            del spath[i]
            del spath[i - 1]
            next(iter_i)
        else:
            del spath[i]
    return index_dict


def get_last_idx_from_path(path):
    spath = path.split('.')
    if spath[-1].isdigit():
        return str(int(spath[-1]) + 1)
    else:
        return None


def get_ids_list(lst, is_root):
    if type(lst) is list:
        list_it = lst[0]
    else:
        list_it = lst
    ids_to_add = {}
    for it in list_it:
        if isIdField(it) and is_root:
            if type(list_it[it]) is dict:
                for id_item in list_it[it]:
                    if isIdField(id_item) and is_root:
                        ids_to_add[get_field_name_without_underscore(
                            it + '_' + get_field_name_without_underscore(id_item))] = get_postgres_type(
                            list_it[it][id_item])
            else:
                ids_to_add[get_field_name_without_underscore(it)] = get_postgres_type(list_it[it])
    if len(ids_to_add) == 0:
        ids_to_add['idx'] = 'bigint'
    return ids_to_add


def get_tables_structure(schema, table, table_mappings, parent_tables_ids, root_table, parent_key):
    if type(schema) is list:
        table_struct = schema[0]
    else:
        table_struct = schema

    table_mappings[table] = {}

    for ids in parent_tables_ids:
        table_mappings[table][ids] = parent_tables_ids[ids]

    if not root_table:
        table_mappings[table][u'idx'] = u'bigint'
        parent_tables_ids[table + u'_idx'] = u'bigint'
    else:
        root_ids = get_ids_list(schema, 1)
        root_id_key = root_ids.iterkeys().next()
        parent_tables_ids[table + '_' + root_id_key] = root_ids[root_id_key].decode('utf-8')
    root_table = 0

    if not type(table_struct) is dict:
        table_mappings[table][get_field_name_without_underscore(parent_key)] = get_postgres_type(table_struct)
        return table_mappings

    for element in table_struct:
        if type(table_struct[element]) is list:
            get_tables_structure(table_struct[element], table[:-1] + '_' + get_field_name_without_underscore(element),
                                 table_mappings, parent_tables_ids.copy(), root_table, element)
        elif type(table_struct[element]) is dict:
            get_table_struct_from_dict(table_struct[element], table, table_mappings, parent_tables_ids.copy(),
                                       get_field_name_without_underscore(element))
        else:
            table_mappings[table][get_field_name_without_underscore(element)] = get_postgres_type(table_struct[element])
    return table_mappings


def get_table_struct_from_dict(schema, table, table_mappings, parent_tables_ids, parent_name):
    for column in schema:
        if type(schema[column]) is dict:
            get_table_struct_from_dict(schema[column], table, table_mappings, parent_tables_ids,
                                       parent_name + '_' + get_field_name_without_underscore(column))
        elif type(schema[column]) is list:
            get_tables_structure(schema[column], table[:-1] + '_' + parent_name + '_' + column, table_mappings,
                                 parent_tables_ids, 0, column)
        else:
            table_mappings[table][parent_name + '_' + get_field_name_without_underscore(column)] = get_postgres_type(
                schema[column])


def get_column_type(schema, table, field_name, collection_name):
    return get_tables_structure(schema, collection_name, {}, {}, 1, '')[table][field_name]


def get_quotes_using(schema, table, field_name, collection_name):
    quotes_not_needed = ['int', 'bigint', 'integer' ,'double']
    if get_column_type(schema, table, field_name, collection_name) in quotes_not_needed:
        return False
    else:
        return True

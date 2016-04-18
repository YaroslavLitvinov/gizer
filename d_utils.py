__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"

import string


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


def get_postgres_type(type_name):
    # TODO should be case insensitive
    # TODO should be replaced
    return {
        'STRING': 'text',
        'INT': 'integer',
        'BOOL': 'boolean',
        'LONG': 'bigint',
        'string': 'text',
        'int': 'integer',
        'bool': 'boolean',
        'long': 'bigint'
    }[type_name]


def get_table_name_from_list(spath):
    spathl = spath[:]
    for it in spathl:
        if it.isdigit():
            spathl.remove(it)
    if len(spathl) > 1:
        return '_'.join(['_'.join((el[:-1]) for el in spathl[:-1]), spathl[-1]])
    else:
        return spathl[-1]


def get_root_table_from_path(path):
    spath = path.split('.')
    collection_path = []
    remove_last = False
    for it in spath:
        if it.isdigit():
            remove_last = True
            break
        collection_path.append(it)
    if remove_last and 1 < len(collection_path):
        del collection_path[len(collection_path) - 1]
    return get_table_name_from_list(collection_path)


def get_indexes_dictionary(path):
    index_dict = {}
    spath = path.split('.')
    iter_i = reversed(xrange(len(spath)))
    for i in iter_i:
        if spath[i].isdigit():
            table_name = get_table_name_from_list(spath)
            index_dict[table_name] = spath[i]
            del spath[i]
            del spath[i - 1]
            next(iter_i)
        else:
            del spath[i]
    return index_dict


def get_last_idx_from_path(path):
    spath = path.split('.')
    if spath[-1].isdigit():
        return spath[-1]
    else:
        return None


def get_ids_list(lst):
    if type(lst) is list:
        list_it = lst[0]
    else:
        list_it = lst
    ids_to_add = {}
    for it in list_it:
        if isIdField(it):
            if type(list_it[it]) is dict:
                for id_item in list_it[it]:
                    if isIdField(id_item):
                        ids_to_add[get_field_name_without_underscore(
                            it + '_' + get_field_name_without_underscore(id_item))] = get_postgres_type(
                            list_it[it][id_item])
            else:
                ids_to_add[get_field_name_without_underscore(it)] = get_postgres_type(list_it[it])
    if len(ids_to_add) == 0:
        ids_to_add['idx'] = 'bigint'
    return ids_to_add


def get_tables_structure(schema, table, table_mappings, parent_tables_ids, root_table):
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
        root_ids = get_ids_list(schema)
        root_id_key = root_ids.iterkeys().next()
        parent_tables_ids[table + '_' + root_id_key] = root_ids[root_id_key].decode('utf-8')
    root_table = 0

    if not type(table_struct) is dict:
        table_mappings[table][u'data'] = get_postgres_type(table_struct)
        return table_mappings

    for element in table_struct:
        if type(table_struct[element]) is list:
            get_tables_structure(table_struct[element], table[:-1] + '_' + get_field_name_without_underscore(element),
                                 table_mappings, parent_tables_ids.copy(), root_table)
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
                                 parent_tables_ids, 0)
        else:
            table_mappings[table][parent_name + '_' + get_field_name_without_underscore(column)] = get_postgres_type(schema[column])


def get_column_type(schema, table, field_name, collection_name):
    return get_tables_structure(schema, collection_name, {}, {}, 1)[table][field_name]


def get_quotes_using(schema, table, field_name, collection_name):
    quotes_not_needed = ['int', 'bigint', 'integer']
    if get_column_type(schema, table, field_name, collection_name) in quotes_not_needed:
        return False
    else:
        return True

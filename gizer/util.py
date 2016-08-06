#!/usr/bin/env
"""Common utils."""
__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"

from collections import namedtuple

DELETE_TMPLT = 'DELETE FROM {table} WHERE {conditions};'
UPDATE_TMPLT = 'UPDATE {table} SET {statements} WHERE {conditions};'
INSERT_TMPLT = 'INSERT INTO {table} ({columns}) VALUES({values});'
SELECT_TMPLT = 'SELECT MAX(idx) FROM {table} WHERE {conditions};'

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

DatabaseInfo = namedtuple('DatabaseInfo', ['database_name', 'schema_name'])


def get_cleaned_field_name(field_name):
    """returns cleared field name for flat database"""
    for i in range(len(field_name)):
        if field_name[i].isalpha():
            ret_val = field_name[i:]
            break
    return ret_val


def is_id_field(field_name):
    """check if field is ID field"""
    return field_name in ['id', 'oid', '_id', '_oid', '_id_oid', 'id_oid']


def get_table_name_schema(str_list):
    """returns string which represented table name including data base name,
    schema name, table name"""
    str_list_quotes = str_list[:-1] + ['"' + str_list[-1] + '"']
    return '.'.join(filter(None, str_list_quotes))


def get_schema(schema_in):
    """returns schema dictionary from json"""
    if type(schema_in) is list:
        return schema_in[0]
    else:
        return schema_in


def get_schema_dict(schema_in):
    """returns schema dictionary from schema_engine object"""
    if type(schema_in) != dict:
        return schema_in.schema
    else:
        return schema_in


def get_cleaned_path(path):
    """returns list of path nodes excluding number parts"""
    new_path_clear = []
    for path_el in path:
        if not path_el.isdigit():
            new_path_clear.append(path_el)
    return new_path_clear


def get_postgres_type(type_name):
    """converts json schema type to PostgreSQL type"""
    return {
        'STRING': 'text',
        'INT': 'integer',
        'BOOLEAN': 'boolean',
        'LONG': 'bigint',
        'TIMESTAMP': 'timestamp',
        'DOUBLE': 'double precision',
        'TINYINT': 'integer'
    }[type_name.upper()]


def get_table_name_from_list(spath):
    """returns table name from nodes list"""
    spathl = spath[:]
    for spathl_it in spathl:
        if spathl_it.isdigit():
            spathl.remove(spathl_it)
    if len(spathl) > 1:
        return '_'.join(
            ['_'.join((spathl_el[:-1] if spathl_el[-1] == 's' else spathl_el)
                      for spathl_el in spathl[:-1]),
             get_cleaned_field_name(spathl[-1])])
    else:
        return spathl[-1]


def get_idx_column_name_from_list(spath):
    """returns list idx_columns"""
    spathl = spath[:]
    for spathl_it in spathl:
        if spathl_it.isdigit():
            spathl.remove(spathl_it)
    if len(spathl) > 1:
        return '_'.join(['_'.join((spathl_el) for spathl_el in spathl[:-1]),
                         spathl[-1]])
    else:
        return spathl[-1]


def get_root_table_from_path(path):
    """extract root table name from path"""
    spath = path.split('.')
    if len(spath) == 0:
        return path
    else:
        return spath[0]


def get_indexes_dictionary(path):
    """returns dictionary of indexes with values from path to object"""
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
    """returns dictionary of indexes with values from path to object with
    idx column name convention"""
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
    """returns last valid index number from path to object"""
    spath = path.split('.')
    if spath[-1].isdigit():
        return str(int(spath[-1]) + 1)
    else:
        return None


def get_ids_list(lst):
    """returns list of ids"""
    if type(lst) is list:
        list_it = lst[0]
    else:
        list_it = lst
    # search for _id/id :{oid, id_bscon} struct
    ids_to_add = {}
    for list_it_el in list_it:
        if get_cleaned_field_name(list_it_el) in ['id', '_id']:
            if type(list_it[list_it_el]) is dict:
                for e_el in list_it[list_it_el]:
                    if get_cleaned_field_name(e_el) in ["oid"]:
                        ids_to_add[get_cleaned_field_name(
                            list_it_el) + "_" + get_cleaned_field_name(
                            e_el)] = get_postgres_type(
                            list_it[list_it_el][e_el])
    if len(ids_to_add) != 0:
        return ids_to_add
    # search for _id/id fields
    for list_it_el in list_it:
        if get_cleaned_field_name(list_it_el) in ['id', '_id']:
            ids_to_add[
                get_cleaned_field_name(list_it_el)] = get_postgres_type(
                list_it[list_it_el])
    if len(ids_to_add) != 0:
        return ids_to_add
    # set index column to idx if id_oid or id not found
    ids_to_add['idx'] = 'bigint'
    return ids_to_add


def get_tables_structure(schema, table, table_mappings, parent_tables_ids,
                         root_table, parent_key):
    """returns structure of all tables in chema with following view:
        {table1_name:{
            field1_name:TYPE,
            field2_name:TYPE
        }}
    """
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
        parent_tables_ids[table + '_' + root_id_key] = root_ids[
            root_id_key].decode('utf-8')
    root_table = 0

    if not type(table_struct) is dict:
        table_mappings[table][
            get_cleaned_field_name(parent_key)] = get_postgres_type(
            table_struct)
        return table_mappings

    for element in table_struct:
        if type(table_struct[element]) is list:
            get_tables_structure(table_struct[element], table[
                                                        :-1] + '_' +
                                 get_cleaned_field_name(
                element),
                                 table_mappings, parent_tables_ids.copy(),
                                 root_table, element)
        elif type(table_struct[element]) is dict:
            get_table_struct_from_dict(table_struct[element], table,
                                       table_mappings, parent_tables_ids.copy(),
                                       get_cleaned_field_name(
                                           element))
        else:
            table_mappings[table][
                get_cleaned_field_name(element)] = get_postgres_type(
                table_struct[element])
    return table_mappings


def get_table_struct_from_dict(schema, table, table_mappings, parent_tables_ids,
                               parent_name):
    """returns tables structures for enclosed objects"""
    for column in schema:
        if type(schema[column]) is dict:
            get_table_struct_from_dict(schema[column], table, table_mappings,
                                       parent_tables_ids,
                                       parent_name + '_' +
                                       get_cleaned_field_name(
                                           column))
        elif type(schema[column]) is list:
            get_tables_structure(schema[column],
                                 table[:-1] + '_' + parent_name + '_' + column,
                                 table_mappings,
                                 parent_tables_ids, 0, column)
        else:
            table_mappings[table][
                parent_name + '_' + get_cleaned_field_name(
                    column)] = get_postgres_type(
                schema[column])


def get_column_type(schema, table, field_name, collection_name):
    """returns column type regarding schema"""
    return get_tables_structure(schema, collection_name, {}, {}, 1, '')[table][
        field_name]


def get_quotes_using(schema, table, field_name, collection_name):
    """returns true or false if qoutes needed to use regarding field type"""
    quotes_not_needed = ['int', 'bigint', 'integer', 'double']
    return not get_column_type(schema, table, field_name, collection_name) in \
            quotes_not_needed

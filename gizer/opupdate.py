""" opupdate module
Implementation of "delete" operation for "realtime" etl process for
transferring data from MongoDB nested collections to PostgreSQL flat data
with using pregenerated schema and tailing records (events) in oplog.rs
collection.
"""

__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"

import datetime

from gizer.opinsert import generate_insert_queries
from gizer.oppartial_record import get_tables_data_from_oplog_set_command
from gizer.opdelete import op_delete_stmts as delete, get_conditions_list
from gizer.util import get_tables_structure, get_table_name_from_list, \
    get_table_name_schema, UPDATE_TMPLT, UPSERT_TMLPT, INSERT_TMPLT, \
    get_schema, get_cleaned_path, get_schema_dict, get_cleaned_field_name
from collections import namedtuple
from logging import getLogger

import bson

OplogBranch = namedtuple('OplogBranch',
                         ['oplog_path', 'normalized_path', 'data',
                          'conditions_list', 'parsed_path', "object_id_field"])
ParsedObjPath = namedtuple('ParsedObjPath', ['table_path', 'column'])
RootInfo = namedtuple('RootInfo', ['root_id', 'root_collection'])
OplogInfo = namedtuple('OplogInfo', ['oplog_data_set',
                                     'oplog_data_object_id'])

def locate_in_schema(schema_in, path):
    """returns true if path is present in schema, false if not"""
    schema = get_schema(schema_in)
    new_path_clear = get_cleaned_path(path)

    current_element = new_path_clear[0]
    if current_element in schema.keys():
        if len(new_path_clear) > 1:
            new_path = new_path_clear[1:]
            if type(schema[current_element]) is list:
                next_element = schema[current_element][0]
            elif type(schema[current_element]) is dict:
                next_element = schema[current_element]

            if len(new_path) >= 1:
                return locate_in_schema(next_element, new_path)
            else:
                return new_path in next_element.keys()
        else:
            return True
    else:
        return False


def get_part_schema(schema_in, path):
    """returns 'child' part of the schema related to path"""
    schema = get_schema(schema_in)
    w_path = []
    if type(path) is list:
        w_path = get_cleaned_path(path)
        current_path = w_path[0]
    else:
        current_path = path

    if current_path in schema.keys():
        if type(schema[current_path]) is dict:
            if len(w_path) > 1:
                return get_part_schema(schema[current_path], w_path[1:])
            else:
                return schema[current_path]
        elif type(schema[current_path]) is list:
            if type(w_path) is list:
                if len(w_path[1:]) == 0:
                    return schema[current_path]
                else:
                    return get_part_schema(schema[current_path], w_path[1:])
            else:
                return schema[current_path]
        else:
            return schema[current_path]


def get_elements_list(schema, path, paths):
    """returns all 'child' elements in schema joined with parent path"""
    for schema_el in schema:
        gen_p = '.'.join(path + [schema_el])
        if type(schema[schema_el]) is dict:
            get_elements_list(schema[schema_el], path + [schema_el], paths)
        elif type(schema[schema_el]) is list:
            paths.append({gen_p: []})
        else:
            paths.append({gen_p: None})
    return paths


def normalize_unset_oplog_recursive(schema, oplog_data, parent_path,
                                    branch_list, root_info):
    """convert oplog raw data for unset operaiotn to 'normalized' format for
    subsequent use"""
    #  root_id, root_collection packed into root_info
    if type(oplog_data) is dict:
        for element in oplog_data:
            element_path = '.'.join(parent_path + element.split('.')).split('.')
            parsed_path = parse_column_path(
                '.'.join([root_info.root_collection] + element_path))
            element_conditios_list = get_conditions_list(schema,
                                        parsed_path.table_path,
                                        root_info.root_id.itervalues().next())
            if not locate_in_schema(schema, element_path):
                getLogger(__name__).warning(
                    '{0} not in schema. SKIPPED!'.format(element_path))
                continue
            elements_to_set_null_untyped = get_part_schema(schema, element_path)
            if type(elements_to_set_null_untyped) is dict:
                elements_to_set_null = elements_to_set_null_untyped.copy()
            elif type(elements_to_set_null_untyped) is list:
                elements_to_set_null = []
            else:
                elements_to_set_null = None

            if type(elements_to_set_null) is dict:
                elements_list = get_elements_list(elements_to_set_null, [], [])
                for elements_list_el in elements_list:
                    parsed_path = parse_column_path('.'.join(
                        [root_info.root_collection, element,
                         elements_list_el.iterkeys().next()]))
                    branch_list.append(
                        OplogBranch('', element + '.' +
                                    elements_list_el.iterkeys().next(),
                                    elements_list_el.itervalues().next(),
                                    element_conditios_list, parsed_path, None))
            elif type(elements_to_set_null) is list:
                parsed_path = parse_column_path(
                    '.'.join([root_info.root_collection] + parent_path +
                             [element]))
                branch_list.append(
                    OplogBranch('', '.'.join(parent_path + [element]), [],
                                element_conditios_list, parsed_path, None))
            else:
                parsed_path = parse_column_path(
                    '.'.join([root_info.root_collection] + parent_path +
                             [element]))
                branch_list.append(
                    OplogBranch('', '.'.join(parent_path + [element]), None,
                                element_conditios_list, parsed_path, None))

    return branch_list


def normalize_oplog_recursive(schema, oplog_data, parent_path, branch_list,
                              root_info):
    """convert oplog raw data for set operaiotn to 'normalized' format for
    subsequent use"""
    #  root_id, root_collection packed into root_info
    if type(oplog_data) is dict:
        for element in oplog_data:
            element_path = '.'.join(parent_path + element.split('.')).split('.')
            parsed_path = parse_column_path(
                '.'.join([root_info.root_collection] + element_path))
            element_conditios_list = get_conditions_list(schema,
                                        parsed_path.table_path,
                                        root_info.root_id.itervalues().next())
            if not locate_in_schema(schema, element_path):
                getLogger(__name__).warning(
                    '{0} not in schema. SKIPPED!'.format(element_path))
                continue
            if type(get_part_schema(schema, element_path)) is list:
                if oplog_data[element] == None:
                    oplog_data[element] = []
                if not element_path[-1].isdigit() and type(
                        oplog_data[element]) != list:
                    oplog_data[element] = []
            if type(oplog_data[element]) is dict:
                branch_list = normalize_oplog_recursive(schema,
                                                oplog_data[element],
                                                parent_path[:] + [element],
                                                branch_list,
                                                root_info)
                if len(oplog_data[element]) == 0 or oplog_data[element] is None:
                    prepared_oplog_data = {element:True}
                    branch_list = normalize_unset_oplog_recursive(schema,
                                                            prepared_oplog_data,
                                                            parent_path,
                                                            branch_list,
                                                            root_info)
            else:
                if type(oplog_data[element]) is bson.objectid.ObjectId:
                    # convert bson.objectid.ObjectId to two fileds _id.oid and
                    # _id.bsontype
                    parsed_path_oid = parse_column_path(
                        '.'.join([root_info.root_collection] + element_path +
                                 ['oid']))
                    branch_list.append(
                        OplogBranch('',
                                    '.'.join(parent_path + [element + '.oid']),
                                    str(oplog_data[element]),
                                    element_conditios_list, parsed_path_oid,
                                    oplog_data[element]))
                    parsed_path_bsontype = parse_column_path('.'.join(
                        [root_info.root_collection] + element_path +
                        ['bsontype']))
                    branch_list.append(
                        OplogBranch('', '.'.join(
                            parent_path + [element + '.bsontype']), 7,
                                    element_conditios_list,
                                    parsed_path_bsontype, None))
                elif type(get_part_schema(schema, element_path)) is dict and \
                                oplog_data[element] is None:
                    prepared_oplog_data = {element: True}
                    branch_list = normalize_unset_oplog_recursive(schema,
                                                            prepared_oplog_data,
                                                            parent_path,
                                                            branch_list,
                                                            root_info)
                else:
                    branch_list.append(
                        OplogBranch('', '.'.join(parent_path + [element]),
                                    oplog_data[element], element_conditios_list,
                                    parsed_path, None))
    else:
        if locate_in_schema(schema, oplog_data):
            parsed_path = parse_column_path(
                '.'.join([root_info.root_collection] + parent_path))
            element_conditios_list = get_conditions_list(schema,
                                        parsed_path.table_path,
                                        root_info.root_id.itervalues().next())
            branch_list.append(
                OplogBranch('.'.join(parent_path), '', oplog_data,
                            element_conditios_list, None))
    return branch_list


def get_grouped_branches(normalized_branch_list):
    """returns branches which are grouped by target rows"""
    grouped_branch_list = {}
    for branch in normalized_branch_list:
        if branch.parsed_path.table_path in grouped_branch_list.keys():
            grouped_branch_list[branch.parsed_path.table_path].append(branch)
        else:
            grouped_branch_list[branch.parsed_path.table_path] = [branch]
    return grouped_branch_list


def normalize_set_unset_op_type(schema, oplog_data, root_table_name,
                                oplog_data_object_id):
    """returns normalized branches list for both set and unset operations"""
    if '$set' in oplog_data['o'].keys():
        oplog_data_set = oplog_data['o']['$set']
        normalized_branch_list = normalize_oplog_recursive(schema,
                                            oplog_data_set, [],
                                            [], RootInfo(get_obj_id(oplog_data),
                                                           root_table_name))
    else:
        oplog_data_set = oplog_data['o']['$unset']
        normalized_branch_list = normalize_unset_oplog_recursive(schema,
                                            oplog_data_set,
                                            [], [],
                                            RootInfo(oplog_data_object_id,
                                            root_table_name))
    return normalized_branch_list


def get_event_type(oplog_data):
    """retruns number which represent event type set or unset"""
    # detecting what kind of operation will be performed
    # 1 = set
    # 2 = unset
    if '$set' in oplog_data['o'].keys():
        operation_type = 1
    else:
        operation_type = 2
    return operation_type



def update(dbreq, schema_e, oplog_data, database_info):
    """generates all sqls related to set or unset event in oplog"""
    # database_name, schema_name packed to DatabaseInfo namedtuple
    schema = get_schema_dict(schema_e)
    ret_val = []
    root_table_name = oplog_data['ns'].split('.')[-1]
    tables_mappings = get_tables_structure(schema, root_table_name, {}, {}, 1,
                                           '')
    # grouping branches by target table
    grouped_branch_list = get_grouped_branches(normalize_set_unset_op_type(
        schema, oplog_data, root_table_name, oplog_data['o2']))

    # parse and join branches to single SQL statement to all updations to each
    # single table and each single record
    # one branch set to one record
    for g_branch in grouped_branch_list:
        for branch in grouped_branch_list[g_branch]:
            if type(branch.data) is list:
                # added check datatype in schema.
                if type(get_part_schema(schema, branch.normalized_path.split(
                        '.'))) is list:
                    oplog_info = OplogInfo({branch.normalized_path:branch.data},
                                        oplog_data['o2'])
                    ret_val.extend(
                        update_list(dbreq, schema_e, '.'.join([root_table_name]
                                                    + [branch.normalized_path]),
                                    oplog_info, database_info))
                else:
                    getLogger(__name__).warning(
                        '{0} specified as {1} in schema, but presented as list\
                         in data. SKIPPED!'.format(
                            branch.normalized_path, get_part_schema(schema,
                                            branch.normalized_path.split(
                                            '.'))))
        for branch in grouped_branch_list[g_branch]:
            if not type(branch.data) is list:
                target_table = get_table_name_from_list(
                    branch.parsed_path.table_path.split('.'))
                # columns from root_object
                dest_column_list_with_value = {}
                for set_column_branch in grouped_branch_list[g_branch]:
                    if not type(set_column_branch.data) is list:
                        # generating column name. Also in case of enclosed
                        # objects
                        col_list = []
                        if set_column_branch.parsed_path.column == '':
                            # if column is empty it means structure
                            # like this : [INT]. this structure shoud be
                            # transformed to structure
                            # with next view [ parent_element_name:INT ]
                            column_name = \
                                set_column_branch.parsed_path.table_path.split(
                                    '.')[
                                    -2]
                        else:
                            column_name = set_column_branch.parsed_path.column

                        for col_part in column_name.split('.'):
                            col_list.append(
                                get_cleaned_field_name(col_part))
                        column_dest_name = '_'.join(col_list)

                        # make dictionary column_name:value with type checking
                        dest_column_list_with_value[
                            column_dest_name] = get_correct_type_value(
                            tables_mappings,
                            target_table,
                            column_dest_name,
                            set_column_branch.data)

                condition_str = ' and '.join(
                    ['"{column}"=(%s)'.format(column=col) for col in
                     sorted(branch.conditions_list['target'])])
                statements_to_set_str = ', '.join(
                    ['"{column}"=(%s)'.format(column=column_dest_name) for
                     column_dest_name in
                     sorted(dest_column_list_with_value)])
                target_table_str = get_table_name_schema(
                    [database_info.database_name, database_info.schema_name,
                     target_table])
                upd_statement_template = UPDATE_TMPLT.format(
                    table=target_table_str, statements=statements_to_set_str,
                    conditions=condition_str)
                upd_values = [dest_column_list_with_value[column_dest_name] for
                              column_dest_name in
                              sorted(dest_column_list_with_value)] + [
                                 branch.conditions_list['target'][col] for col
                                 in
                                 sorted(branch.conditions_list['target'])]
                # here is a question. is it possible to to make upset operation
                # in mongo to unexisting enclosed record
                if target_table != root_table_name and \
                        (get_event_type(oplog_data) != 2):
                    # As we don`t know if updatetd object is already exist we
                    # are generating INSERT statements for  enclosed objects in
                    # array and concatenate their with UPDATE statement to
                    # "UPSERT" operation for postgres
                    ret_val.append(generate_upsert_statements(branch,
                                                dest_column_list_with_value,
                                                target_table_str,
                                                upd_statement_template,
                                                upd_values))
                else:
                    ret_val.append(
                    {upd_statement_template: [tuple(upd_values)]})
                break
            else:
                continue
    return ret_val


def generate_upsert_statements(branch, dest_column_list_with_value,
                               target_table_str, upd_statement_template,
                               upd_values):
    """generates upsert statement"""
    columns_list_ins = \
        [col for col in sorted(branch.conditions_list['target'])]\
        + \
        [column_dest_name for column_dest_name in sorted(
            dest_column_list_with_value)]
    ins_values = [branch.conditions_list['target'][col] for
                       col in
                       sorted(branch.conditions_list[
                                  'target'])] + [
                          dest_column_list_with_value[
                              column_dest_name] for
                          column_dest_name in
                          sorted(dest_column_list_with_value)]

    columns_list_str = ', '.join(['"{0}"'.format(col_name) for col_name in
         columns_list_ins])
    values_list_str = ', '.join('%s' for count_el in columns_list_ins)
    insert_statement_template = INSERT_TMPLT.format(
        table=target_table_str, columns=columns_list_str,
        values=values_list_str)
    upsert_statement_template = UPSERT_TMLPT.format(
        update=upd_statement_template,
        insert=insert_statement_template)
    upsert_values = upd_values + ins_values
    return {upsert_statement_template: [tuple(upsert_values)]}



def insert_wrapper(schema_e, oploda_data_set, oplog_data_object_id,
                   schema_name):
    """wrapper for insert operation"""
    get_tables_data_from_oplog_set = get_tables_data_from_oplog_set_command(
        schema_e, oploda_data_set,
        oplog_data_object_id)
    ins_stmnt = {}
    for set_el in get_tables_data_from_oplog_set:
        for table in set_el.tables.itervalues():
            ins_result = generate_insert_queries(table, schema_name, "",
                                         set_el.initial_indexes)
            ins_stmnt[ins_result[0]] = ins_result[1]
    return ins_stmnt


def parse_column_path(path):
    """parse column path"""
    # parse full column path.
    # split into table path and column path
    if type(path) is list:
        w_path = path
    else:
        w_path = path.split('.')
    last_digit_index = 0
    for i, elemnt in enumerate(w_path):
        if elemnt.isdigit():
            last_digit_index = i
    if not last_digit_index == 0:
        parsed_path = ParsedObjPath('.'.join(w_path[:last_digit_index + 1]),
                                    '.'.join(w_path[last_digit_index + 1:]))
    else:
        parsed_path = ParsedObjPath('.'.join(w_path[:1]), '.'.join(w_path[1:]))
    return parsed_path


def update_list(dbreq, schema_e, upd_path_str, oplog_info, database_info):
    """returns SQLs statements and values for deleting and updateing array
    object"""
    # database_name, schema_name packed to DatabaseInfo
    schema = get_schema_dict(schema_e)
    ret_val = []
    doc_id = get_obj_id_recursive(oplog_info.oplog_data_object_id, [], [])
    del_stmnt = delete(dbreq, schema, upd_path_str, doc_id.itervalues().next(),
                       database_info)
    for del_stmnt_op in del_stmnt:
        if type(del_stmnt[del_stmnt_op]) is dict:
            for k in del_stmnt[del_stmnt_op]:
                ret_val.append({k: [tuple(del_stmnt[del_stmnt_op][k])]})
    insert_stmnts = insert_wrapper(schema_e, oplog_info.oplog_data_set,
                                   oplog_info.oplog_data_object_id,
                                   database_info.schema_name)
    if insert_stmnts != {}:
        ret_val.append(insert_stmnts)
    return ret_val



def get_correct_type_value(tables_mappings, table, column, value, ):
    """do check if data type in particular field in oplod matches with datatype
    for corresponding column in schema and fix it if needed"""
    types = {
        'integer': int,
        'boolean': bool,
        'double precision': float,
        'bigint': long,
        'timestamp': datetime.datetime
    }
    if value is None:
        return value
    if table in tables_mappings.keys():
        if column in tables_mappings[table].keys():
            column_type = tables_mappings[table][column]
            if column_type in types.keys():
                if isinstance(value, types[column_type]):
                    return value
                else:
                    if column_type == 'double precision':
                        if isinstance(value, types['integer']) or isinstance(
                                value, types['bigint']):
                            return float(value)
                        # :)
                        else:
                            return None
                    else:
                        return None
            else:
                return value


def get_obj_id(oplog_data):
    """wrapper for get_obj_id_recursive"""
    return get_obj_id_recursive(oplog_data["o2"], [], [])


def get_obj_id_recursive(data, name, value_id):
    """returns column name and value for vaues with ObjectII type"""
    if type(data) is dict:
        next_column = data.iterkeys().next()
    name.append(get_cleaned_field_name(next_column))
    if type(data[next_column]) == bson.objectid.ObjectId:
        name.append('oid')
        value_id.append(str(data[next_column]))
    if type(data[next_column]) is dict:
        get_obj_id_recursive(data[next_column], name, value_id)
    else:
        value_id.append(data[next_column])
    return {'_'.join(name): value_id[0]}

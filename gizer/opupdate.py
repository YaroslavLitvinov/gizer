import datetime

from gizer.opinsert import *
from gizer.oppartial_record import get_tables_data_from_oplog_set_command

from opdelete import op_delete_stmts as delete, get_conditions_list
from util import *
from collections import namedtuple
from logging import getLogger

import bson

OplogBranch = namedtuple('OplogBranch',
                         ['oplog_path', 'normalized_path', 'data', 'conditions_list', 'parsed_path', "object_id_field"])
ParsedObjPath = namedtuple('ParsedObjPath', ['table_path', 'column'])


def locate_in_schema(schema_in, path):
    if type(schema_in) is list:
        schema = schema_in[0]
    else:
        schema = schema_in
    new_path_clear = []
    for el in path:
        if not el.isdigit():
            new_path_clear.append(el)

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
                if new_path in next_element.keys():
                    return True
                else:
                    return False
        else:
            return True
    else:
        return False


def get_part_schema(schema_in, path):
    if type(schema_in) is list:
        schema = schema_in[0]
    else:
        schema = schema_in
    w_path = []
    if type(path) is list:
        for el in path:
            if not el.isdigit():
                w_path.append(el)
        # current_path = path[0]
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
    for el in schema:
        gen_p = '.'.join(path + [el])
        if type(schema[el]) is dict:
            get_elements_list(schema[el], path + [el], paths)
        elif type(schema[el]) is list:
            paths.append({gen_p: []})
        else:
            paths.append({gen_p: None})
    return paths


def normalize_unset_oplog_recursive(schema, oplog_data, parent_path, branch_list, root_id, root_collection):
    if type(oplog_data) is dict:
        for element in oplog_data:
            element_path = '.'.join(parent_path + element.split('.')).split('.')
            parsed_path = parse_column_path('.'.join([root_collection] + element_path))
            element_conditios_list = get_conditions_list(schema, parsed_path.table_path, root_id.itervalues().next())
            if not locate_in_schema(schema, element_path):
                getLogger(__name__).warning('{0} not in schema. SKIPPED!'.format(element_path))
                continue
            elements_to_set_null_untyped = get_part_schema(schema, element_path)
            if type(elements_to_set_null_untyped) is dict:
                elements_to_set_null = elements_to_set_null_untyped.copy()
            elif type(elements_to_set_null_untyped) is list:
                elements_to_set_null = []
            else:
                print('single element')
                elements_to_set_null = None

            if type(elements_to_set_null) is dict:
                elements_list = get_elements_list(elements_to_set_null, [], [])
                # full_paths = []
                for el in elements_list:
                    # full_paths.append({element + '.' + el.iterkeys().next() :el.itervalues().next() })
                    parsed_path = parse_column_path('.'.join([root_collection, element, el.iterkeys().next()]))
                    # branch_list.append( OplogBranch('', '.'.join(parent_path+[element]), oplog_data[element], element_conditios_list, parsed_path))
                    branch_list.append(OplogBranch('', element + '.' + el.iterkeys().next(), el.itervalues().next(),
                                                   element_conditios_list, parsed_path, None))
                    # print('paths', full_paths)
            elif type(elements_to_set_null) is list:
                parsed_path = parse_column_path('.'.join([root_collection] + parent_path + [element]))
                branch_list.append(
                    OplogBranch('', '.'.join(parent_path + [element]), [], element_conditios_list, parsed_path, None))
            else:
                parsed_path = parse_column_path('.'.join([root_collection] + parent_path + [element]))
                branch_list.append(
                    OplogBranch('', '.'.join(parent_path + [element]), None, element_conditios_list, parsed_path, None))

                # branch_list.append( OplogBranch('', '.'.join(parent_path+[element+'.oid']), str(oplog_data[element]), element_conditios_list, parsed_path_oid))
                # if type(get_part_schema(schema, element_path)) is list:
                #     if oplog_data[element] == None:
                #        oplog_data[element] = []
    return branch_list


def normalize_oplog_recursive(schema, oplog_data, parent_path, branch_list, root_id, root_collection):
    if type(oplog_data) is dict:
        for element in oplog_data:
            element_path = '.'.join(parent_path + element.split('.')).split('.')
            parsed_path = parse_column_path('.'.join([root_collection] + element_path))
            element_conditios_list = get_conditions_list(schema, parsed_path.table_path, root_id.itervalues().next())
            if not locate_in_schema(schema, element_path):
                getLogger(__name__).warning('{0} not in schema. SKIPPED!'.format(element_path))
                continue
            if type(get_part_schema(schema, element_path)) is list:
                if oplog_data[element] == None:
                    oplog_data[element] = []
            if type(oplog_data[element]) is dict:
                branch_list = normalize_oplog_recursive(schema, oplog_data[element], parent_path[:] + [element],
                                                        branch_list, root_id, root_collection)
            else:
                if type(oplog_data[element]) is bson.objectid.ObjectId:
                    # convert bson.objectid.ObjectId to two fileds _id.oid and _id.bsontype
                    parsed_path_oid = parse_column_path('.'.join([root_collection] + element_path + ['oid']))
                    branch_list.append(
                        OplogBranch('', '.'.join(parent_path + [element + '.oid']), str(oplog_data[element]),
                                    element_conditios_list, parsed_path_oid, oplog_data[element]))
                    parsed_path_bsontype = parse_column_path('.'.join([root_collection] + element_path + ['bsontype']))
                    branch_list.append(
                        OplogBranch('', '.'.join(parent_path + [element + '.bsontype']), 7, element_conditios_list,
                                    parsed_path_bsontype, None))
                else:
                    branch_list.append(
                        OplogBranch('', '.'.join(parent_path + [element]), oplog_data[element], element_conditios_list,
                                    parsed_path, None))
    else:
        print('SINGLE element')
        if locate_in_schema(schema, oplog_data):
            parsed_path = parse_column_path('.'.join([root_collection] + parent_path))
            element_conditios_list = get_conditions_list(schema, parsed_path.table_path, root_id.itervalues().next())
            branch_list.append(OplogBranch('.'.join(parent_path), '', oplog_data, element_conditios_list, None))
    return branch_list


def update(dbreq, schema_e, oplog_data, database_name, schema_name):
    if type(schema_e) != dict:
        schema = schema_e.schema
    else:
        schema = schema_e
    oplog_data_object_id = oplog_data['o2']
    oplog_data_ns = oplog_data['ns']
    ret_val = []
    root_table_name = oplog_data_ns.split('.')[-1]
    tables_mappings = get_tables_structure(schema, root_table_name, {}, {}, 1, '')
    # detecting what kind of operation will be performed
    # 1 = set
    # 2 = unset
    operation_type = 0
    if '$set' in oplog_data['o'].keys():
        operation_type = 1
        oplog_data_set = oplog_data['o']['$set']
        normalized_branch_list = normalize_oplog_recursive(schema, oplog_data_set, [], [], get_obj_id(oplog_data),
                                                           root_table_name)
    else:
        operation_type = 2
        oplog_data_set = oplog_data['o']['$unset']
        normalized_branch_list = normalize_unset_oplog_recursive(schema, oplog_data_set, [], [], oplog_data_object_id,
                                                                 root_table_name)

    # grouping branches by target table
    grouped_branch_list = {}
    for branch in normalized_branch_list:
        if branch.parsed_path.table_path in grouped_branch_list.keys():
            grouped_branch_list[branch.parsed_path.table_path].append(branch)
        else:
            grouped_branch_list[branch.parsed_path.table_path] = [branch]
    # parse and join branches to single SQL statement to all updations to each single table and each single record
    # one branch set to one record
    for g_branch in grouped_branch_list:
        g_branches = grouped_branch_list[g_branch]
        for branch in g_branches:
            if type(branch.data) is list:
                # added check datatype in schema.
                if type(get_part_schema(schema, branch.normalized_path.split('.'))) is list:
                    ret_val.extend(update_list(dbreq, schema_e, '.'.join([root_table_name] + [branch.normalized_path]),
                                               {branch.normalized_path: branch.data}, oplog_data_object_id,
                                               database_name, schema_name))
                else:
                    getLogger(__name__).warning(
                        '{0} specified as {1} in schema, but presented as list in data. SKIPPED!'.format(
                            branch.normalized_path, get_part_schema(schema, branch.normalized_path.split('.'))))
        for branch in g_branches:
            if not type(branch.data) is list:
                target_table = get_table_name_from_list(branch.parsed_path.table_path.split('.'))
                # columns from root_object
                dest_column_list_with_value = {}
                for set_column_branch in g_branches:
                    if not type(set_column_branch.data) is list:
                        # generating column name. Also in case of enclosed objects
                        col_list = []
                        if set_column_branch.parsed_path.column == '':
                            # if column is empty it means structure like this : [INT].
                            # this structure shoud be transformed to structure with next view [ parent_element_name:INT ]
                            column_name = set_column_branch.parsed_path.table_path.split('.')[-2]
                        else:
                            column_name = set_column_branch.parsed_path.column

                        for col_part in column_name.split('.'):
                            col_list.append(get_field_name_without_underscore(col_part))
                        column_dest_name = '_'.join(col_list)
                        # make dictionary column_name:value with type checking
                        dest_column_list_with_value[column_dest_name] = get_correct_type_value(tables_mappings,
                                                                                               target_table,
                                                                                               column_dest_name,
                                                                                               set_column_branch.data)
                condition_str = ' and '.join(
                    ['{column}=(%s)'.format(column=col) for col in sorted(branch.conditions_list['target'])])
                statements_to_set_str = ', '.join(
                    ['{column}=(%s)'.format(column=column_dest_name) for column_dest_name in
                     sorted(dest_column_list_with_value)])
                target_table_str = get_table_name_schema([database_name, schema_name, target_table])
                upd_statement_template = UPDATE_TMPLT.format(table=target_table_str, statements=statements_to_set_str,
                                                             conditions=condition_str)
                upd_values = [dest_column_list_with_value[column_dest_name] for column_dest_name in
                              sorted(dest_column_list_with_value)] + [branch.conditions_list['target'][col] for col in
                                                                      sorted(branch.conditions_list['target'])]
                # here is a question. is it possible to to make upset operation in mongo to unexisting enclosed record
                if target_table != root_table_name and (operation_type != 2):
                    #  As we don`t know if updatetd object is already exist we are generating INSERT statements for
                    # enclosed objects in array and concatenate their with UPDATE statement to "UPSERT" operation for postgres
                    columns_list_ins = [col for col in sorted(branch.conditions_list['target'])] + [column_dest_name for
                                                                                                    column_dest_name in
                                                                                                    sorted(
                                                                                                        dest_column_list_with_value)]
                    values_list_ins = [branch.conditions_list['target'][col] for col in
                                       sorted(branch.conditions_list['target'])] + [
                                          dest_column_list_with_value[column_dest_name] for column_dest_name in
                                          sorted(dest_column_list_with_value)]

                    columns_list_str = ', '.join(columns_list_ins)
                    values_list_str = ', '.join('%s' for el in columns_list_ins)
                    insert_statement_template = INSERT_TMPLT.format(table=target_table_str, columns=columns_list_str,
                                                                    values=values_list_str)
                    upsert_statement_template = UPSERT_TMLPT.format(update=upd_statement_template,
                                                                    insert=insert_statement_template)
                    ins_values = values_list_ins
                    upsert_values = upd_values + ins_values
                    ret_val.append({upsert_statement_template: [tuple(upsert_values)]})
                else:
                    ret_val.append({upd_statement_template: [tuple(upd_values)]})
                break
            else:
                continue
    return ret_val


def insert_wrapper(schema_e, oploda_data_set, oplog_data_object_id, schema_name):
    get_tables_data_from_oplog_set = get_tables_data_from_oplog_set_command(schema_e, oploda_data_set,
                                                                            oplog_data_object_id)
    ins_stmnt = {}
    for set_el in get_tables_data_from_oplog_set:
        for name, table in set_el.tables.iteritems():
            rr = generate_insert_queries(table, schema_name, "", set_el.initial_indexes)
            ins_stmnt[rr[0]] = rr[1]
    return ins_stmnt


def parse_column_path(path):
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
        parsed_path = ParsedObjPath('.'.join(w_path[:last_digit_index + 1]), '.'.join(w_path[last_digit_index + 1:]))
    else:
        parsed_path = ParsedObjPath('.'.join(w_path[:1]), '.'.join(w_path[1:]))
    return parsed_path


def update_list(dbreq, schema_e, upd_path_str, oplog_data_set, oplog_data_object_id, database_name, schema_name):
    if type(schema_e) != dict:
        schema = schema_e.schema
    else:
        schema = schema_e
    ret_val = []
    doc_id = get_obj_id_recursive(oplog_data_object_id, [], [])
    del_stmnt = delete(dbreq, schema, upd_path_str, doc_id.itervalues().next(), database_name, schema_name)
    for op in del_stmnt:
        if type(del_stmnt[op]) is dict:
            for k in del_stmnt[op]:
                ret_val.append({k: [tuple(del_stmnt[op][k])]})
    insert_stmnts = insert_wrapper(schema_e, oplog_data_set, oplog_data_object_id, schema_name)
    if insert_stmnts != {}:
        ret_val.append(insert_stmnts)
    return ret_val


def is_root_object(path):
    if type(path) is list:
        temp_path = path
    else:
        temp_path = path.split('.')

    for elenemt in temp_path:
        if elenemt.isdigit():
            return False
    return True


def get_correct_type_value(tables_mappings, table, column, value, ):
    # def is_date(string):
    #     try:
    #         datetime.datetime.
    #         parse(string)
    #         return True
    #     except ValueError:
    #         return False
    # 'STRING': 'text',
    # 'INT': 'integer',
    # 'BOOLEAN': 'boolean',
    # 'LONG': 'bigint',
    # 'TIMESTAMP': 'timestamp',
    # 'DOUBLE': 'double',
    # 'TINYINT': 'integer'

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
                        if isinstance(value, types['integer']) or isinstance(value, types['bigint']):
                            return float(value)
                        # :)
                        else:
                            return None
                    else:
                        return None
            else:
                return value


def get_obj_id(oplog_data):
    return get_obj_id_recursive(oplog_data["o2"], [], [])


def get_obj_id_recursive(data, name, value_id):
    if type(data) is dict:
        next_column = data.iterkeys().next()
    name.append(get_field_name_without_underscore(next_column))
    if type(data[next_column]) == bson.objectid.ObjectId:
        name.append('oid')
        value_id.append(str(data[next_column]))
    if type(data[next_column]) is dict:
        get_obj_id_recursive(data[next_column], name, value_id)
    else:
        value_id.append(data[next_column])
    return {'_'.join(name): value_id[0]}


def get_query_columns_with_nested(schema, u_data, parent_path, columns_list):
    for k in u_data:
        if parent_path <> '':
            column_name = parent_path + '_' + get_field_name_without_underscore(k)
        else:
            column_name = get_field_name_without_underscore(k)
        if type(u_data[k]) is dict:
            get_query_columns_with_nested(schema, u_data[k], column_name, columns_list).copy()
        # if type(u_data[k]) in list:
        #     print('update list {0}'.format(parent_path) )
        #     pass
        if type(u_data[k]) == bson.objectid.ObjectId:
            columns_list[column_name + '_oid'] = str(u_data[k])
            columns_list[column_name + '_bsontype'] = 7
        else:
            columns_list[column_name] = u_data[k]
    return columns_list

#!/usr/bin/env python
"""Update callback."""

import itertools
import datetime

from gizer.opinsert import *
from gizer.oppartial_record import get_tables_data_from_oplog_set_command

from opdelete import op_delete_stmts as delete, get_conditions_list
from util import *

import bson



def localte_in_schema(schema_in, path):
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
                return localte_in_schema(next_element, new_path)
            else:
                if new_path in next_element.keys():
                    return  True
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
    if type(path) is list:
        current_path = path[0]
    else:
        current_path = path

    if current_path in schema.keys():
        if type(schema[current_path]) is dict:
            if len(path) > 1:
                return get_part_schema(schema[current_path], path[1:])
            else:
                return schema[current_path]
        elif type(schema[current_path]) is list:
            if type(path) is list:
                if len(path[1:]) == 0:
                    return schema[current_path]
                else:
                    return get_part_schema(schema[current_path], path[1:])# schema[path[0]]
            else:
                    return schema[current_path]
        else:
            return schema[current_path]


def unset(dbreq, schema_e, oplog_data_unset, oplog_data_object_id,root_table_name, tables_mappings, database_name, schema_name):
    if type(schema_e) != dict:
        schema = schema_e.schema
    else:
        schema = schema_e
    ret_val = []
    tables_mappings = get_tables_structure(schema, root_table_name, {}, {}, 1, '')
    for element in oplog_data_unset:
        updating_obj = element.split('.')
        if not localte_in_schema(schema[0], updating_obj):
            continue
        last_digit_index = 1
        is_root = True
        for i, path_el in enumerate(updating_obj):
            if path_el.isdigit():
                is_root = False
                last_digit_index = i

        if is_root:
            s_part = get_part_schema(schema,updating_obj)
            if not type(s_part) is list:
                last_digit_index = 0

        if last_digit_index == 0:
            unset_table_path = [root_table_name]
            unset_object_path = updating_obj
            unset_target_table_path = [root_table_name]
        else:
            unset_table_path =  updating_obj[:last_digit_index+1]
            unset_object_path = updating_obj[last_digit_index+1:]
            unset_target_table_path = [root_table_name] + unset_table_path
        doc_id = get_obj_id_recursive(oplog_data_object_id, [], [])
        '.'.join(unset_target_table_path)
        cond_list = get_conditions_list(schema, '.'.join([root_table_name] + unset_table_path),doc_id.itervalues().next())
        unset_object_path_column = '_'.join([get_field_name_without_underscore(column) for column in unset_object_path])
        target_table = get_table_name_from_list(unset_target_table_path)
        set_to_null_columns_list = {}
        for column in tables_mappings[target_table]:
            if column.startswith(unset_object_path_column+'_'):
                set_to_null_columns_list[column] = None
        if len(set_to_null_columns_list) > 0:
            statements_str = ', '.join(['{column}=(%s)'.format(column=col) for col in set_to_null_columns_list])
            conditions_str = ' and '.join(['{column}=(%s)'.format(column=col) for col in sorted(cond_list['target'])])
            upd_stmnt = UPDATE_TMPLT.format( table='.'.join(filter(None, [database_name, schema_name, target_table])), statements=statements_str, conditions=conditions_str )
            ret_val.append({upd_stmnt:[tuple([set_to_null_columns_list[col] for col in set_to_null_columns_list]+[cond_list['target'][col] for col in sorted(cond_list['target']) ])]})
        if target_table[:-1] + '_' + unset_object_path_column in tables_mappings.keys():
            del_stmnt = delete(dbreq, schema, root_table_name + '.' + element, doc_id.itervalues().next(), database_name, schema_name)
            for op in del_stmnt:
                if type(del_stmnt[op]) is dict:
                    for k in del_stmnt[op]:
                        ret_val.append({k:[tuple(del_stmnt[op][k])]})
        else:
            conditions_str_child = ' and '.join(['{column}=(%s)'.format(column=col) for col in sorted(cond_list['child'])])
            pattern_locate_table_name = target_table[:-1] + '_' + unset_object_path_column + '_'
            for table in tables_mappings.keys():
                if table.startswith(pattern_locate_table_name):
                    del_stamnt = DELETE_TMPLT.format(table = '.'.join(filter(None, [database_name, schema_name, table])), conditions = conditions_str_child)
                    ret_val.append({del_stamnt:[tuple([cond_list['child'][col] for col in sorted(cond_list['child']) ])]})
        return ret_val


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
    if '$set' in oplog_data['o'].keys():
        oplog_data_set = oplog_data['o']['$set']
    else:
        return unset(dbreq,schema_e,oplog_data['o']['$unset'], oplog_data_object_id, root_table_name,tables_mappings,database_name,schema_name)
    is_root_object_updated = False
    for element in oplog_data_set:
        updating_obj = element.split('.')
        upd_path = [root_table_name] + updating_obj
        if not localte_in_schema(schema[0], updating_obj):
            continue

        if len(updating_obj) > 1:
            last_digit = -1
            for i, path_el in enumerate(updating_obj):
                if path_el.isdigit():
                    last_digit = i
            if last_digit >= 0:
                if last_digit == len(updating_obj) - 1:
                    new_oplog_data_set = {element:oplog_data_set[element]}
                    ret_val.extend(update_cmd(dbreq,schema_e,new_oplog_data_set,oplog_data_object_id,oplog_data_ns,tables_mappings,database_name,schema_name))
                else:
                    new_oplog_data_set = oplog_data_set[element]
                    for i in reversed(range(last_digit + 1, len(updating_obj))):
                        new_oplog_data_set = {updating_obj[i]:new_oplog_data_set}
                    new_oplog_data_set = { '.'.join(updating_obj[:last_digit+1]):new_oplog_data_set}
                    ret_val.extend(update_cmd(dbreq,schema_e,new_oplog_data_set,oplog_data_object_id,oplog_data_ns,tables_mappings,database_name,schema_name))
        else:
            if type(get_part_schema(schema,element)) is list:
                new_oplog_data_set = {element:oplog_data_set[element]}
                ret_val.extend(update_cmd(dbreq,schema_e,new_oplog_data_set,oplog_data_object_id,oplog_data_ns,tables_mappings,database_name,schema_name))
            else:
                # ret_val.extend (update_cmd(dbreq,schema_e,oplog_data_set,oplog_data_object_id,oplog_data_ns,tables_mappings,database_name,schema_name))
                if not is_root_object_updated:
                    ret_val.extend (update_cmd(dbreq,schema_e,oplog_data_set,oplog_data_object_id,oplog_data_ns,tables_mappings,database_name,schema_name))
                    is_root_object_updated = True
    # print(ret_val)
    return ret_val

def update_list (dbreq, schema_e, upd_path_str, oplog_data_set, oplog_data_object_id, database_name, schema_name):
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
                ret_val.append({k:[tuple(del_stmnt[op][k])]})
    tables, initial_indexes \
        = get_tables_data_from_oplog_set_command(schema_e,
                                                 oplog_data_set,
                                                 oplog_data_object_id)
    for name, table in tables.iteritems():
        rr = generate_insert_queries(table, schema_name, "", initial_indexes)

        ret_val.append({rr[0]:rr[1]})
    return  ret_val

def is_root_object(path):
    if type(path) is list:
        temp_path = path
    else:
        temp_path = path.split('.')

    for elenemt in temp_path:
        if elenemt.isdigit():
            return False
    return True

def update_cmd (dbreq, schema_e, oplog_data_set, oplog_data_object_id, oplog_data_ns, tables_mappings, database_name, schema_name):
    #compatibility with schema object for insert
    if type(schema_e) != dict:
        schema = schema_e.schema
    else:
        schema = schema_e

    doc_id = get_obj_id_recursive(oplog_data_object_id, [], [])
    u_data = oplog_data_set
    root_table_name = oplog_data_ns.split('.')[-1]
    ret_val = []

    k = u_data.iterkeys().next()
    updating_obj = k.split('.')
    upd_path = [root_table_name] + updating_obj
    upd_path_str = '.'.join(upd_path)
    ins_stmnt = {}
    del_stmnt = {}
    upd_stmnt = {}

    if not updating_obj[-1].isdigit():
        if type(u_data[k]) is list:
            ret_val.extend(update_list(dbreq,schema_e, upd_path_str, oplog_data_set, oplog_data_object_id,database_name,schema_name))
        else:
            id_column = doc_id.iterkeys().next()
            unfiltered_q_columns = get_query_columns_with_nested(schema, u_data, '', {})
            q_columns = {}
            if is_root_object(k):
                if type(u_data[k]) is dict:
                    for element in u_data[k]:
                        locate_element_path = [k, element]
                        if localte_in_schema(schema, locate_element_path):
                            if type(get_part_schema(schema, locate_element_path)) is list:
                                delete_path = '.'.join([root_table_name] + locate_element_path)
                                ins_obj = '.'.join([k, element])
                                # new_oplog_data_set =  {k:{element:u_data[k][element]}}
                                new_oplog_data_set =  {ins_obj:u_data[k][element]}
                                ret_val.extend(update_list(dbreq,schema_e,delete_path,new_oplog_data_set,oplog_data_object_id,database_name,schema_name))
                for column in unfiltered_q_columns:
                    updated_obj_split = updating_obj
                    if column in tables_mappings[root_table_name].keys():
                        if localte_in_schema(schema, updated_obj_split):
                            q_columns[column] = get_correct_type_value(tables_mappings,root_table_name,column, unfiltered_q_columns[column])
                    else:
                        if column in tables_mappings[root_table_name].keys():
                            q_columns[column] = get_correct_type_value(tables_mappings,root_table_name,column, unfiltered_q_columns[column])
                    if localte_in_schema(schema, updated_obj_split):
                        if type(get_part_schema(schema,updated_obj_split)) is list:
                            delete_path = '.'.join([root_table_name, column])
                            ret_val.extend(update_list(dbreq,schema_e,delete_path,{column:oplog_data_set[column]},oplog_data_object_id,database_name,schema_name))
            else:
                print('non root obj', k)
            # q_statements_list = [('{column}=(%s)' if not get_quotes_using(schema, root_table_name, col, root_table_name) else '{column}=(%s)').format(column=col) for col in q_columns]
            # q_conditions = ('{column}=(%s)' if not get_quotes_using(schema, root_table_name, id_column,root_table_name) else '{column}=(%s)').format( column=id_column)
            q_statements_list = ['{column}=(%s)'.format(column=col) for col in q_columns]
            if len(q_statements_list) == 0:
                return ret_val
            q_conditions = '{column}=(%s)'.format( column=id_column)
            upd_statement_template = UPDATE_TMPLT.format(
                table=get_table_name_schema([database_name, schema_name, root_table_name]),
                statements=', '.join(q_statements_list), conditions=q_conditions)
            upd_values = [q_columns[col] for col in q_columns] + [doc_id.itervalues().next()]
            ret_val.append({upd_statement_template:[tuple(upd_values)]})
    else:
        # update nested element
        if type(u_data[k]) is dict:
            for element in u_data[k]:
                if not localte_in_schema(schema, updating_obj + [element]):
                    continue
                if type(u_data[k][element]) is list:
                    oplog_data_set_list = {}
                    upd_path_str = '.'.join([upd_path_str, element])
                    upd_list_result = update_list(dbreq, schema_e,upd_path_str, {'.'.join([k, element]):u_data[k][element]},oplog_data_object_id,database_name,schema_name)
                    for q_element in upd_list_result:
                        ret_val.append(q_element)

            target_table_name = get_table_name_from_list(upd_path)
            q_conditions = get_conditions_list(schema, '.'.join(upd_path), doc_id.itervalues().next())
            q_columns_unfiltered = get_query_columns_with_nested(schema, u_data[k], '', {})
            q_columns= {}
            for it in q_columns_unfiltered:
                if it in tables_mappings[target_table_name].keys():
                    q_columns[it] =  get_correct_type_value(tables_mappings, target_table_name,it, q_columns_unfiltered[it])
            if len(q_columns) == 0:
                return ret_val
            q_statements_str = ', '.join(['{column}=(%s)'.format(column=col) for col in q_columns])
            q_conditions_str = ' and '.join(['{column}=(%s)'.format(column=col) for col in q_conditions['target']])
            upd_statement_template = UPDATE_TMPLT.format(
                table=get_table_name_schema([database_name, schema_name, target_table_name]),
                statements=q_statements_str, conditions=q_conditions_str)
            upd_values = [q_columns[col] for col in q_columns] + [q_conditions['target'][col] for col in q_conditions['target']]
            tables, initial_indexes \
                = get_tables_data_from_oplog_set_command(schema_e,
                                                         oplog_data_set,
                                                         oplog_data_object_id)

            for name, table in tables.iteritems():
                rr = generate_insert_queries(table, schema_name, "", initial_indexes)
                ins_stmnt[rr[0]] = rr[1]
            # TODO miltiple queries in case of enclosed objects
            insert_statement_template = ins_stmnt.iterkeys().next()
            upsert_statement_template = UPSERT_TMLPT.format(update=upd_statement_template,
                                                            insert=insert_statement_template)
            ins_values = list(ins_stmnt[insert_statement_template][0])
            upsert_values = upd_values + ins_values
            upd_stmnt[upsert_statement_template] = upsert_values

            ret_val.append({upsert_statement_template:[tuple(upsert_values)]})
        else:
            q_conditions = get_conditions_list(schema, '.'.join(upd_path), doc_id.itervalues().next())
            q_conditions_str = ' and '.join(['{column}=(%s)'.format(column=col) for col in q_conditions['target']])
            q_values = [q_conditions['target'][col] for col in q_conditions['target']]
            target_table_name = get_table_name_from_list(upd_path)
            target_table_name_db_schema = get_table_name_schema([database_name, schema_name, target_table_name])
            item_value = u_data[k]
            column_name = ''
            for it in k.split('.'):
                if not it.isdigit():
                    column_name = it
            columns_str = ', '.join([column for column in q_conditions['target']] + [column_name])
            values_str = ', '.join(['%s' for column in tables_mappings[target_table_name].keys()])
            checked_value = get_correct_type_value(tables_mappings,target_table_name,column_name,item_value)
            #TODO move to Yaroslavs INSERT
            ins_stmnt = INSERT_TMPLT.format( table=target_table_name_db_schema, columns=columns_str, values=values_str)
            upd_stmnt = UPDATE_TMPLT.format( table=target_table_name_db_schema, statements='{column_name}=(%s)'.format(column_name=column_name), conditions=q_conditions_str)
            upsetrt_tmplt = UPSERT_TMLPT.format(update = upd_stmnt, insert=ins_stmnt)
            ret_val.append({upsetrt_tmplt:[tuple([checked_value] + q_values + q_values + [checked_value])]})
    return ret_val

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
        'integer':int,
        'boolean':bool,
        'double precision':float,
        'bigint':long,
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
    return {'_'.join(name):value_id[0]}

def get_query_columns_with_nested (schema, u_data, parent_path, columns_list):
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
            columns_list[column_name+'_oid'] = str(u_data[k])
            columns_list[column_name+'_bsontype'] = 7
        else:
            columns_list[column_name] = u_data[k]
    return columns_list

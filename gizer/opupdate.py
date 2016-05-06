#!/usr/bin/env python
"""Update callback."""

import itertools

from gizer.opinsert import *
from gizer.oplog_parser import OplogQuery
from gizer.oppartial_record import get_tables_data_from_oplog_set_command

from opdelete import op_delete_stmts as delete, get_conditions_list
from util import *

import bson
import datetime


UPSERT_TMLPT = """\
LOOP
    {update}
    IF found THEN
        RETURN;
    END IF;
    BEGIN
        {insert}
        RETURN;
    EXCEPTION WHEN unique_violation THEN
    END;
END LOOP;
"""


def update (schema_e, oplog_data):
    #compatibility with schema object for insert
    if type(schema_e) != dict:
        schema = schema_e.schema
    else:
        schema = schema_e

    doc_id = get_obj_id(oplog_data)
    u_data = oplog_data["o"]["$set"]
    root_table_name = oplog_data["ns"].split('.')[-1]
    collection_name = oplog_data["ns"]
    k = u_data.iterkeys().next()
    updating_obj = k.split('.')
    # simple object update
    upd_path = [root_table_name] + updating_obj
    upd_path_str = '.'.join(upd_path)
    ins_stmnt = {}
    del_stmnt = {}
    upd_stmnt = {}
    if not updating_obj[-1].isdigit():
        # update array object.In that case we need to use two combine of two operations
        # delete old array (all elements linked to parent record) and insert new one (all elements listed in oplog query)
        if type(u_data[k]) is list:
            del_stmnt = delete(schema, upd_path_str, doc_id.itervalues().next())

            tables, initial_indexes \
                = get_tables_data_from_oplog_set_command(schema_e,
                                                     oplog_data['o']['$set'],
                                                     oplog_data['o2'])
            res = []
            for name, table in tables.iteritems():
                rr = generate_insert_queries(table, "", "", initial_indexes)
                res.append(OplogQuery("u", rr))
                ins_stmnt[rr[0]] = list(rr[1][0])
        else:
            #update root object just simple update needed
            id_column = doc_id.iterkeys().next()
            q_columns = get_query_columns_with_nested(schema, u_data, '', {})
            q_statements_list = [ ('{column}=(%s)' if not get_quotes_using(schema,root_table_name,col,root_table_name) else '{column}=(%s)').format(column=col) for col in q_columns]
            q_conditions = ('{column}=(%s)' if not get_quotes_using(schema,root_table_name,id_column,root_table_name) else '{column}=(%s)').format(column = id_column)
            upd_statement_template = UPDATE_TMPLT.format( table = root_table_name, statements = ', '.join(q_statements_list), conditions = q_conditions)
            upd_values = [q_columns[col] for col in q_columns] + [doc_id.itervalues().next()]
            upd_stmnt[upd_statement_template] = upd_values
    else:
        # update nested element
        if type(u_data[k]) is dict:

            # insert statement should to be added  in case when updated record  does not exist
            # we should execute only one of the insert or update statements
            target_table_name = get_table_name_from_list(upd_path)
            q_conditions = get_conditions_list(schema, '.'.join(upd_path), doc_id.itervalues().next())
            q_columns = get_query_columns_with_nested(schema, u_data[k], '', {})
            # q_statements_str = ', '.join([('{column}=(%s)' if not get_quotes_using(schema,target_table_name,col,root_table_name) else '{column}=(%s)').format(column=col) for col in q_columns])
            # q_conditions_str = ' and '.join([('{column}=(%s)' if not get_quotes_using(schema,target_table_name,col,root_table_name) else '{column}=(%s)').format(column = col) for col in q_conditions['target']])
            q_statements_str = ', '.join(['{column}=(%s)'.format(column=col) for col in q_columns])
            q_conditions_str = ' and '.join(['{column}=(%s)'.format(column = col) for col in q_conditions['target']])
            upd_statement_template = UPDATE_TMPLT.format( table=target_table_name, statements=q_statements_str, conditions=q_conditions_str)
            upd_values = [q_columns[col] for col in q_columns] + [q_conditions['target'][col] for col in q_conditions['target']]
            upd_stmnt[upd_statement_template] = upd_values
            # scratch insert statements for single object
            #TODO should be calculated idx number and placed into INSERT query. OR just replace generating insert from opinsert module
            q_values_template = ['%s' if not get_quotes_using(schema,target_table_name,col,root_table_name) else '%s'  for col in q_columns]
            ins_statement_template = INSERT_TMPLT.format( table=target_table_name, columns=', '.join([col for col in q_columns]), values=', '.join(q_values_template))
            ins_values = [q_columns[col] for col in q_columns] + [q_conditions['target'][col] for col in q_conditions['target']]
            #
            upd_stmnt[upd_statement_template] = upd_values
            #ins_stmnt = {INSERT_TMPLT:[]}
    ret_val = []
    #TODO need to be fixed. result of delete opertion should be just single dictionary, where: keys - SQL template, values - values for template (data, conditions, ids)
    for op in del_stmnt:
        if type(del_stmnt[op]) is dict:
            for k in del_stmnt[op]:
                ret_val.append({k:del_stmnt[op][k]})
    for op in upd_stmnt:
        ret_val.append({op:upd_stmnt[op]})
    for op in ins_stmnt:
        ret_val.append({op:ins_stmnt[op]})
    return ret_val


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
        if type(u_data[k]) == bson.objectid.ObjectId:
            columns_list[column_name+'_oid'] = str(u_data[k])
        else:
            columns_list[column_name] = u_data[k]
    return columns_list
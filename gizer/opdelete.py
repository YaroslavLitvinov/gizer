"""opdelete module

Implementation of "delete" operation for "realtime" etl process for
transferring data from MongoDB nested collections to PostgreSQL flat data
with using pregenerated schema and tailing records (events) in oplog.rs
collection.

How to use:

    del = op_delete_stmts(dbreq, schema, path, str_id, database_name,
        schema_name)

    parameters:
        dbreq - connection to PostgreSQL,
        schema - schema of nested data represented as json object,
        path - path to object for deleteion
        str_id - string representation of root ObjectID
        database_name - database name for destionation database (PostgreSQL)
        schema_name - schema name for destionation database (PostgreSQL)

    return value:
        as delete operation is an complicated operation it usualy comes in
        combination of sets two kinds of single operations UPDATE and DELETE for
        PostgreSQL and retruned value has following view
        {
        'upd': {
            'UPDATE database_name.schema_name."table_name" SET idx=(%s) WHERE
                idx=(%s), parent_id_iod=(%s);': [1, 2, 'abc'],
            'UPDATE database_name.schema_name."table_name" SET idx=(%s) WHERE
                idx=(%s), parent_id_iod=(%s);': [2, 3, 'abc']
        },
        'del': {
            'DELETE FROM database_name.schema_name."table_name" WHERE
                idx=(%s), parent_id_iod=(%s);': [1, 'abc']
        }
"""

__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"

from gizer.util import get_idx_column_name_from_list, SELECT_TMPLT, \
    UPDATE_TMPLT, DELETE_TMPLT, get_indexes_dictionary_idx, \
    get_root_table_from_path, get_ids_list, get_table_name_from_list, \
    get_tables_structure, get_table_name_schema, get_last_idx_from_path
import psycopg2


def op_delete_stmts(dbreq, schema, path, str_id, database_info):
    """delete operation wrapper"""
    return gen_statements(dbreq, schema, path, str_id, database_info)


def get_max_id_in_array(dbreq, table, condition_list, database_info):
    """returns value fo max index in array from where object shod be deleted"""
    cond_list = {}
    for column in condition_list['target']:
        if column != 'idx':
            cond_list[column] = condition_list['target'][column]
    where = get_where_templates({'target': cond_list, 'child': {}})['target']
    sql_query = SELECT_TMPLT.format(
        table='.'.join([database_info.database_name, database_info.schema_name,
                        table]),
        conditions=where['template'])
    if not type(dbreq) == psycopg2.extensions.cursor:
        curs = dbreq.cursor()
    else:
        curs = dbreq
    curs.execute(sql_query, tuple(where['values']))
    idx = curs.fetchone()[0]
    if idx is None:
        idx = 0
    return idx


def get_conditions_list(schema, path, str_id):
    """returns conditions list for target and for 'child' tables based on path
    and sring represented root object ID value
    """
    spath = path.split('.')
    parental_tables_idx_list = get_indexes_dictionary_idx(path)
    target_table = get_table_name_from_list(spath)
    target_table_idxname_for_child = get_idx_column_name_from_list(spath)
    params_target = {}
    params_child = {}
    root_table = get_root_table_from_path(path)
    for parent_table_idx in parental_tables_idx_list:
        if parent_table_idx != root_table:
            if target_table_idxname_for_child != parent_table_idx:
                params_target[parent_table_idx + '_idx'] = \
                    parental_tables_idx_list[parent_table_idx]
                params_child[parent_table_idx + '_idx'] = \
                    parental_tables_idx_list[parent_table_idx]
            else:
                params_target['idx'] = parental_tables_idx_list[
                    parent_table_idx]
                params_child[parent_table_idx + '_idx'] = \
                    parental_tables_idx_list[parent_table_idx]
    ids = get_ids_list(schema)
    root_id = ids.iterkeys().next()
    if root_table == target_table:
        params_target[root_id] = str(str_id)
    else:
        params_target[root_table + '_' + root_id] = str(str_id)

    params_child[root_table + '_' + root_id] = str(str_id)
    return {'target': params_target, 'child': params_child}


def get_where_templates(conditions_list):
    """generates where templates for target and 'child' tables based on
    conditions list"""

    def condition_with_quotes(key):
        """returns templae for value for query in wrapped in quotes or
         not depending on value type"""
        temp = ''
        if key.endswith('_idx') or key == 'idx':
            temp = '({0}=(%s))'.format(key)
        else:
            temp = '({0}=(%s))'.format(key)
        return temp

    where_list = {'target': {}, 'child': {}}
    where_list['target']['template'] = ' and '.join(
        sorted([(condition_with_quotes(key)) for key in
                conditions_list['target']]))
    where_list['target']['values'] = [conditions_list['target'][key] for key in
                                      sorted(conditions_list['target'])]
    where_list['child']['template'] = ' and '.join(
        sorted(
            [(condition_with_quotes(key)) for key in conditions_list['child']]))
    where_list['child']['values'] = [conditions_list['child'][key] for key in
                                     sorted(conditions_list['child'])]

    return where_list


def gen_statements(dbreq, schema, path, str_id, database_info):
    """generates all SQL statements with parameteres related for oplog event
    related to delete operation"""
    tables_mappings = get_tables_structure(schema, path.split('.')[0], {}, {},
                                           1, '')
    conditions_list = get_conditions_list(schema, path, str_id)
    where_clauses = get_where_templates(conditions_list)
    target_table = get_table_name_from_list(path.split('.'))
    if not target_table in tables_mappings.keys():
        return {'del': {}, 'upd': {}}
    tables_list = []
    for table in tables_mappings.keys():
        if str.startswith(str(table), target_table[:-1], 0,
                          len(table)) and not table == target_table:
            tables_list.append(table)
    del_statements = {}
    del_statements[DELETE_TMPLT.format(
        table=get_table_name_schema([database_info.database_name,
                                     database_info.schema_name, target_table]),
        conditions=where_clauses['target']['template'])] = \
        where_clauses['target']['values']
    for table in tables_list:
        del_statements[DELETE_TMPLT.format(
            table=get_table_name_schema([database_info.database_name,
                                         database_info.schema_name, table]),
            conditions=where_clauses['child']['template'])] = \
            where_clauses['child']['values']
    update_statements = {}
    idx = get_last_idx_from_path(path)
    if idx == None:
        return {'del': del_statements, 'upd': update_statements}
    max_idx = get_max_id_in_array(dbreq, target_table, conditions_list,
                                  database_info)
    if idx <= max_idx:
        return {'del': del_statements, 'upd': update_statements}

    for ind in range(int(idx) + 1, int(max_idx) + 1):
        spath = path.split('.')
        del spath[-1]
        spath.append(str(ind - 1))
        path_to_update = '.'.join(spath)
        udpate_where = get_where_templates(
            get_conditions_list(schema, path_to_update, str_id))
        update_statements[UPDATE_TMPLT.format(table=get_table_name_schema(
            [database_info.database_name, database_info.schema_name,
             target_table]),
            statements='idx=' + str(ind - 1),
            conditions=udpate_where['target'][
                'template'])] = \
            udpate_where['target'][
                'values']

        for table in tables_list:
            update_statements[UPDATE_TMPLT.format(table=get_table_name_schema(
                [database_info.database_name, database_info.schema_name,
                 table]),
                statements=get_idx_column_name_from_list(path.split('.')) +
                           '_idx=' + str(ind - 1),
                conditions=
                udpate_where['child'][
                    'template'])] = \
                udpate_where['child'][
                    'values']
    return {'del': del_statements, 'upd': update_statements}

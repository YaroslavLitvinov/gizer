__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"

from util import *
import psycopg2


def op_delete_stmts(dbreq, schema, path, id, database_name, schema_name):
    return gen_statements(dbreq, schema, path, id, database_name, schema_name)


def get_max_id_in_array(dbreq, table, condition_list, database_name, schema_name):
    cond_list = {}
    for column in condition_list['target']:
        if column <> 'idx':
            cond_list[column] = condition_list['target'][column]
    where = get_where_templates({'target': cond_list, 'child': {}})['target']
    SQL_query = SELECT_TMPLT.format(table='.'.join([database_name, schema_name, table]), conditions=where['template'])
    if not type(dbreq) == psycopg2.extensions.cursor:
        curs = dbreq.cursor()
    else:
        curs = dbreq
    curs.execute(SQL_query, tuple(where['values']))
    idx = curs.fetchone()[0]
    if idx is None:
        idx = 0
    return idx


def get_conditions_list(schema, path, id):
    spath = path.split('.')
    parental_tables_idx_list = get_indexes_dictionary_idx(path)
    target_table = get_table_name_from_list(spath)
    target_table_idxname_for_child = get_idx_column_name_from_list(spath)
    params_target = {}
    params_child = {}
    root_table = get_root_table_from_path(path)
    for it in parental_tables_idx_list:
        if it <> root_table:
            if target_table_idxname_for_child <> it:
                params_target[it + '_idx'] = parental_tables_idx_list[it]
                params_child[it + '_idx'] = parental_tables_idx_list[it]
            else:
                params_target['idx'] = parental_tables_idx_list[it]
                params_child[it + '_idx'] = parental_tables_idx_list[it]
    ids = get_ids_list(schema, 1)
    root_id = ids.iterkeys().next()
    if root_table == target_table:
        params_target[root_id] = str(id)
    else:
        params_target[root_table + '_' + root_id] = str(id)

    params_child[root_table + '_' + root_id] = str(id)
    return {'target': params_target, 'child': params_child}


def get_where_templates(conditions_list):
    def condition_with_quotes(key):
        temp = ''
        if key.endswith('_idx') or key == 'idx':
            temp = '({0}=(%s))'.format(key)
        else:
            temp = '({0}=(%s))'.format(key)
        return temp

    where_list = {'target': {}, 'child': {}}
    where_list['target']['template'] = ' and '.join(
        sorted([(condition_with_quotes(key)) for key in conditions_list['target']]))
    where_list['target']['values'] = [conditions_list['target'][key] for key in sorted(conditions_list['target'])]
    where_list['child']['template'] = ' and '.join(
        sorted([(condition_with_quotes(key)) for key in conditions_list['child']]))
    where_list['child']['values'] = [conditions_list['child'][key] for key in sorted(conditions_list['child'])]

    return where_list


def gen_statements(dbreq, schema, path, id, database_name, schema_name):
    tables_mappings = get_tables_structure(schema, path.split('.')[0], {}, {}, 1, '')
    conditions_list = get_conditions_list(schema, path, id)
    where_clauses = get_where_templates(conditions_list)
    target_table = get_table_name_from_list(path.split('.'))
    if not target_table in tables_mappings.keys():
        return {'del': {}, 'upd': {}}
    target_table_idx_name = get_idx_column_name_from_list(path.split('.'))
    tables_list = []
    for table in tables_mappings.keys():
        if str.startswith(str(table), target_table[:-1], 0, len(table)) and not table == target_table:
            tables_list.append(table)
    del_statements = {}
    del_statements[DELETE_TMPLT.format(table=get_table_name_schema([database_name, schema_name, target_table]),
                                       conditions=where_clauses['target']['template'])] = \
        where_clauses['target']['values']
    for table in tables_list:
        del_statements[DELETE_TMPLT.format(table=get_table_name_schema([database_name, schema_name, table]),
                                           conditions=where_clauses['child']['template'])] = \
            where_clauses['child']['values']
    update_statements = {}
    idx = get_last_idx_from_path(path)
    if idx == None:
        return {'del': del_statements, 'upd': update_statements}
    max_idx = get_max_id_in_array(dbreq, target_table, conditions_list, database_name, schema_name)
    if idx <= max_idx:
        return {'del': del_statements, 'upd': update_statements}

    for ind in range(int(idx) + 1, int(max_idx) + 1):
        spath = path.split('.')
        del spath[-1]
        spath.append(str(ind - 1))
        path_to_update = '.'.join(spath)
        udpate_where = get_where_templates(get_conditions_list(schema, path_to_update, id))
        update_statements[UPDATE_TMPLT.format(table=get_table_name_schema([database_name, schema_name, target_table]),
                                              statements='idx=' + str(ind - 1),
                                              conditions=udpate_where['target']['template'])] = udpate_where['target'][
            'values']

        for table in tables_list:
            update_statements[UPDATE_TMPLT.format(table=get_table_name_schema([database_name, schema_name, table]),
                                                  statements=target_table_idx_name + '_idx=' + str(ind - 1),
                                                  conditions=udpate_where['child']['template'])] = \
            udpate_where['child'][
                'values']
    return {'del': del_statements, 'upd': update_statements}

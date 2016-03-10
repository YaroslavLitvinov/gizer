__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"

from d_utils import *

DELETE_TMPL = 'DELETE FROM {table} WHERE {condition};'
UPDATE_TMPL = 'UPDATE {table} SET {statement} WHERE {condition};'


def op_delete_stmts(schema, path, id):
    return gen_statements(schema, path, id)


def get_max_id_in_array(path):
    # stub
    #
    # get max index of element in array corresponding for deleted record

    return '10'


def get_ids_list(lst):
    list = lst[0]
    ids_to_add = {}
    for it in list:
        if isIdField(it):
            if type(list[it]) is dict:
                for id_item in list[it]:
                    if isIdField(id_item):
                        ids_to_add[get_field_name_without_underscore(
                            it + '_' + get_field_name_without_underscore(id_item))] = get_postgres_type(
                            list[it][id_item])
            else:
                ids_to_add[get_field_name_without_underscore(it)] = get_postgres_type(list[it])
    if len(ids_to_add) == 0:
        ids_to_add['idx'] = 'bigint'
    return ids_to_add


def get_child_dict_item(dict_items, table):
    tables_list = []
    for it in dict_items:
        if type(dict_items[it]) is dict:
            tables_list = get_child_dict_item(dict_items[it], table[:-1] + '_' + it)
        elif type(dict_items[it]) is list:
            tables_list = get_tables_list(dict_items[it], table[:-1] + '_' + it)
    return tables_list


def get_tables_list(schema, table):
    tables_list = []
    tables_list.append(table)
    for item_list in schema:
        if type(item_list) is dict or type(item_list) is list:
            for it in item_list:
                item_value = item_list[it]
                if type(item_value) is dict:
                    tables_list.extend(get_child_dict_item(item_value, table[:-1] + '_' + it))
                elif type(item_value) is list:
                    tables_list.extend(get_tables_list(item_value, table[:-1] + '_' + it))
    return tables_list


def gen_where_clauses(schema, path, id):
    spath = path.split('.')
    parental_tables_list = get_indexes_dictionary(path)
    where_clause_target_table = []
    target_table = get_table_name_from_list(spath)
    for it in parental_tables_list:
        if target_table <> it:
            where_clause_target_table.append('(' + it + '_idx=' + parental_tables_list[it] + ')')
    ids = get_ids_list(schema)
    root_id = ids.iterkeys().next()
    root_table = get_root_table_from_path(path)

    where_clause_child_tables = where_clause_target_table[:]
    if root_table == target_table:
        where_clause_target_table.append('(' + root_id + "='" + id + "')")
        where_clause_child_tables.append('(' + target_table + '_' + root_id + "='" + id + "')")
    else:
        where_clause_target_table.append('(' + root_table + '_' + root_id + "='" + id + "')")
        where_clause_child_tables.append('(' + root_table + '_' + root_id + "='" + id + "')")
        where_clause_target_table.append('(idx=' + parental_tables_list[target_table] + ')')
        where_clause_child_tables.append('(' + target_table + '_idx=' + parental_tables_list[target_table] + ')')

    where_clauses = {}
    where_clauses['target'] = ' and '.join(where_clause_target_table)
    where_clauses['child'] = ' and '.join(where_clause_child_tables)
    return where_clauses


def gen_statements(schema, path, id):
    where_clauses = gen_where_clauses(schema, path, id)
    all_tables_list = get_tables_list(schema, get_root_table_from_path(path))
    target_table = get_table_name_from_list(path.split('.'))
    tables_list = []
    for table in all_tables_list:
        if str.startswith(str(table), target_table[:-1], 0, len(table)) and not table == target_table:
            tables_list.append(table)
    del_statements = []
    del_statements.append(
        DELETE_TMPL.format(table=target_table, condition=where_clauses['target']))

    for table in tables_list:
        del_statements.append(
            DELETE_TMPL.format(table=table, condition=where_clauses['child']))

    update_statements = []
    idx = get_last_idx_from_path(path)
    max_idx = get_max_id_in_array(path)
    if idx <= max_idx:
        return {'del': del_statements, 'upd': update_statements}

    for ind in range(int(idx) + 1, int(max_idx) + 1):
        spath = path.split('.')
        del spath[-1]
        spath.append(str(ind))
        path_to_update = '.'.join(spath)
        udpate_where = gen_where_clauses(schema, path_to_update, id)
        update_statements.append(
            UPDATE_TMPL.format(table=target_table, statement='idx=' + str(ind - 1), condition=udpate_where['target']))
        for table in tables_list:
            update_statements.append(UPDATE_TMPL.format(table=table, statement=target_table + '_idx=' + str(ind - 1),
                                                        condition=udpate_where['child']))
    return {'del': del_statements, 'upd': update_statements}

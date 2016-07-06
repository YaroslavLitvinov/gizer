__author__ = 'volodymyr'

from gizer.util import *
import json
import re


def test_get_field_name_without_underscore():
    field_name = '_test_'
    assert get_field_name_without_underscore(field_name) == 'test_'
    field_name = '__test_'
    assert get_field_name_without_underscore(field_name) == 'test_'
    field_name = '_ _test'
    assert get_field_name_without_underscore(field_name) == 'test'
    field_name = '_ _3_test'
    assert get_field_name_without_underscore(field_name) == 'test'
    print('TEST', 'get_field_name_without_underscore', 'PASSED')


def test_isIdField():
    field_names = ['id', 'oid', '_id', '_oid', '_id_oid', 'id_oid']
    for field_name in field_names:
        assert isIdField(field_name)
    field_names = ['___id', 'oid_aaaa', 'ID']
    for field_name in field_names:
        assert not isIdField(field_name)
    print('TEST', 'isIdField', 'PASSED')


def test_get_postgres_type():
    type_name = 'STRING'
    assert get_postgres_type(type_name) == 'text'
    type_name = 'INT'
    assert get_postgres_type(type_name) == 'integer'
    type_name = 'BOOLean'
    assert get_postgres_type(type_name) == 'boolean'
    type_name = 'LONG'
    assert get_postgres_type(type_name) == 'bigint'
    # TODO should be 'text'
    # type_name = 'string'
    # assert get_postgres_type(type_name) is None
    print('TEST', 'get_postgres_type', 'PASSED')


def test_get_table_name_from_list():
    # TODO should be person_relative_contact_phones
    path = 'persons.relatives.contacts.phones'
    assert get_table_name_from_list(path.split('.')) == 'person_relative_contact_phones'
    path = 'persons.relatives.2.contacts.3.phones.4'
    assert get_table_name_from_list(path.split('.')) == 'person_relative_contact_phones'
    path = 'persons.relatives.2contactd.phones'
    assert get_table_name_from_list(path.split('.')) == 'person_relative_2contactd_phones'
    path = 'persons.relatives2'
    assert get_table_name_from_list(path.split('.')) == 'person_relatives2'
    path = 'persons'
    assert get_table_name_from_list(path.split('.')) == 'persons'
    print('TEST', 'get_table_name_from_list', 'PASSED')


def test_get_root_table_from_path():
    path = 'persons.relatives.2.contacts.3.phones.4'
    # assert get_root_table_from_path(path) == 'persons_relatives_contacts_phones'
    assert get_root_table_from_path(path) == 'persons'
    path = 'persons.relatives.contacts.3.phones.4'
    assert get_root_table_from_path(path) == 'persons'
    path = 'persons.relatives.contacts.phones'
    assert get_root_table_from_path(path) == 'persons'
    path = 'persons.4.relatives.contacts.phones'
    assert get_root_table_from_path(path) == 'persons'
    print('TEST', 'get_root_table_from_path', 'PASSED')


def test_get_indexes_dictionary():
    path = 'persons.relatives.2.contacts.3.phones.4'
    model = {'person_relative_contact_phones': '5', 'person_relatives': '3', 'person_relative_contacts': '4'}
    f_return = get_indexes_dictionary(path)
    assert model == f_return

    path = 'persons.relatives.contacts.phones.4'
    model = {'person_relative_contact_phones': '5'}
    f_return = get_indexes_dictionary(path)
    assert model == f_return

    path = 'persons.1.relatives.2.contacts.3.phones.4'
    model = {'person_relative_contact_phones': '5', 'person_relatives': '3', 'person_relative_contacts': '4',
             'persons': '2'}
    f_return = get_indexes_dictionary(path)
    assert model == f_return

    path = 'persons.relatives.contacts.phones'
    model = {}
    f_return = get_indexes_dictionary(path)
    assert model == f_return

    path = 'persons.1.relatives.2.contacts.3.phones'
    model = {'person_relative_contacts': '4', 'persons': '2', 'person_relatives': '3'}
    f_return = get_indexes_dictionary(path)
    assert model == f_return

    path = 'persons.1.relatives.contacts.3.phones.6.numbers'
    model = {'person_relative_contacts': '4', 'persons': '2', 'person_relative_contact_phones': '7'}
    f_return = get_indexes_dictionary(path)
    assert model == f_return

    print('TEST', 'get_indexes_dictionary', 'PASSED')


def test_get_indexes_dictionary_idx():
    path = 'persons.relatives.2.contacts.3.phones.4'
    model = {'persons_relatives_contacts_phones': '5', 'persons_relatives': '3', 'persons_relatives_contacts': '4'}
    f_return = get_indexes_dictionary_idx(path)
    assert model == f_return

    path = 'persons.relatives.contacts.phones.4'
    model = {'persons_relatives_contacts_phones': '5'}
    f_return = get_indexes_dictionary_idx(path)
    assert model == f_return

    path = 'persons.1.relatives.2.contacts.3.phones.4'
    model = {'persons_relatives_contacts_phones': '5', 'persons_relatives': '3', 'persons_relatives_contacts': '4',
             'persons': '2'}
    f_return = get_indexes_dictionary_idx(path)
    assert model == f_return

    path = 'persons.relatives.contacts.phones'
    model = {}
    f_return = get_indexes_dictionary_idx(path)
    assert model == f_return

    path = 'persons.1.relatives.2.contacts.3.phones'
    model = {'persons_relatives_contacts': '4', 'persons': '2', 'persons_relatives': '3'}
    f_return = get_indexes_dictionary_idx(path)
    assert model == f_return

    path = 'persons.1.relatives.contacts.3.phones.6.numbers'
    model = {'persons_relatives_contacts': '4', 'persons': '2', 'persons_relatives_contacts_phones': '7'}
    f_return = get_indexes_dictionary_idx(path)
    assert model == f_return

    print('TEST', 'get_indexes_dictionary', 'PASSED')


def test_get_last_idx_from_path():
    path = 'persons.relatives.contacts.phones'
    assert get_last_idx_from_path(path) is None

    path = 'persons.1.relatives.2.contacts.3.phones.4'
    assert get_last_idx_from_path(path) == '5'

    path = 'persons.relatives.contacts.phones.234'
    assert get_last_idx_from_path(path) == '235'
    print('TEST', 'get_last_idx_from_path', 'PASSED')


def test_get_tables_structure():
    schema = json.loads(open('test_data/test_schema5.txt').read())
    collection_name = 'documents'
    result = get_tables_structure(schema, collection_name, {}, {}, 1, '')
    model = {
        u'documents': {
            u'personal_info_driver_licence': u'text',
            u'personal_info_fl_name_f_name': u'text',
            u'id_bsontype': u'integer',
            u'personal_info_date_of_birth': u'text',
            u'clients': u'text',
            u'personal_info_fl_name_l_name': u'text',
            u'id_oid': u'text'
        },
        u'document_relative_contacts': {
            u'city': u'text',
            u'apartment': u'text',
            u'street': u'text',
            u'idx': u'bigint',
            u'zip': u'text',
            u'document_relatives_idx': u'bigint',
            u'state': u'text',
            u'documents_id_oid': u'text',
            u'id': u'text'
        },
        u'document_relative_contact_phones': {
            u'count': u'integer',
            u'documents_id_oid': u'text',
            u'idx': u'bigint',
            u'document_relatives_idx': u'bigint',
            u'number': u'text',
            u'document_relative_contacts_idx': u'bigint'
        },
        u'document_dates': {
            u'date1': u'text',
            u'date3': u'text',
            u'documents_id_oid': u'text',
            u'date4': u'text',
            u'idx': u'bigint',
            u'date2': u'text'
        },
        u'document_relatives': {
            u'age': u'integer',
            u'documents_id_oid': u'text',
            u'relation': u'text',
            u'name': u'text',
            u'idx': u'bigint'
        },
        u'document_personal_info_fl_name_SSNs': {
            u'documents_id_oid': u'text',
            u'SSNs': u'integer',
            u'idx': u'bigint'
        },
        u'document_indeces': {
            u'documents_id_oid': u'text',
            u'indeces': u'integer',
            u'idx': u'bigint'
        },
        u'document_relative_jobs': {
            u'test1': u'integer',
            u'test2': u'text',
            u'documents_id_oid': u'text',
            u'idx': u'bigint',
            u'document_relatives_idx': u'bigint'
        }
    }
    assert model == result

    schema = json.loads(open('test_data/test_schema6.txt').read())
    result = get_tables_structure(schema, collection_name, {}, {}, 1, '')
    model = {'documents': {}}
    assert model == result

    schema = json.loads(open('test_data/test_schema4.txt').read())
    result = get_tables_structure(schema, collection_name, {}, {}, 1, '')
    model = {
        'documents': {
            'personal_info_driver_licence': 'text',
            'personal_info_date_of_birth': 'text',
            'id_bsontype': 'integer',
            'personal_info_full_name_last_name': 'text',
            'field': 'text',
            'id_oid': 'text',
            'personal_info_full_name_first_name': 'text'
        },
        'document_relative_contacts': {
            'city': 'text',
            'apartment': 'text',
            'street': 'text',
            'idx': 'bigint',
            'zip': 'text',
            'document_relatives_idx': 'bigint',
            'state': 'text',
            'documents_id_oid': 'text',
            'id': 'text'
        },
        'document_relative_contact_phones': {
            'count': 'integer',
            'documents_id_oid': 'text',
            'idx': 'bigint',
            'document_relatives_idx': 'bigint',
            'number': 'text',
            'document_relative_contacts_idx': 'bigint'
        },
        'document_dates': {
            'date1': 'text',
            'date3': 'text',
            'documents_id_oid': 'text',
            'date4': 'text',
            'idx': 'bigint',
            'date2': 'text'
        },
        'document_relatives': {
            'age': 'integer',
            'documents_id_oid': 'text',
            'relation': 'text',
            'name': 'text',
            'idx': 'bigint'
        },
        'document_indeces': {
            'documents_id_oid': 'text',
            'indeces': 'integer',
            'idx': 'bigint'
        },
        'document_personal_info_full_name_SSNs': {
            'documents_id_oid': 'text',
            'SSNs': 'integer',
            'idx': 'bigint'
        }
    }
    assert model == result

    schema = json.loads(open('test_data/test_schema.txt').read())
    result = get_tables_structure(schema, collection_name, {}, {}, 1, '')
    model = {
        'documents': {
            'field': 'text',
            'i2d_bsontype': 'integer',
            'i2d_oid': 'text'
        },
        'document_relative_contacts': {
            'city': 'text',
            'apartment': 'text',
            'idx': 'bigint',
            'zip': 'text',
            'document_relatives_idx': 'bigint',
            'state': 'text',
            'street': 'text',
            'documents_idx': 'bigint'
        },
        'document_relative_contact_phones': {
            'count': 'integer',
            'idx': 'bigint',
            'document_relatives_idx': 'bigint',
            'number': 'text',
            'document_relative_contacts_idx': 'bigint',
            'documents_idx': 'bigint'
        },
        'document_dates': {
            'date1': 'text',
            'date3': 'text',
            'date2': 'text',
            'date4': 'text',
            'idx': 'bigint',
            'documents_idx': 'bigint'
        },
        'document_relatives': {
            'age': 'integer',
            'documents_idx': 'bigint',
            'relation': 'text',
            'name': 'text',
            'idx': 'bigint'
        },
        'document_indeces': {
            'documents_idx': 'bigint',
            'indeces': 'integer',
            'idx': 'bigint'
        }
    }
    assert model == result
    print('TEST', 'get_tables_structure', 'PASSED')


def test_get_quotes_using():
    schema = json.loads(open('test_data/test_schema5.txt').read())
    collection_name = 'documents'

    table = 'documents'
    field_name = 'id_bsontype'
    model = False
    result = get_quotes_using(schema, table, field_name, collection_name)
    assert model == result

    table = 'documents'
    field_name = 'personal_info_fl_name_f_name'
    model = True
    result = get_quotes_using(schema, table, field_name, collection_name)
    assert model == result

    table = 'document_relative_contacts'
    field_name = 'zip'
    model = True
    result = get_quotes_using(schema, table, field_name, collection_name)
    assert model == result

    table = 'document_personal_info_fl_name_SSNs'
    field_name = 'documents_id_oid'
    model = True
    result = get_quotes_using(schema, table, field_name, collection_name)
    assert model == result

    table = 'document_personal_info_fl_name_SSNs'
    field_name = 'SSNs'
    model = False
    result = get_quotes_using(schema, table, field_name, collection_name)
    assert model == result
    print('TEST', 'get_quotes_using', 'PASSED')


def test_get_column_type():
    schema = json.loads(open('test_data/test_schema4.txt').read())
    collection_name = 'documents'

    table = 'document_relatives'
    field_name = 'relation'
    model = 'text'
    result = get_column_type(schema, table, field_name, collection_name)
    assert model == result

    table = 'document_personal_info_full_name_SSNs'
    field_name = 'SSNs'
    model = 'integer'
    result = get_column_type(schema, table, field_name, collection_name)
    assert model == result

    table = 'document_personal_info_full_name_SSNs'
    field_name = 'documents_id_oid'
    model = 'text'
    result = get_column_type(schema, table, field_name, collection_name)
    assert model == result

    table = 'document_relative_contact_phones'
    field_name = 'document_relative_contacts_idx'
    model = 'bigint'
    result = get_column_type(schema, table, field_name, collection_name)
    assert model == result

    table = 'document_relative_contact_phones'
    field_name = 'number'
    model = 'text'
    result = get_column_type(schema, table, field_name, collection_name)
    assert model == result
    print('TEST', 'get_column_type', 'PASSED')


# functions for comparing SQL queries
def sqls_to_dict(sql_dict):
    parsed_dict = {}
    # print(sql_dict)
    for model_item in sql_dict:
        if model_item == 'upd' or model_item == 'del':
            if model_item == 'upd':
                if type(sql_dict[model_item]) == dict:
                    for sql in sql_dict[model_item]:
                        r = re.compile('UPDATE(.*?)WHERE')
                        ext_key = str.strip(r.search(sql).group(1)).replace('"', '')
                        parsed_dict['UPD_' + ext_key] = parse_upd(sql, sql_dict[model_item][sql])
            if model_item == 'del':
                if type(sql_dict[model_item]) == dict:
                    for sql in sql_dict[model_item]:
                        r = re.compile('DELETE FROM(.*?)WHERE')
                        ext_key = str.strip(r.search(sql).group(1)).replace('"', '')
                        parsed_dict['DEL_' + ext_key] = parse_del({sql: sql_dict[model_item][sql]})
        else:
            q_item = model_item.iterkeys().next()
            q_vals = list(model_item.itervalues().next()[0])
            if q_item.startswith('DELETE FROM '):
                r = re.compile('DELETE FROM(.*?)WHERE')
                ext_key = str.strip(r.search(q_item).group(1)).replace('"', '')
                parsed_dict['DELETE_' + ext_key] = parse_del({q_item: model_item[q_item][0]})
            elif q_item.startswith('UPDATE '):
                # for sql in sql_dict[model_item]:
                # r = re.compile('UPDATE (.*?)WHERE')
                # ext_key = str.strip(r.search(q_item).group(1))
                parsed_dict.update(parse_upd(q_item, model_item[q_item][0]))
            elif q_item.startswith('do $$    begin'):
                upsert_dict = upsert_to_dict(q_item, q_vals)
                parsed_dict.update(upsert_dict)
            elif q_item.startswith('INSERT INTO '):
                insert_dict = parse_insert(q_item, q_vals)
                parsed_dict.update(insert_dict)
            else:
                pass
    return parsed_dict


def upsert_to_dict(tmplt, values):
    res = re.search('begin    (.*?)IF FOUND THEN', tmplt)
    if not res:
        return {'empty': None}
    upd_tmplt = str.strip(res.group(1))
    val_count = upd_tmplt.count('=(%s)')
    upd_dict = parse_upd(tmplt, values[:val_count])

    res = re.search('END IF;    BEGIN        (.*?)        RETURN;', tmplt)
    if not res:
        return {'empty': None}
    ins_tmplt = str.strip(res.group(1))
    ins_dict = parse_insert(ins_tmplt, values[val_count:])
    main_key = 'UPSERT_' + upd_dict.iterkeys().next() + '_' + ins_dict.iterkeys().next()
    ret_val = {main_key: {'upd': upd_dict, 'ins': ins_dict}}
    return ret_val


def parse_insert(sql_ins, values):
    sql = sql_ins
    updated_table = re.search('INSERT INTO(.*?)\(', sql).group(1).strip().replace('"', '')
    columns_strs = [ins_col.strip() for ins_col in re.search('\((.*?)\)', sql).group(1).split(',')]
    values_strs = [set_col.strip() for set_col in re.search('VALUES\((.*?)\);', sql).group(1).split(',')]
    assert len(columns_strs) == len(values_strs)
    insert_value_dict = {}
    insert_values_list = []
    for i, column in enumerate(columns_strs):
        el_val = (column + '=' + str(values[i])).replace('"', '').replace("'", '').replace(':', '').replace('(',
                                                                                                            '').replace(
            '(', '').replace(')', '')
        insert_values_list.append(el_val)
    ins_key = 'INSERT_' + '_'.join(sorted(insert_values_list))
    return {ins_key: {'table': updated_table, 'insert_value': insert_value_dict}}


def parse_upd(sql_upd, values):
    sql = sql_upd  # ql_upd.iterkeys().next()
    updated_table = re.search('UPDATE(.*?)SET', sql).group(1).strip().replace('"', '')
    set_strs = [set_col.strip() for set_col in re.search('SET(.*?)WHERE', sql).group(1).split(', ')]
    where_strs = [set_col.strip() for set_col in re.search('WHERE(.*?);', sql).group(1).split('and')]
    all_strs = set_strs + where_strs
    i = 0
    all_str_val = []
    for el in all_strs:
        if el.count('(%s)') > 0:
            el_val = el.replace('(%s)', str(values[i])).replace('"', '').replace("'", '').replace(':', '').replace('(',
                                                                                                                   '').replace(
                '(', '').replace(')', '')
            all_str_val.append(el_val)
            i = i + 1

    key = 'UPDATE_' + '_'.join(sorted(all_str_val))
    set_value = {}
    last_i = 0
    filled_values = 0
    for i, column in enumerate(set_strs):
        set_value[column.replace('"', '')] = values[i]
        if not column.endswith('=(%s)'):
            filled_values = filled_values + 1
        last_i = i + 1
    last_i = last_i - filled_values
    where_value = {}
    for i, column in enumerate(where_strs):
        where_value[column.replace('"', '')] = values[i + last_i]
    return {key: {'table': updated_table, 'set_value': set_value, 'where_dict': where_value}}


def parse_del(sql_upd):
    if not type(sql_upd) is dict:
        return {}
    stmnt = sql_upd.iterkeys().next()
    values = sql_upd.itervalues().next()
    clauses = stmnt.split(' ')
    updated_table = clauses[2].replace('"', '')
    where_clauses = [cl.replace(';', '') for cl in clauses[4:] if cl != 'and']
    where_dict = {}
    for i, cl in enumerate(where_clauses):
        where_dict[cl.replace('"', '')] = values[i]
    return {'table': 'DELETE_' + updated_table, 'where_dict': where_dict}


def upsert_pretty_print(upsert_stmnts):
    ups_SQL = upsert_stmnts.iterkeys().next()
    ups_vals = upsert_stmnts[ups_SQL][0]
    res = re.search('begin    (.*?)IF FOUND THEN', ups_SQL)
    if res:
        upd_tmplt = str.strip(res.group(1))
        val_count = upd_tmplt.count('=(%s)')
        print(upd_tmplt)
        print([str(el) for el in ups_vals[:val_count]])

        res = re.search('END IF;    BEGIN        (.*?)        RETURN;', ups_SQL)

        if res:
            ins_tmplt = str.strip(res.group(1))
            print(ins_tmplt)
            print ([str(el) for el in ups_vals[val_count:]])
    else:
        print(ups_SQL)
        print([str(el) for el in ups_vals])


def sql_pretty_print(sqls):
    print('\n\n')
    for el in sqls:
        if type(el) is dict:
            upsert_pretty_print(el)
        else:
            print(el)


def run_tests_():
    test_get_field_name_without_underscore()
    test_isIdField()
    test_get_postgres_type()
    test_get_table_name_from_list()
    test_get_root_table_from_path()
    test_get_indexes_dictionary()
    test_get_indexes_dictionary_idx()
    test_get_last_idx_from_path()
    test_get_tables_structure()
    test_get_quotes_using()
    test_get_column_type()


if __name__ == "__main__":
    run_tests_()

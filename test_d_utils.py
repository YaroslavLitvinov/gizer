__author__ = 'volodymyr'

from d_utils import *
import json


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
    type_name = 'BOOL'
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
    path = 'persons.relatives.2contacts.phones'
    assert get_table_name_from_list(path.split('.')) == 'person_relative_2contact_phones'
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
    assert get_root_table_from_path(path) == 'person_relatives'
    path = 'persons.relatives.contacts.phones'
    assert get_root_table_from_path(path) == 'person_relative_contact_phones'
    path = 'persons.4.relatives.contacts.phones'
    assert get_root_table_from_path(path) == 'persons'
    print('TEST', 'get_root_table_from_path', 'PASSED')


def test_get_indexes_dictionary():
    path = 'persons.relatives.2.contacts.3.phones.4'
    model = {'person_relative_contact_phones': '4', 'person_relatives': '2', 'person_relative_contacts': '3'}
    f_return = get_indexes_dictionary(path)
    assert model == f_return

    path = 'persons.relatives.contacts.phones.4'
    model = {'person_relative_contact_phones': '4'}
    f_return = get_indexes_dictionary(path)
    assert model == f_return

    path = 'persons.1.relatives.2.contacts.3.phones.4'
    model = {'person_relative_contact_phones': '4', 'person_relatives': '2', 'person_relative_contacts': '3',
             'persons': '1'}
    f_return = get_indexes_dictionary(path)
    assert model == f_return

    path = 'persons.relatives.contacts.phones'
    model = {}
    f_return = get_indexes_dictionary(path)
    assert model == f_return

    print('TEST', 'get_indexes_dictionary', 'PASSED')


def test_get_last_idx_from_path():
    path = 'persons.relatives.contacts.phones'
    assert get_last_idx_from_path(path) is None

    path = 'persons.1.relatives.2.contacts.3.phones.4'
    assert get_last_idx_from_path(path) == '4'

    path = 'persons.relatives.contacts.phones.234'
    assert get_last_idx_from_path(path) == '234'
    print('TEST', 'get_last_idx_from_path', 'PASSED')


def test_get_tables_structure():
    schema = json.loads(open('test_data/test_schema5.txt').read())
    collection_name = 'documents'
    result = get_tables_structure(schema, collection_name, {}, {}, 1)
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
            u'data': u'integer',
            u'idx': u'bigint'
        },
        u'document_indeces': {
            u'documents_id_oid': u'text',
            u'data': u'integer',
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
    result = get_tables_structure(schema, collection_name, {}, {}, 1)
    model = {'documents': {}}
    assert model == result

    schema = json.loads(open('test_data/test_schema4.txt').read())
    result = get_tables_structure(schema, collection_name, {}, {}, 1)
    model = {
        'documents' : {
            'personal_info_driver_licence' : 'text',
            'personal_info_date_of_birth' : 'text',
            'id_bsontype' : 'integer',
            'personal_info_full_name_last_name' : 'text',
            'field' : 'text',
            'id_oid' : 'text',
            'personal_info_full_name_first_name' : 'text'
        },
        'document_relative_contacts' : {
            'city' : 'text',
            'apartment' : 'text',
            'street' : 'text',
            'idx' : 'bigint',
            'zip' : 'text',
            'document_relatives_idx' : 'bigint',
            'state' : 'text',
            'documents_id_oid' : 'text',
            'id' : 'text'
        },
        'document_relative_contact_phones' : {
            'count' : 'integer',
            'documents_id_oid' : 'text',
            'idx' : 'bigint',
            'document_relatives_idx' : 'bigint',
            'number' : 'text',
            'document_relative_contacts_idx' : 'bigint'
        },
        'document_dates' : {
            'date1' : 'text',
            'date3' : 'text',
            'documents_id_oid' : 'text',
            'date4' : 'text',
            'idx' : 'bigint',
            'date2' : 'text'
        },
        'document_relatives' : {
            'age' : 'integer',
            'documents_id_oid' : 'text',
            'relation' : 'text',
            'name' : 'text',
            'idx' : 'bigint'
        },
        'document_indeces' : {
            'documents_id_oid' : 'text',
            'data' : 'integer',
            'idx' : 'bigint'
        },
        'document_personal_info_full_name_SSNs' : {
            'documents_id_oid' : 'text',
            'data' : 'integer',
            'idx' : 'bigint'
        }
    }
    assert model == result

    schema = json.loads(open('test_data/test_schema.txt').read())
    result = get_tables_structure(schema, collection_name, {}, {}, 1)
    model = {
        'documents' : {
            'field' : 'text',
            'i2d_bsontype' : 'integer',
            'i2d_oid' : 'text'
        },
        'document_relative_contacts' : {
            'city' : 'text',
            'apartment' : 'text',
            'idx' : 'bigint',
            'zip' : 'text',
            'document_relatives_idx' : 'bigint',
            'state' : 'text',
            'street' : 'text',
            'documents_idx' : 'bigint'
        },
        'document_relative_contact_phones' : {
            'count' : 'integer',
            'idx' : 'bigint',
            'document_relatives_idx' : 'bigint',
            'number' : 'text',
            'document_relative_contacts_idx' : 'bigint',
            'documents_idx' : 'bigint'
        },
        'document_dates' : {
            'date1' : 'text',
            'date3' : 'text',
            'date2' : 'text',
            'date4' : 'text',
            'idx' : 'bigint',
            'documents_idx' : 'bigint'
        },
        'document_relatives' : {
            'age' : 'integer',
            'documents_idx' : 'bigint',
            'relation' : 'text',
            'name' : 'text',
            'idx' : 'bigint'
        },
        'document_indeces' : {
            'documents_idx' : 'bigint',
            'data' : 'integer',
            'idx' : 'bigint'
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
    result = get_quotes_using(schema,table, field_name, collection_name)
    assert model == result

    table = 'documents'
    field_name = 'personal_info_fl_name_f_name'
    model = True
    result = get_quotes_using(schema,table, field_name, collection_name)
    assert model == result

    table = 'document_relative_contacts'
    field_name = 'zip'
    model = True
    result = get_quotes_using(schema,table, field_name, collection_name)
    assert model == result

    table = 'document_personal_info_fl_name_SSNs'
    field_name = 'documents_id_oid'
    model = True
    result = get_quotes_using(schema,table, field_name, collection_name)
    assert model == result

    table = 'document_personal_info_fl_name_SSNs'
    field_name = 'data'
    model = False
    result = get_quotes_using(schema,table, field_name, collection_name)
    assert model == result
    print('TEST', 'get_quotes_using', 'PASSED')


def run_tests_():
    test_get_field_name_without_underscore()
    test_isIdField()
    test_get_postgres_type()
    test_get_table_name_from_list()
    test_get_root_table_from_path()
    test_get_indexes_dictionary()
    test_get_last_idx_from_path()
    test_get_tables_structure()
    test_get_quotes_using()


if __name__ == "__main__":
    run_tests_()

# data = open('test_data/test_schema4.txt').read()
# schema = json.loads(data)
# table_name = 'main'
#
# pp = pprint.PrettyPrinter(indent=4)
#
# path = 'main.relatives.4.contacts.3.phones.2'
# id = 'aabbccddeeff'
#
# pp.pprint(get_tables_list(schema, 'main'))
#
# # print(get_parental_tables_from_path(path))
# print (gen_where_clauses(schema, path, id))
#
# # print(get_table_name_from_link(path))
#
# print(get_indexes_dictionary(path))
#
# pp.pprint(gen_statements(schema, path, id))
# path = 'main.relatives.4'
# pp.pprint(gen_statements(schema, path, id))
# path = 'main.relatives.4.contacts.4'
# pp.pprint(gen_statements(schema, path, id))

# pp.pprint(get_element(schema, path))

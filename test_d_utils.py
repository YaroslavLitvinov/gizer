__author__ = 'volodymyr'

from d_utils import *


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
    assert get_table_name_from_list(path.split('.')) == 'persons_relatives_contacts_phones'
    path = 'persons.relatives.2.contacts.3.phones.4'
    assert get_table_name_from_list(path.split('.')) == 'persons_relatives_contacts_phones'
    path = 'persons.relatives.2contacts.phones'
    assert get_table_name_from_list(path.split('.')) == 'persons_relatives_2contacts_phones'
    path = 'persons.relatives2'
    assert get_table_name_from_list(path.split('.')) == 'persons_relatives2'
    path = 'persons'
    assert get_table_name_from_list(path.split('.')) == 'persons'
    print('TEST', 'get_table_name_from_list', 'PASSED')


def test_get_root_table_from_path():
    path = 'persons.relatives.2.contacts.3.phones.4'
    # assert get_root_table_from_path(path) == 'persons_relatives_contacts_phones'
    assert get_root_table_from_path(path) == 'persons'
    path = 'persons.relatives.contacts.3.phones.4'
    assert get_root_table_from_path(path) == 'persons_relatives'
    path = 'persons.relatives.contacts.phones'
    assert get_root_table_from_path(path) == 'persons_relatives_contacts_phones'
    path = 'persons.4.relatives.contacts.phones'
    assert get_root_table_from_path(path) == 'persons'
    print('TEST', 'get_root_table_from_path', 'PASSED')


def test_get_indexes_dictionary():
    path = 'persons.relatives.2.contacts.3.phones.4'
    model = {'persons_relatives_contacts_phones': '4', 'persons_relatives': '2', 'persons_relatives_contacts': '3'}
    f_return = get_indexes_dictionary(path)
    assert check_dict(model, f_return)

    path = 'persons.relatives.contacts.phones.4'
    model = {'persons_relatives_contacts_phones': '4'}
    f_return = get_indexes_dictionary(path)
    assert check_dict(model, f_return)

    path = 'persons.1.relatives.2.contacts.3.phones.4'
    model = {'persons_relatives_contacts_phones': '4', 'persons_relatives': '2', 'persons_relatives_contacts': '3',
             'persons': '1'}
    f_return = get_indexes_dictionary(path)
    assert check_dict(model, f_return)

    path = 'persons.relatives.contacts.phones'
    model = {}
    f_return = get_indexes_dictionary(path)
    assert check_dict(model, f_return)

    print('TEST', 'get_indexes_dictionary', 'PASSED')


def test_get_last_idx_from_path():
    path = 'persons.relatives.contacts.phones'
    assert get_last_idx_from_path(path) is None

    path = 'persons.1.relatives.2.contacts.3.phones.4'
    assert get_last_idx_from_path(path) == '4'

    path = 'persons.relatives.contacts.phones.234'
    assert get_last_idx_from_path(path) == '234'
    print('TEST', 'get_last_idx_from_path', 'PASSED')


def check_dict(list1, list2):
    for it in list1:
        if list1[it] <> list2[it]:
            return False
    return True


def run_tests():
    test_get_field_name_without_underscore()
    test_isIdField()
    test_get_postgres_type()
    test_get_table_name_from_list()
    test_get_root_table_from_path()
    test_get_indexes_dictionary()
    test_get_last_idx_from_path()


if __name__ == "__main__":
    print()
    run_tests()

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

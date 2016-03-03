__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"

from opdelete import *
import json
import pprint

TEST_INFO = 'TEST_OPDELETE'


def test_get_ids_list():
    schema = json.loads(open('test_data/test_schema.txt').read())
    model = {'idx': 'bigint'}
    assert check_dict(get_ids_list(schema), model)

    schema = json.loads(open('test_data/test_schema2.txt').read())
    model = {'id': 'text'}
    assert check_dict(get_ids_list(schema), model)

    schema = json.loads(open('test_data/test_schema3.txt').read())
    model = {'id_oid': 'text'}
    assert check_dict(get_ids_list(schema), model)

    print(TEST_INFO, 'get_ids_list', 'PASSED')


def test_get_child_dict_item():
    print(TEST_INFO, 'get_child_dict_item', 'Not finished yet')
    # print(TEST_INFO, 'get_child_dict_item', 'PASSED')


def test_get_tables_list():
    schema = json.loads(open('test_data/test_schema5.txt').read())
    result = get_tables_list(schema, 'mains')
    model = ['mains', 'mains_personal_info_fl_name_SSNs', 'mains_relatives', 'mains_relatives_contacts',
             'mains_relatives_contacts_phones', 'mains_relatives_jobs', 'mains_indeces', 'mains_dates']
    assert sorted(model) == sorted(result)

    schema = json.loads(open('test_data/test_schema4.txt').read())
    result = get_tables_list(schema, 'table1')
    model = ['table1_personal_info_full_name_SSNs', 'table1_relatives_contacts_phones', 'table1_relatives_contacts',
             'table1_relatives', 'table1', 'table1_indeces', 'table1_dates']
    assert sorted(model) == sorted(result)

    schema = json.loads(open('test_data/test_schema3.txt').read())
    result = get_tables_list(schema, 'table1')
    model = ['table1', 'table1_relatives_contacts_phones', 'table1_relatives_contacts', 'table1_relatives',
             'table1_indeces', 'table1_dates']
    assert sorted(model) == sorted(result)

    schema = json.loads(open('test_data/test_schema6.txt').read())
    result = get_tables_list(schema, 'table1')
    model = ['table1']
    assert sorted(model) == sorted(result)

    print(TEST_INFO, 'get_tables_list', 'PASSED')


def check_dict(list1, list2):
    for it in list1:
        if list1[it] <> list2[it]:
            return False
    return True


def run_tests():
    test_get_ids_list()
    test_get_child_dict_item()
    test_get_tables_list()


run_tests()

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


def test_gen_where_clauses():
    schema = json.loads(open('test_data/test_schema.txt').read())
    path = 'persons.relatives.contacts.phones'
    id = '0123456789abcdef'
    model = {'target':"(idx='0123456789abcdef')", 'child':"(persons_relatives_contacts_phones_idx='0123456789abcdef')"}
    result = gen_where_clauses(schema, path, id)
    assert check_dict(model, result)

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.3.phones.4'
    id = '0123456789abcdef'
    model = {
        'target': "(persons_relatives_idx=2) and (persons_relatives_contacts_idx=3) and (persons_id_oid='0123456789abcdef') and (idx=4)",
        'child': "(persons_relatives_idx=2) and (persons_relatives_contacts_idx=3) and (persons_id_oid='0123456789abcdef') and (persons_relatives_contacts_phones_idx=4)"
    }
    result = gen_where_clauses(schema, path, id)
    assert check_dict(model, result)

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.5'
    id = '0123456789abcdef'
    model = {
        'target': "(persons_relatives_idx=2) and (persons_id_oid='0123456789abcdef') and (idx=5)",
        'child': "(persons_relatives_idx=2) and (persons_id_oid='0123456789abcdef') and (persons_relatives_contacts_idx=5)"
    }

    result = gen_where_clauses(schema, path, id)
    assert check_dict(model, result)

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons'
    id = '0123456789abcdef'
    model = {
        'target': "(id_oid='0123456789abcdef')",
        'child': "(persons_id_oid='0123456789abcdef')"
    }

    result = gen_where_clauses(schema, path, id)
    assert check_dict(model, result)
    print(TEST_INFO, 'gen_where_clauses', 'PASSED')


def test_gen_statements():
    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons'
    id = '0123456789ABCDEF'
    result = gen_statements(schema, path, id)
    model = {
        'upd': [],
        'del': [
            "DELETE FROM persons WHERE (id_oid='0123456789ABCDEF');",
            "DELETE FROM persons_dates WHERE (persons_id_oid='0123456789ABCDEF');",
            "DELETE FROM persons_relatives WHERE (persons_id_oid='0123456789ABCDEF');",
            "DELETE FROM persons_relatives_jobs WHERE (persons_id_oid='0123456789ABCDEF');",
            "DELETE FROM persons_relatives_contacts WHERE (persons_id_oid='0123456789ABCDEF');",
            "DELETE FROM persons_relatives_contacts_phones WHERE (persons_id_oid='0123456789ABCDEF');",
            "DELETE FROM persons_personal_info_fl_name_SSNs WHERE (persons_id_oid='0123456789ABCDEF');",
            "DELETE FROM persons_indeces WHERE (persons_id_oid='0123456789ABCDEF');"
        ]
    }
    assert check_dict(model, result)

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.5'
    id = '0123456789ABCDEF'
    result = gen_statements(schema, path, id)
    model = {
        'upd': [
            "UPDATE persons_relatives_contacts SET idx=5 WHERE (persons_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (idx=6);",
            "UPDATE persons_relatives_contacts_phones SET persons_relatives_contacts_idx=5 WHERE (persons_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (persons_relatives_contacts_idx=6);",
            "UPDATE persons_relatives_contacts SET idx=6 WHERE (persons_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (idx=7);",
            "UPDATE persons_relatives_contacts_phones SET persons_relatives_contacts_idx=6 WHERE (persons_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (persons_relatives_contacts_idx=7);",
            "UPDATE persons_relatives_contacts SET idx=7 WHERE (persons_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (idx=8);",
            "UPDATE persons_relatives_contacts_phones SET persons_relatives_contacts_idx=7 WHERE (persons_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (persons_relatives_contacts_idx=8);",
            "UPDATE persons_relatives_contacts SET idx=8 WHERE (persons_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (idx=9);",
            "UPDATE persons_relatives_contacts_phones SET persons_relatives_contacts_idx=8 WHERE (persons_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (persons_relatives_contacts_idx=9);",
            "UPDATE persons_relatives_contacts SET idx=9 WHERE (persons_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (idx=10);",
            "UPDATE persons_relatives_contacts_phones SET persons_relatives_contacts_idx=9 WHERE (persons_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (persons_relatives_contacts_idx=10);"
        ],
        'del': [
            "DELETE FROM persons_relatives_contacts WHERE (persons_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (idx=5);",
            "DELETE FROM persons_relatives_contacts_phones WHERE (persons_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (persons_relatives_contacts_idx=5);"
        ]
    }
    assert check_dict(model, result)

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2'
    id = '0123456789ABCDEF'
    result = gen_statements(schema, path, id)
    model = {
        'upd': [
            "UPDATE persons_relatives SET idx=2 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=3);",
            "UPDATE persons_relatives_jobs SET persons_relatives_idx=2 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=3);",
            "UPDATE persons_relatives_contacts SET persons_relatives_idx=2 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=3);",
            "UPDATE persons_relatives_contacts_phones SET persons_relatives_idx=2 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=3);",
            "UPDATE persons_relatives SET idx=3 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=4);",
            "UPDATE persons_relatives_jobs SET persons_relatives_idx=3 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=4);",
            "UPDATE persons_relatives_contacts SET persons_relatives_idx=3 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=4);",
            "UPDATE persons_relatives_contacts_phones SET persons_relatives_idx=3 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=4);",
            "UPDATE persons_relatives SET idx=4 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=5);",
            "UPDATE persons_relatives_jobs SET persons_relatives_idx=4 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=5);",
            "UPDATE persons_relatives_contacts SET persons_relatives_idx=4 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=5);",
            "UPDATE persons_relatives_contacts_phones SET persons_relatives_idx=4 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=5);",
            "UPDATE persons_relatives SET idx=5 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=6);",
            "UPDATE persons_relatives_jobs SET persons_relatives_idx=5 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=6);",
            "UPDATE persons_relatives_contacts SET persons_relatives_idx=5 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=6);",
            "UPDATE persons_relatives_contacts_phones SET persons_relatives_idx=5 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=6);",
            "UPDATE persons_relatives SET idx=6 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=7);",
            "UPDATE persons_relatives_jobs SET persons_relatives_idx=6 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=7);",
            "UPDATE persons_relatives_contacts SET persons_relatives_idx=6 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=7);",
            "UPDATE persons_relatives_contacts_phones SET persons_relatives_idx=6 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=7);",
            "UPDATE persons_relatives SET idx=7 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=8);",
            "UPDATE persons_relatives_jobs SET persons_relatives_idx=7 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=8);",
            "UPDATE persons_relatives_contacts SET persons_relatives_idx=7 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=8);",
            "UPDATE persons_relatives_contacts_phones SET persons_relatives_idx=7 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=8);",
            "UPDATE persons_relatives SET idx=8 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=9);",
            "UPDATE persons_relatives_jobs SET persons_relatives_idx=8 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=9);",
            "UPDATE persons_relatives_contacts SET persons_relatives_idx=8 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=9);",
            "UPDATE persons_relatives_contacts_phones SET persons_relatives_idx=8 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=9);",
            "UPDATE persons_relatives SET idx=9 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=10);",
            "UPDATE persons_relatives_jobs SET persons_relatives_idx=9 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=10);",
            "UPDATE persons_relatives_contacts SET persons_relatives_idx=9 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=10);",
            "UPDATE persons_relatives_contacts_phones SET persons_relatives_idx=9 WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=10);"
        ],
        'del': [
            "DELETE FROM persons_relatives WHERE (persons_id_oid='0123456789ABCDEF') and (idx=2);",
            "DELETE FROM persons_relatives_jobs WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=2);",
            "DELETE FROM persons_relatives_contacts WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=2);",
            "DELETE FROM persons_relatives_contacts_phones WHERE (persons_id_oid='0123456789ABCDEF') and (persons_relatives_idx=2);"
        ]
    }
    assert check_dict(model, result)
    print(TEST_INFO, 'gen_statements', 'PASSED')



def check_dict(list1, list2):
    if len(list2) <> len(list2):
        return False
    for it in list1:
        if list1[it] <> list2[it]:
            return False
    return True


def run_tests():
    test_get_ids_list()
    test_get_child_dict_item()
    test_get_tables_list()
    test_gen_where_clauses()
    test_gen_statements()


run_tests()

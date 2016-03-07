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
    model = ['mains', 'main_personal_inf_fl_nam_SSNs', 'main_relatives', 'main_relative_contacts',
             'main_relative_contact_phones', 'main_relative_jobs', 'main_indeces', 'main_dates']
    assert sorted(model) == sorted(result)

    schema = json.loads(open('test_data/test_schema4.txt').read())
    result = get_tables_list(schema, 'table1')
    model = ['table_personal_inf_full_nam_SSNs', 'table_relative_contact_phones', 'table_relative_contacts',
             'table_relatives', 'table1', 'table_indeces', 'table_dates']
    assert sorted(model) == sorted(result)

    schema = json.loads(open('test_data/test_schema3.txt').read())
    result = get_tables_list(schema, 'table1')
    model = ['table1', 'table_relative_contact_phones', 'table_relative_contacts', 'table_relatives',
             'table_indeces', 'table_dates']
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
    model = {'target':"(idx='0123456789abcdef')", 'child':"(person_relative_contact_phones_idx='0123456789abcdef')"}
    result = gen_where_clauses(schema, path, id)
    assert check_dict(model, result)

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.3.phones.4'
    id = '0123456789abcdef'
    model = {
        'target': "(person_relatives_idx=2) and (person_relative_contacts_idx=3) and (persons_id_oid='0123456789abcdef') and (idx=4)",
        'child': "(person_relatives_idx=2) and (person_relative_contacts_idx=3) and (persons_id_oid='0123456789abcdef') and (person_relative_contact_phones_idx=4)"
    }
    result = gen_where_clauses(schema, path, id)
    assert check_dict(model, result)

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.5'
    id = '0123456789abcdef'
    model = {
        'target': "(person_relatives_idx=2) and (persons_id_oid='0123456789abcdef') and (idx=5)",
        'child': "(person_relatives_idx=2) and (persons_id_oid='0123456789abcdef') and (person_relative_contacts_idx=5)"
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
            "DELETE FROM person_dates WHERE (persons_id_oid='0123456789ABCDEF');",
            "DELETE FROM person_relatives WHERE (persons_id_oid='0123456789ABCDEF');",
            "DELETE FROM person_relative_jobs WHERE (persons_id_oid='0123456789ABCDEF');",
            "DELETE FROM person_relative_contacts WHERE (persons_id_oid='0123456789ABCDEF');",
            "DELETE FROM person_relative_contact_phones WHERE (persons_id_oid='0123456789ABCDEF');",
            "DELETE FROM person_personal_inf_fl_nam_SSNs WHERE (persons_id_oid='0123456789ABCDEF');",
            "DELETE FROM person_indeces WHERE (persons_id_oid='0123456789ABCDEF');"
        ]
    }
    assert check_dict(model, result)


    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.5'
    id = '0123456789ABCDEF'
    result = gen_statements(schema, path, id)
    model = {
        'upd': [
            "UPDATE person_relative_contacts SET idx=5 WHERE (person_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (idx=6);",
            "UPDATE person_relative_contact_phones SET person_relative_contacts_idx=5 WHERE (person_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (person_relative_contacts_idx=6);",
            "UPDATE person_relative_contacts SET idx=6 WHERE (person_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (idx=7);",
            "UPDATE person_relative_contact_phones SET person_relative_contacts_idx=6 WHERE (person_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (person_relative_contacts_idx=7);",
            "UPDATE person_relative_contacts SET idx=7 WHERE (person_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (idx=8);",
            "UPDATE person_relative_contact_phones SET person_relative_contacts_idx=7 WHERE (person_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (person_relative_contacts_idx=8);",
            "UPDATE person_relative_contacts SET idx=8 WHERE (person_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (idx=9);",
            "UPDATE person_relative_contact_phones SET person_relative_contacts_idx=8 WHERE (person_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (person_relative_contacts_idx=9);",
            "UPDATE person_relative_contacts SET idx=9 WHERE (person_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (idx=10);",
            "UPDATE person_relative_contact_phones SET person_relative_contacts_idx=9 WHERE (person_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (person_relative_contacts_idx=10);"
        ],
        'del': [
            "DELETE FROM person_relative_contacts WHERE (person_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (idx=5);",
            "DELETE FROM person_relative_contact_phones WHERE (person_relatives_idx=2) and (persons_id_oid='0123456789ABCDEF') and (person_relative_contacts_idx=5);"
        ]
    }
    assert check_dict(model, result)

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2'
    id = '0123456789ABCDEF'
    result = gen_statements(schema, path, id)
    model = {
        'upd': [
            "UPDATE person_relatives SET idx=2 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=3);",
            "UPDATE person_relative_jobs SET person_relatives_idx=2 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=3);",
            "UPDATE person_relative_contacts SET person_relatives_idx=2 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=3);",
            "UPDATE person_relative_contact_phones SET person_relatives_idx=2 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=3);",
            "UPDATE person_relatives SET idx=3 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=4);",
            "UPDATE person_relative_jobs SET person_relatives_idx=3 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=4);",
            "UPDATE person_relative_contacts SET person_relatives_idx=3 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=4);",
            "UPDATE person_relative_contact_phones SET person_relatives_idx=3 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=4);",
            "UPDATE person_relatives SET idx=4 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=5);",
            "UPDATE person_relative_jobs SET person_relatives_idx=4 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=5);",
            "UPDATE person_relative_contacts SET person_relatives_idx=4 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=5);",
            "UPDATE person_relative_contact_phones SET person_relatives_idx=4 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=5);",
            "UPDATE person_relatives SET idx=5 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=6);",
            "UPDATE person_relative_jobs SET person_relatives_idx=5 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=6);",
            "UPDATE person_relative_contacts SET person_relatives_idx=5 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=6);",
            "UPDATE person_relative_contact_phones SET person_relatives_idx=5 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=6);",
            "UPDATE person_relatives SET idx=6 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=7);",
            "UPDATE person_relative_jobs SET person_relatives_idx=6 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=7);",
            "UPDATE person_relative_contacts SET person_relatives_idx=6 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=7);",
            "UPDATE person_relative_contact_phones SET person_relatives_idx=6 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=7);",
            "UPDATE person_relatives SET idx=7 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=8);",
            "UPDATE person_relative_jobs SET person_relatives_idx=7 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=8);",
            "UPDATE person_relative_contacts SET person_relatives_idx=7 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=8);",
            "UPDATE person_relative_contact_phones SET person_relatives_idx=7 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=8);",
            "UPDATE person_relatives SET idx=8 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=9);",
            "UPDATE person_relative_jobs SET person_relatives_idx=8 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=9);",
            "UPDATE person_relative_contacts SET person_relatives_idx=8 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=9);",
            "UPDATE person_relative_contact_phones SET person_relatives_idx=8 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=9);",
            "UPDATE person_relatives SET idx=9 WHERE (persons_id_oid='0123456789ABCDEF') and (idx=10);",
            "UPDATE person_relative_jobs SET person_relatives_idx=9 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=10);",
            "UPDATE person_relative_contacts SET person_relatives_idx=9 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=10);",
            "UPDATE person_relative_contact_phones SET person_relatives_idx=9 WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=10);"
        ],
        'del': [
            "DELETE FROM person_relatives WHERE (persons_id_oid='0123456789ABCDEF') and (idx=2);",
            "DELETE FROM person_relative_jobs WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=2);",
            "DELETE FROM person_relative_contacts WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=2);",
            "DELETE FROM person_relative_contact_phones WHERE (persons_id_oid='0123456789ABCDEF') and (person_relatives_idx=2);"
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

__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"

from opdelete import *
import json
import pprint

TEST_INFO = 'TEST_OPDELETE'


def test_get_ids_list():
    schema = json.loads(open('test_data/test_schema.txt').read())
    model = {'idx': 'bigint'}
    assert get_ids_list(schema) == model

    schema = json.loads(open('test_data/test_schema2.txt').read())
    model = {'id': 'text'}
    assert get_ids_list(schema) == model

    schema = json.loads(open('test_data/test_schema3.txt').read())
    model = {'id_oid': 'text'}
    assert get_ids_list(schema) == model

    schema = json.loads(open('test_data/test_schema5.txt').read())
    # print('list', get_ids_list(schema))
    # model = {'id_oid': 'text'}
    # assert check_dict(get_ids_list(schema), model)
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


def test_get_conditions_list():
    schema = json.loads(open('test_data/test_schema.txt').read())
    path = 'persons.relatives.contacts.phones'
    id = '0123456789abcdef'
    model = {'target': {'idx': '0123456789abcdef'},
             'child': {'persons_relatives_contacts_phones_idx': '0123456789abcdef'}}
    result = get_conditions_list(schema, path, id)
    assert model == result

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.3.phones.4'
    id = '0123456789abcdef'
    model = {
        'target': {'persons_relatives_idx': '2', 'persons_relatives_contacts_idx': '3',
                   'persons_id_oid': '0123456789abcdef', 'idx': '4'},
        'child': {'persons_relatives_idx': '2', 'persons_relatives_contacts_idx': '3',
                  'persons_id_oid': '0123456789abcdef', 'persons_relatives_contacts_phones_idx': '4'}
    }
    result = get_conditions_list(schema, path, id)
    assert model == result

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.3.phones'
    id = '0123456789abcdef'
    model = {
        'target': {'persons_relatives_idx': '2', 'persons_relatives_contacts_idx': '3',
                   'persons_id_oid': '0123456789abcdef'},
        'child': {'persons_relatives_idx': '2', 'persons_relatives_contacts_idx': '3',
                  'persons_id_oid': '0123456789abcdef'}
    }
    result = get_conditions_list(schema, path, id)
    assert model == result

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.5'
    id = '0123456789abcdef'
    model = {
        'target': {'persons_relatives_idx': '2', 'persons_id_oid': '0123456789abcdef', 'idx': '5'},
        'child': {'persons_relatives_idx': '2', 'persons_id_oid': '0123456789abcdef',
                  'persons_relatives_contacts_idx': '5'}
    }
    result = get_conditions_list(schema, path, id)
    assert model == result

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons'
    id = '0123456789abcdef'
    model = {
        'target': {'id_oid': '0123456789abcdef'},
        'child': {'persons_id_oid': '0123456789abcdef'}
    }
    result = get_conditions_list(schema, path, id)
    assert model == result

    print(TEST_INFO, 'get_conditions_list', 'PASSED')


def test_get_where_templates():
    conditions_list = {'target': {'idx': '0123456789abcdef'},
                       'child': {'persons_relatives_contacts_phones_idx': '0123456789abcdef'}}
    model = {'target': {'values': ['0123456789abcdef'], 'template': '(idx=%s)'},
             'child': {'values': ['0123456789abcdef'], 'template': '(persons_relatives_contacts_phones_idx=%s)'}}
    result = get_where_templates(conditions_list)
    assert model == result

    conditions_list = {
        'target': {'persons_relatives_idx': '2', 'persons_relatives_contacts_idx': '3',
                   'persons_id_oid': '0123456789abcdef', 'idx': '4'},
        'child': {'persons_relatives_idx': '2', 'persons_relatives_contacts_idx': '3',
                  'persons_id_oid': '0123456789abcdef', 'persons_relatives_contacts_phones_idx': '4'}
    }

    model = {'target': {'values': ['4', '0123456789abcdef', '3', '2'],
                        'template': '(idx=%s) and (persons_id_oid="%s") and (persons_relatives_contacts_idx=%s) and (persons_relatives_idx=%s)'},
             'child': {'values': ['0123456789abcdef', '3', '4', '2'],
                       'template': '(persons_id_oid="%s") and (persons_relatives_contacts_idx=%s) and (persons_relatives_contacts_phones_idx=%s) and (persons_relatives_idx=%s)'}}
    result = get_where_templates(conditions_list)
    assert model == result

    conditions_list = {
        'target': {'persons_relatives_idx': '2', 'persons_id_oid': '0123456789abcdef', 'idx': '5'},
        'child': {'persons_relatives_idx': '2', 'persons_id_oid': '0123456789abcdef',
                  'persons_relatives_contacts_idx': '5'}
    }
    model = {'target': {'values': ['5', '0123456789abcdef', '2'],
                        'template': '(idx=%s) and (persons_id_oid="%s") and (persons_relatives_idx=%s)'},
             'child': {'values': ['0123456789abcdef', '5', '2'],
                       'template': '(persons_id_oid="%s") and (persons_relatives_contacts_idx=%s) and (persons_relatives_idx=%s)'}}
    result = get_where_templates(conditions_list)
    assert model == result

    conditions_list = {
        'target': {'id_oid': '0123456789abcdef'},
        'child': {'persons_id_oid': '0123456789abcdef'}
    }
    model = {
        'target': {
            'values': ['0123456789abcdef'],
            'template': '(id_oid="%s")'
        },
        'child': {
            'values': ['0123456789abcdef'],
            'template': '(persons_id_oid="%s")'
        }
    }
    result = get_where_templates(conditions_list)
    assert model == result

    print(TEST_INFO, 'get_where_templates', 'PASSED')


def test_gen_statements():
    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons'
    id = '0123456789ABCDEF'
    result = gen_statements(schema, path, id)
    model = {
        'upd': {},
        'del': {
            'DELETE FROM persons WHERE (id_oid="%s");': ['0123456789ABCDEF'],
            'DELETE FROM person_dates WHERE (persons_id_oid="%s");': ['0123456789ABCDEF'],
            'DELETE FROM person_relatives WHERE (persons_id_oid="%s");': ['0123456789ABCDEF'],
            'DELETE FROM person_relative_jobs WHERE (persons_id_oid="%s");': ['0123456789ABCDEF'],
            'DELETE FROM person_relative_contacts WHERE (persons_id_oid="%s");': ['0123456789ABCDEF'],
            'DELETE FROM person_relative_contact_phones WHERE (persons_id_oid="%s");': ['0123456789ABCDEF'],
            'DELETE FROM person_personal_inf_fl_nam_SSNs WHERE (persons_id_oid="%s");': ['0123456789ABCDEF'],
            'DELETE FROM person_indeces WHERE (persons_id_oid="%s");': ['0123456789ABCDEF']
        }
    }
    assert model == result

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.5'
    id = '0123456789ABCDEF'
    result = gen_statements(schema, path, id)
    model = {
        'upd': {
            'UPDATE person_relative_contact_phones SET persons_relatives_contacts_idx=6 WHERE (persons_id_oid="%s") and (persons_relatives_contacts_idx=%s) and (persons_relatives_idx=%s);': [
                '0123456789ABCDEF', '7', '2'],
            'UPDATE person_relative_contact_phones SET persons_relatives_contacts_idx=8 WHERE (persons_id_oid="%s") and (persons_relatives_contacts_idx=%s) and (persons_relatives_idx=%s);': [
                '0123456789ABCDEF', '9', '2'],
            'UPDATE person_relative_contact_phones SET persons_relatives_contacts_idx=5 WHERE (persons_id_oid="%s") and (persons_relatives_contacts_idx=%s) and (persons_relatives_idx=%s);': [
                '0123456789ABCDEF', '6', '2'],
            'UPDATE person_relative_contact_phones SET persons_relatives_contacts_idx=7 WHERE (persons_id_oid="%s") and (persons_relatives_contacts_idx=%s) and (persons_relatives_idx=%s);': [
                '0123456789ABCDEF', '8', '2'],
            'UPDATE person_relative_contacts SET idx=5 WHERE (idx=%s) and (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
                '6', '0123456789ABCDEF', '2'],
            'UPDATE person_relative_contacts SET idx=8 WHERE (idx=%s) and (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
                '9', '0123456789ABCDEF', '2'],
            'UPDATE person_relative_contacts SET idx=7 WHERE (idx=%s) and (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
                '8', '0123456789ABCDEF', '2'],
            'UPDATE person_relative_contacts SET idx=9 WHERE (idx=%s) and (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
                '10', '0123456789ABCDEF', '2'],
            'UPDATE person_relative_contacts SET idx=6 WHERE (idx=%s) and (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
                '7', '0123456789ABCDEF', '2'],
            'UPDATE person_relative_contact_phones SET persons_relatives_contacts_idx=9 WHERE (persons_id_oid="%s") and (persons_relatives_contacts_idx=%s) and (persons_relatives_idx=%s);': [
                '0123456789ABCDEF', '10', '2']},
        'del': {
            'DELETE FROM person_relative_contacts WHERE (idx=%s) and (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
                '5', '0123456789ABCDEF', '2'],
            'DELETE FROM person_relative_contact_phones WHERE (persons_id_oid="%s") and (persons_relatives_contacts_idx=%s) and (persons_relatives_idx=%s);': [
                '0123456789ABCDEF', '5', '2']}}
    assert model == result

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2'
    id = '0123456789ABCDEF'
    result = gen_statements(schema, path, id)
    model = {'upd': {
        'UPDATE person_relative_jobs SET persons_relatives_idx=6 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '7'],
        'UPDATE person_relatives SET idx=5 WHERE (idx=%s) and (persons_id_oid="%s");': ['6', '0123456789ABCDEF'],
        'UPDATE person_relative_contact_phones SET persons_relatives_idx=2 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '3'],
        'UPDATE person_relative_contact_phones SET persons_relatives_idx=3 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '4'],
        'UPDATE person_relative_contacts SET persons_relatives_idx=8 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '9'],
        'UPDATE person_relative_jobs SET persons_relatives_idx=4 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '5'],
        'UPDATE person_relative_jobs SET persons_relatives_idx=7 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '8'],
        'UPDATE person_relative_contact_phones SET persons_relatives_idx=6 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '7'],
        'UPDATE person_relative_contact_phones SET persons_relatives_idx=9 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '10'],
        'UPDATE person_relatives SET idx=2 WHERE (idx=%s) and (persons_id_oid="%s");': ['3', '0123456789ABCDEF'],
        'UPDATE person_relative_contact_phones SET persons_relatives_idx=7 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '8'],
        'UPDATE person_relatives SET idx=8 WHERE (idx=%s) and (persons_id_oid="%s");': ['9', '0123456789ABCDEF'],
        'UPDATE person_relative_contacts SET persons_relatives_idx=3 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '4'],
        'UPDATE person_relative_contacts SET persons_relatives_idx=5 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '6'],
        'UPDATE person_relative_contacts SET persons_relatives_idx=4 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '5'],
        'UPDATE person_relatives SET idx=4 WHERE (idx=%s) and (persons_id_oid="%s");': ['5', '0123456789ABCDEF'],
        'UPDATE person_relative_jobs SET persons_relatives_idx=9 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '10'],
        'UPDATE person_relatives SET idx=3 WHERE (idx=%s) and (persons_id_oid="%s");': ['4', '0123456789ABCDEF'],
        'UPDATE person_relative_contact_phones SET persons_relatives_idx=4 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '5'],
        'UPDATE person_relative_contacts SET persons_relatives_idx=2 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '3'],
        'UPDATE person_relative_contacts SET persons_relatives_idx=7 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '8'],
        'UPDATE person_relative_contacts SET persons_relatives_idx=9 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '10'],
        'UPDATE person_relative_jobs SET persons_relatives_idx=8 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '9'],
        'UPDATE person_relatives SET idx=9 WHERE (idx=%s) and (persons_id_oid="%s");': ['10', '0123456789ABCDEF'],
        'UPDATE person_relatives SET idx=7 WHERE (idx=%s) and (persons_id_oid="%s");': ['8', '0123456789ABCDEF'],
        'UPDATE person_relatives SET idx=6 WHERE (idx=%s) and (persons_id_oid="%s");': ['7', '0123456789ABCDEF'],
        'UPDATE person_relative_contact_phones SET persons_relatives_idx=5 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '6'],
        'UPDATE person_relative_jobs SET persons_relatives_idx=5 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '6'],
        'UPDATE person_relative_contact_phones SET persons_relatives_idx=8 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '9'],
        'UPDATE person_relative_jobs SET persons_relatives_idx=2 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '3'],
        'UPDATE person_relative_jobs SET persons_relatives_idx=3 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '4'],
        'UPDATE person_relative_contacts SET persons_relatives_idx=6 WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '7']}, 'del': {
        'DELETE FROM person_relative_contact_phones WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '2'],
        'DELETE FROM person_relative_jobs WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '2'],
        'DELETE FROM person_relatives WHERE (idx=%s) and (persons_id_oid="%s");': ['2', '0123456789ABCDEF'],
        'DELETE FROM person_relative_contacts WHERE (persons_id_oid="%s") and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '2']}}
    assert model == result

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.5.phones'
    id = '0123456789ABCDEF'
    result = gen_statements(schema, path, id)
    model = {'upd': {}, 'del': {
        'DELETE FROM person_relative_contact_phones WHERE (persons_id_oid="%s") and (persons_relatives_contacts_idx=%s) and (persons_relatives_idx=%s);': [
            '0123456789ABCDEF', '5', '2']}}
    assert model == result

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
    test_get_conditions_list()
    test_get_where_templates()
    test_gen_statements()


pp = pprint.PrettyPrinter(indent=4)
run_tests()

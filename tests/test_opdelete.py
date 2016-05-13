__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"

from gizer.opdelete import *
import json
import pprint
import re


"""
Tests are using postgres database
For succesfully running tests should to be created database test_db with schema test_schema with full access for used user
"""

TEST_INFO = 'TEST_OPDELETE'

def test_database_prepare():
    connstr = environ['TEST_PSQLCONN']
    connector = psycopg2.connect(connstr)

    curs = connector.cursor()
    test_database_clear(connector)

    #preparing test tables && records
    SQL_CREATE_person_relative_contacts = '\
        CREATE TABLE test_schema.person_relative_contacts\
        (\
          idx bigint,\
          persons_relatives_idx bigint,\
          persons_id_oid text\
        );\
    '
    curs.execute(SQL_CREATE_person_relative_contacts)
    SQL_INSERT_max_id_person_relative_contacts= "\
        INSERT INTO test_schema.person_relative_contacts(\
            idx, persons_relatives_idx, persons_id_oid)\
            VALUES (10, 2, '0123456789ABCDEF');\
    "
    curs.execute(SQL_INSERT_max_id_person_relative_contacts)

    SQL_CREATE_person_relative_contacts = '\
        CREATE TABLE test_schema.person_relatives\
        (\
          idx bigint,\
          persons_id_oid text\
        )\
    '
    curs.execute(SQL_CREATE_person_relative_contacts)


    SQL_INSERT_max_id_person_relative_contacts= "\
        INSERT INTO test_schema.person_relatives(\
            idx, persons_id_oid)\
            VALUES (10, '0123456789ABCDEF');    \
    "
    curs.execute(SQL_INSERT_max_id_person_relative_contacts)
    connector.commit()
    return connector

def test_database_clear( connector ):
    curs = connector.cursor()
    SQL_DROP_person_relatives = "DROP TABLE IF EXISTS test_schema.person_relatives;"
    SQL_DROP_person_relative_contacts = "DROP TABLE IF EXISTS test_schema.person_relative_contacts;"

    curs.execute(SQL_DROP_person_relative_contacts)
    curs.execute(SQL_DROP_person_relatives)
    connector.commit()


def test_get_ids_list():
    schema = json.loads(open('test_data/test_schema.txt').read())
    model = {'idx': 'bigint'}
    assert get_ids_list(schema, 0) == model

    schema = json.loads(open('test_data/test_schema2.txt').read())
    model = {'id': 'text'}
    assert get_ids_list(schema, 1) == model

    schema = json.loads(open('test_data/test_schema3.txt').read())
    model = {'id_oid': 'text'}
    assert get_ids_list(schema, 1) == model

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
    path = 'persons'
    id = '0123456789abcdef'
    model = {'target': {'idx': '0123456789abcdef'},
             'child': {'persons_idx': '0123456789abcdef'}}
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
    model = {'target': {'values': ['0123456789abcdef'], 'template': '(idx=(%s))'},
             'child': {'values': ['0123456789abcdef'], 'template': '(persons_relatives_contacts_phones_idx=(%s))'}}
    result = get_where_templates(conditions_list)
    assert model == result

    conditions_list = {
        'target': {'persons_relatives_idx': '2', 'persons_relatives_contacts_idx': '3',
                   'persons_id_oid': '0123456789abcdef', 'idx': '4'},
        'child': {'persons_relatives_idx': '2', 'persons_relatives_contacts_idx': '3',
                  'persons_id_oid': '0123456789abcdef', 'persons_relatives_contacts_phones_idx': '4'}
    }

    model = {'target': {'values': ['4', '0123456789abcdef', '3', '2'],
                        'template': '(idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s))'},
             'child': {'values': ['0123456789abcdef', '3', '4', '2'],
                       'template': '(persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_contacts_phones_idx=(%s)) and (persons_relatives_idx=(%s))'}}
    result = get_where_templates(conditions_list)
    assert model == result

    conditions_list = {
        'target': {'persons_relatives_idx': '2', 'persons_id_oid': '0123456789abcdef', 'idx': '5'},
        'child': {'persons_relatives_idx': '2', 'persons_id_oid': '0123456789abcdef',
                  'persons_relatives_contacts_idx': '5'}
    }
    model = {'target': {'values': ['5', '0123456789abcdef', '2'],
                        'template': '(idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s))'},
             'child': {'values': ['0123456789abcdef', '5', '2'],
                       'template': '(persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s))'}}
    result = get_where_templates(conditions_list)
    assert model == result

    conditions_list = {
        'target': {'id_oid': '0123456789abcdef'},
        'child': {'persons_id_oid': '0123456789abcdef'}
    }
    model = {
        'target': {
            'values': ['0123456789abcdef'],
            'template': '(id_oid=(%s))'
        },
        'child': {
            'values': ['0123456789abcdef'],
            'template': '(persons_id_oid=(%s))'
        }
    }
    result = get_where_templates(conditions_list)
    assert model == result

    print(TEST_INFO, 'get_where_templates', 'PASSED')


def test_gen_statements():
    dbreq = test_database_prepare()
    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons'
    id = '0123456789ABCDEF'
    result = gen_statements(dbreq, schema, path, id, 'test_db', 'operational')
    model = {
        'upd': {},
        'del': {
            'DELETE FROM test_db.operational.persons WHERE (id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM test_db.operational.person_dates WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM test_db.operational.person_relatives WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM test_db.operational.person_relative_jobs WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM test_db.operational.person_relative_contacts WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM test_db.operational.person_relative_contact_phones WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM test_db.operational.person_personal_inf_fl_nam_SSNs WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM test_db.operational.person_indeces WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF']
        }
    }

    assert model == result

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons'
    id = '0123456789ABCDEF'
    result = gen_statements(dbreq, schema, path, id, 'test_db', 'test_schema')
    model = {
        'upd': {},
        'del': {
            'DELETE FROM test_db.test_schema.persons WHERE (id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM test_db.test_schema.person_dates WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM test_db.test_schema.person_relatives WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM test_db.test_schema.person_relative_jobs WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM test_db.test_schema.person_relative_contacts WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM test_db.test_schema.person_relative_contact_phones WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM test_db.test_schema.person_personal_inf_fl_nam_SSNs WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM test_db.test_schema.person_indeces WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF']
        }
    }
    assert model == result

    #SQL_prepare = 'INSERT INTO '
    #TODO place insert preset rows HERE
    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.5'
    id = '0123456789ABCDEF'
    result = gen_statements(dbreq, schema, path, id, 'test_db', 'test_schema')
    model = {
        'upd': {
            'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_contacts_idx=6 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '7', '2'],
            'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_contacts_idx=8 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '9', '2'],
            'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_contacts_idx=5 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '6', '2'],
            'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_contacts_idx=7 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '8', '2'],
            'UPDATE test_db.test_schema.person_relative_contacts SET idx=5 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '6', '0123456789ABCDEF', '2'],
            'UPDATE test_db.test_schema.person_relative_contacts SET idx=8 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '9', '0123456789ABCDEF', '2'],
            'UPDATE test_db.test_schema.person_relative_contacts SET idx=7 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '8', '0123456789ABCDEF', '2'],
            'UPDATE test_db.test_schema.person_relative_contacts SET idx=9 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '10', '0123456789ABCDEF', '2'],
            'UPDATE test_db.test_schema.person_relative_contacts SET idx=6 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '7', '0123456789ABCDEF', '2'],
            'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_contacts_idx=9 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '10', '2']},
        'del': {
            'DELETE FROM test_db.test_schema.person_relative_contacts WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '5', '0123456789ABCDEF', '2'],
            'DELETE FROM test_db.test_schema.person_relative_contact_phones WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '5', '2']}}
    assert sqls_to_dict(model) == sqls_to_dict(result)

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.5'
    id = '0123456789ABCDEF'
    result = gen_statements(dbreq, schema, path, id, 'test_db', 'test_schema')
    model = {
        'upd': {
            'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_contacts_idx=6 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '7', '2'],
            'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_contacts_idx=8 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '9', '2'],
            'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_contacts_idx=5 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '6', '2'],
            'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_contacts_idx=7 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '8', '2'],
            'UPDATE test_db.test_schema.person_relative_contacts SET idx=5 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '6', '0123456789ABCDEF', '2'],
            'UPDATE test_db.test_schema.person_relative_contacts SET idx=8 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '9', '0123456789ABCDEF', '2'],
            'UPDATE test_db.test_schema.person_relative_contacts SET idx=7 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '8', '0123456789ABCDEF', '2'],
            'UPDATE test_db.test_schema.person_relative_contacts SET idx=9 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '10', '0123456789ABCDEF', '2'],
            'UPDATE test_db.test_schema.person_relative_contacts SET idx=6 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '7', '0123456789ABCDEF', '2'],
            'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_contacts_idx=9 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '10', '2']},
        'del': {
            'DELETE FROM test_db.test_schema.person_relative_contacts WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '5', '0123456789ABCDEF', '2'],
            'DELETE FROM test_db.test_schema.person_relative_contact_phones WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '5', '2']}}
    assert sqls_to_dict(model) == sqls_to_dict(result)

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2'
    id = '0123456789ABCDEF'
    result = gen_statements(dbreq, schema, path, id, 'test_db', 'test_schema')
    model = {'upd': {
        'UPDATE test_db.test_schema.person_relative_jobs SET persons_relatives_idx=6 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '7'],
        'UPDATE test_db.test_schema.person_relatives SET idx=5 WHERE (idx=(%s)) and (persons_id_oid=(%s));': ['6', '0123456789ABCDEF'],
        'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_idx=2 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '3'],
        'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_idx=3 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '4'],
        'UPDATE test_db.test_schema.person_relative_contacts SET persons_relatives_idx=8 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '9'],
        'UPDATE test_db.test_schema.person_relative_jobs SET persons_relatives_idx=4 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '5'],
        'UPDATE test_db.test_schema.person_relative_jobs SET persons_relatives_idx=7 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '8'],
        'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_idx=6 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '7'],
        'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_idx=9 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '10'],
        'UPDATE test_db.test_schema.person_relatives SET idx=2 WHERE (idx=(%s)) and (persons_id_oid=(%s));': ['3', '0123456789ABCDEF'],
        'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_idx=7 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '8'],
        'UPDATE test_db.test_schema.person_relatives SET idx=8 WHERE (idx=(%s)) and (persons_id_oid=(%s));': ['9', '0123456789ABCDEF'],
        'UPDATE test_db.test_schema.person_relative_contacts SET persons_relatives_idx=3 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '4'],
        'UPDATE test_db.test_schema.person_relative_contacts SET persons_relatives_idx=5 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '6'],
        'UPDATE test_db.test_schema.person_relative_contacts SET persons_relatives_idx=4 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '5'],
        'UPDATE test_db.test_schema.person_relatives SET idx=4 WHERE (idx=(%s)) and (persons_id_oid=(%s));': ['5', '0123456789ABCDEF'],
        'UPDATE test_db.test_schema.person_relative_jobs SET persons_relatives_idx=9 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '10'],
        'UPDATE test_db.test_schema.person_relatives SET idx=3 WHERE (idx=(%s)) and (persons_id_oid=(%s));': ['4', '0123456789ABCDEF'],
        'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_idx=4 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '5'],
        'UPDATE test_db.test_schema.person_relative_contacts SET persons_relatives_idx=2 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '3'],
        'UPDATE test_db.test_schema.person_relative_contacts SET persons_relatives_idx=7 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '8'],
        'UPDATE test_db.test_schema.person_relative_contacts SET persons_relatives_idx=9 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '10'],
        'UPDATE test_db.test_schema.person_relative_jobs SET persons_relatives_idx=8 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '9'],
        'UPDATE test_db.test_schema.person_relatives SET idx=9 WHERE (idx=(%s)) and (persons_id_oid=(%s));': ['10', '0123456789ABCDEF'],
        'UPDATE test_db.test_schema.person_relatives SET idx=7 WHERE (idx=(%s)) and (persons_id_oid=(%s));': ['8', '0123456789ABCDEF'],
        'UPDATE test_db.test_schema.person_relatives SET idx=6 WHERE (idx=(%s)) and (persons_id_oid=(%s));': ['7', '0123456789ABCDEF'],
        'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_idx=5 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '6'],
        'UPDATE test_db.test_schema.person_relative_jobs SET persons_relatives_idx=5 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '6'],
        'UPDATE test_db.test_schema.person_relative_contact_phones SET persons_relatives_idx=8 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '9'],
        'UPDATE test_db.test_schema.person_relative_jobs SET persons_relatives_idx=2 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '3'],
        'UPDATE test_db.test_schema.person_relative_jobs SET persons_relatives_idx=3 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '4'],
        'UPDATE test_db.test_schema.person_relative_contacts SET persons_relatives_idx=6 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '7']}, 'del': {
        'DELETE FROM test_db.test_schema.person_relative_contact_phones WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '2'],
        'DELETE FROM test_db.test_schema.person_relative_jobs WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '2'],
        'DELETE FROM test_db.test_schema.person_relatives WHERE (idx=(%s)) and (persons_id_oid=(%s));': ['2', '0123456789ABCDEF'],
        'DELETE FROM test_db.test_schema.person_relative_contacts WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '2']}}
    assert model == result

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.5.phones'
    id = '0123456789ABCDEF'
    result = gen_statements(dbreq, schema, path, id, '', '')
    model = {'upd': {}, 'del': {
        'DELETE FROM person_relative_contact_phones WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '5', '2']}}
    assert model == result

    test_database_clear(dbreq)

    print(TEST_INFO, 'gen_statements', 'PASSED')


def check_dict(list1, list2):
    if len(list2) <> len(list2):
        return False
    for it in list1:
        if list1[it] <> list2[it]:
            return False
    return True


def sqls_to_dict(sql_dict):
    parsed_dict = {}
    for model_item in sql_dict:
        if model_item == 'upd':
            if type(sql_dict[model_item]) == dict:
                for sql in sql_dict[model_item]:
                    r = re.compile('UPDATE (.*?)WHERE')
                    ext_key = r.search(sql).group(1)
                    parsed_dict[ext_key] = parse_upd({sql:sql_dict[model_item][sql]})
        if model_item == 'del':
            if type(sql_dict[model_item]) == dict:
                for sql in sql_dict[model_item]:
                    r = re.compile('DELETE FROM(.*?)WHERE')
                    ext_key = r.search(sql).group(1)
                    parsed_dict[ext_key] = parse_del({sql:sql_dict[model_item][sql]})
    return parsed_dict


def parse_upd(sql_upd):
    if not type(sql_upd) is dict:
        return {}
    stmnt = sql_upd.iterkeys().next()
    values = sql_upd.itervalues().next()
    clauses = stmnt.split(' ')
    updated_table = clauses [1]
    set_value = clauses [3]
    's'.replace(';', '')
    where_clauses = [cl.replace(';', '') for cl in clauses [5:] if cl != 'and']
    where_dict = {}
    for i, cl in enumerate(where_clauses):
        where_dict[cl] = values[i]
    return {'table':updated_table, 'set_value':set_value, 'where_dict':where_dict}


def parse_del(sql_upd):
    if not type(sql_upd) is dict:
        return {}
    stmnt = sql_upd.iterkeys().next()
    values = sql_upd.itervalues().next()
    clauses = stmnt.split(' ')
    updated_table = clauses [2]
    where_clauses = [cl.replace(';', '') for cl in clauses [4:] if cl != 'and']
    where_dict = {}
    for i, cl in enumerate(where_clauses):
        where_dict[cl] = values[i]
    return {'table':updated_table, 'where_dict':where_dict}


def UPDATE_compatator(model_upd, result_upd):
    parsed_model = parse_upd(model_upd)
    parsed_result = parse_upd(result_upd)
    if parsed_model == parsed_result:
        return True
    else:
        return False


def run_tests():
    test_get_ids_list()
    test_get_child_dict_item()
    test_get_tables_list()
    test_get_conditions_list()
    test_get_where_templates()
    test_gen_statements()


pp = pprint.PrettyPrinter(indent=4)
run_tests()

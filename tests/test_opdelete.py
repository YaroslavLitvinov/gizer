__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"

from gizer.opdelete import *
import json
import pprint
from test_util import *
from psycopg2.extensions import AsIs
from psycopg2 import ProgrammingError
from os import environ



"""
Tests are using postgres database
For succesfully running tests should to be created database test_db with schema test_schema with full access for used user
"""

TEST_INFO = 'TEST_OPDELETE'
SCHEMA_NAME = 'public'



def database_clear(dbconnector):
    curs = dbconnector.cursor()
    SQL_DROP_person_relatives = "DROP TABLE IF EXISTS "+SCHEMA_NAME+".person_relatives;"
    SQL_DROP_person_relative_contacts = "DROP TABLE IF EXISTS "+SCHEMA_NAME+".person_relative_contacts;"

    try:
        curs.execute(SQL_DROP_person_relative_contacts)
        curs.execute(SQL_DROP_person_relatives)
    except:
        pass
    dbconnector.commit()


def database_prepare():
    connstr = environ['TEST_PSQLCONN']
    user_str = dict(re.findall(r'(\S+)=(".*?"|\S+)', connstr))['user']
    connector = psycopg2.connect(connstr)

    curs = connector.cursor()
    SQL_create_schema = """CREATE SCHEMA %s AUTHORIZATION %s;"""
    params = (AsIs(SCHEMA_NAME), AsIs(user_str))
    try:
        curs.execute(SQL_create_schema, params)
    except ProgrammingError:
        pass
    database_clear(connector)

    #preparing test tables && records
    SQL_CREATE_person_relative_contacts = ' \
        CREATE TABLE test_schema.person_relative_contacts\
        (\
          idx bigint,\
          persons_relatives_idx bigint,\
          persons_id_oid text\
        );\
    '
    SQL_DROP_person_relative_contacts = 'DROP TABLE test_schema.person_relative_contacts;'
    try:
        curs.execute(SQL_DROP_person_relative_contacts)
    except ProgrammingError:
        pass
    curs.execute('COMMIT')
    curs.execute(SQL_CREATE_person_relative_contacts)
    SQL_INSERT_max_id_person_relative_contacts= "\
        INSERT INTO test_schema.person_relative_contacts(\
            idx, persons_relatives_idx, persons_id_oid)\
            VALUES (11, 3, '0123456789ABCDEF');\
    "
    curs.execute(SQL_INSERT_max_id_person_relative_contacts)

    SQL_CREATE_person_relatives = '\
        CREATE TABLE test_schema.person_relatives\
        (\
          idx bigint,\
          persons_id_oid text\
        )\
    '
    SQL_DROP_person_relatives = 'DROP TABLE test_schema.person_relatives;'
    try:
        curs.execute(SQL_DROP_person_relatives)
    except ProgrammingError:
        pass

    curs.execute(SQL_CREATE_person_relatives)


    SQL_INSERT_max_id_person_relative_contacts= "\
        INSERT INTO test_schema.person_relatives(\
            idx, persons_id_oid)\
            VALUES (11, '0123456789ABCDEF');    \
    "
    curs.execute(SQL_INSERT_max_id_person_relative_contacts)
    connector.commit()
    return connector


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

    print(TEST_INFO, 'get_ids_list', 'PASSED')



def test_get_tables_list():

    schema = json.loads(open('test_data/test_schema5.txt').read())
    result = get_tables_structure(schema,'mains',{},{},1, '').keys()
    model = ['mains', 'main_personal_info_fl_name_SSNs', 'main_relatives', 'main_relative_contacts',
             'main_relative_contact_phones', 'main_relative_jobs', 'main_indeces', 'main_dates']
    assert sorted(model) == sorted(result)

    schema = json.loads(open('test_data/test_schema4.txt').read())
    result = get_tables_structure(schema,'table1',{},{},1, '').keys()
    model = ['table_personal_info_full_name_SSNs', 'table_relative_contact_phones', 'table_relative_contacts',
             'table_relatives', 'table1', 'table_indeces', 'table_dates']
    assert sorted(model) == sorted(result)

    schema = json.loads(open('test_data/test_schema3.txt').read())
    result = get_tables_structure(schema,'table1',{},{},1, '').keys()
    model = ['table1', 'table_relative_contact_phones', 'table_relative_contacts', 'table_relatives',
             'table_indeces', 'table_dates']
    assert sorted(model) == sorted(result)

    schema = json.loads(open('test_data/test_schema6.txt').read())
    result = get_tables_structure(schema,'table1',{},{},1, '').keys()
    model = ['table1']
    assert sorted(model) == sorted(result)

    schema = json.loads(open('test_data/schemas/rails4_mongoid_development/rated_posts.js').read())
    result = get_tables_structure(schema,'rated_posts',{},{},1, '').keys()
    model = [u'rated_posts', u'rated_post_tests', u'rated_post_comments', u'rated_post_comment_tests', u'rated_post_comment_rates', u'rated_post_comment_rate_item_rates', u'rated_post_rates', u'rated_post_enclosed_field_array']
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
        'target': {'persons_relatives_idx': '3', 'persons_relatives_contacts_idx': '4',
                   'persons_id_oid': '0123456789abcdef', 'idx': '5'},
        'child': {'persons_relatives_idx': '3', 'persons_relatives_contacts_idx': '4',
                  'persons_id_oid': '0123456789abcdef', 'persons_relatives_contacts_phones_idx': '5'}
    }
    result = get_conditions_list(schema, path, id)
    assert model == result

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.3.phones'
    id = '0123456789abcdef'
    model = {
        'target': {'persons_relatives_idx': '3', 'persons_relatives_contacts_idx': '4',
                   'persons_id_oid': '0123456789abcdef'},
        'child': {'persons_relatives_idx': '3', 'persons_relatives_contacts_idx': '4',
                  'persons_id_oid': '0123456789abcdef'}
    }
    result = get_conditions_list(schema, path, id)
    assert model == result

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.5'
    id = '0123456789abcdef'
    model = {
        'target': {'persons_relatives_idx': '3', 'persons_id_oid': '0123456789abcdef', 'idx': '6'},
        'child': {'persons_relatives_idx': '3', 'persons_id_oid': '0123456789abcdef',
                  'persons_relatives_contacts_idx': '6'}
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
    connstr = environ['TEST_PSQLCONN']
    db_name = dict(re.findall(r'(\S+)=(".*?"|\S+)', connstr))['dbname']
    schema_name = SCHEMA_NAME+'.'
    dbreq = database_prepare()
    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons'
    id = '0123456789ABCDEF'
    result = gen_statements(dbreq, schema, path, id, DatabaseInfo(db_name, schema_name[:-1]))
    model = {
        'upd': {},
        'del': {
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"persons" WHERE (id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_dates" WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_relatives" WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_relative_jobs" WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_relative_contacts" WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_relative_contact_phones" WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_personal_info_fl_name_SSNs" WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_indeces" WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF']
        }
    }

    assert model == result

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons'
    id = '0123456789ABCDEF'
    result = gen_statements(dbreq, schema, path, id, DatabaseInfo(db_name, SCHEMA_NAME))
    model = {
        'upd': {},
        'del': {
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"persons" WHERE (id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_dates" WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_relatives" WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_relative_jobs" WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_relative_contacts" WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_relative_contact_phones" WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_personal_info_fl_name_SSNs" WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_indeces" WHERE (persons_id_oid=(%s));': ['0123456789ABCDEF']
        }
    }
    assert model == result

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.5'
    id = '0123456789ABCDEF'
    result = gen_statements(dbreq, schema, path, id, DatabaseInfo(db_name, SCHEMA_NAME))
    model = {
        'upd': {
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_contacts_idx=7 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));':
                ['0123456789ABCDEF', '8', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_contacts_idx=9 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));':
                ['0123456789ABCDEF', '10', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_contacts_idx=6 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '7', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_contacts_idx=8 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '9', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET idx=6 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '7', '0123456789ABCDEF', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET idx=9 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '10', '0123456789ABCDEF', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET idx=8 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '9', '0123456789ABCDEF', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET idx=10 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '11', '0123456789ABCDEF', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET idx=7 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '8', '0123456789ABCDEF', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_contacts_idx=10 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '11', '3']},
        'del': {
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_relative_contacts" WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '6', '0123456789ABCDEF', '3'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'"person_relative_contact_phones" WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '6', '3']}}
    assert sqls_to_dict(model) == sqls_to_dict(result)

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.5'
    id = '0123456789ABCDEF'
    result = gen_statements(dbreq, schema, path, id, DatabaseInfo(db_name, SCHEMA_NAME))
    model = {
        'upd': {
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_contacts_idx=7 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '8', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_contacts_idx=9 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '10', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_contacts_idx=6 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '7', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_contacts_idx=8 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '9', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET idx=6 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '7', '0123456789ABCDEF', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET idx=9 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '10', '0123456789ABCDEF', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET idx=8 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '9', '0123456789ABCDEF', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET idx=10 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '11', '0123456789ABCDEF', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET idx=7 WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '8', '0123456789ABCDEF', '3'],
            'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_contacts_idx=10 WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '11', '3']},
        'del': {
            'DELETE FROM '+'.'.join([db_name, schema_name])+'person_relative_contacts WHERE (idx=(%s)) and (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));': [
                '6', '0123456789ABCDEF', '3'],
            'DELETE FROM '+'.'.join([db_name, schema_name])+'person_relative_contact_phones WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
                '0123456789ABCDEF', '6', '3']}}
    assert sqls_to_dict(model) == sqls_to_dict(result)

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2'
    id = '0123456789ABCDEF'
    result = gen_statements(dbreq, schema, path, id, DatabaseInfo(db_name, SCHEMA_NAME))
    model = {'upd': {
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_jobs SET persons_relatives_idx=7 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':            ['0123456789ABCDEF', '8'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relatives SET idx=6 WHERE (idx=(%s)) and (persons_id_oid=(%s));':                                                    ['7', '0123456789ABCDEF'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_idx=3 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':  ['0123456789ABCDEF', '4'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_idx=4 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':  ['0123456789ABCDEF', '5'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET persons_relatives_idx=9 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':        ['0123456789ABCDEF', '10'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_jobs SET persons_relatives_idx=5 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':            ['0123456789ABCDEF', '6'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_jobs SET persons_relatives_idx=8 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':            ['0123456789ABCDEF', '9'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_idx=7 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':  ['0123456789ABCDEF', '8'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_idx=10 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':  ['0123456789ABCDEF', '11'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relatives SET idx=3 WHERE (idx=(%s)) and (persons_id_oid=(%s));':                                                    ['4', '0123456789ABCDEF'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_idx=8 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':  ['0123456789ABCDEF', '9'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relatives SET idx=9 WHERE (idx=(%s)) and (persons_id_oid=(%s));':                                                    ['10', '0123456789ABCDEF'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET persons_relatives_idx=4 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':        ['0123456789ABCDEF', '5'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET persons_relatives_idx=6 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':        ['0123456789ABCDEF', '7'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET persons_relatives_idx=5 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':        ['0123456789ABCDEF', '6'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relatives SET idx=5 WHERE (idx=(%s)) and (persons_id_oid=(%s));':                                                    ['6', '0123456789ABCDEF'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_jobs SET persons_relatives_idx=10 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':            ['0123456789ABCDEF', '11'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relatives SET idx=4 WHERE (idx=(%s)) and (persons_id_oid=(%s));':                                                    ['5', '0123456789ABCDEF'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_idx=5 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':  ['0123456789ABCDEF', '6'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET persons_relatives_idx=3 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':        ['0123456789ABCDEF', '4'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET persons_relatives_idx=8 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':        ['0123456789ABCDEF', '9'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET persons_relatives_idx=10 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':        ['0123456789ABCDEF', '11'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_jobs SET persons_relatives_idx=9 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':            ['0123456789ABCDEF', '10'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relatives SET idx=10 WHERE (idx=(%s)) and (persons_id_oid=(%s));':                                                    ['11', '0123456789ABCDEF'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relatives SET idx=8 WHERE (idx=(%s)) and (persons_id_oid=(%s));':                                                    ['9', '0123456789ABCDEF'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relatives SET idx=7 WHERE (idx=(%s)) and (persons_id_oid=(%s));':                                                    ['8', '0123456789ABCDEF'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_idx=6 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':  ['0123456789ABCDEF', '7'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_jobs SET persons_relatives_idx=6 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':            ['0123456789ABCDEF', '7'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contact_phones SET persons_relatives_idx=9 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':  ['0123456789ABCDEF', '10'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_jobs SET persons_relatives_idx=3 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':            ['0123456789ABCDEF', '4'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_jobs SET persons_relatives_idx=4 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':            ['0123456789ABCDEF', '5'],
        'UPDATE '+'.'.join([db_name, schema_name])+'person_relative_contacts SET persons_relatives_idx=7 WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':        ['0123456789ABCDEF', '8']},
    'del': {
        'DELETE FROM '+'.'.join([db_name, schema_name])+'person_relative_contact_phones WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':                         ['0123456789ABCDEF', '3'],
        'DELETE FROM '+'.'.join([db_name, schema_name])+'person_relative_jobs WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':                                   ['0123456789ABCDEF', '3'],
        'DELETE FROM '+'.'.join([db_name, schema_name])+'person_relatives WHERE (idx=(%s)) and (persons_id_oid=(%s));':                                                         ['3', '0123456789ABCDEF'],
        'DELETE FROM '+'.'.join([db_name, schema_name])+'person_relative_contacts WHERE (persons_id_oid=(%s)) and (persons_relatives_idx=(%s));':                               ['0123456789ABCDEF', '3']}}
    assert sqls_to_dict(model) == sqls_to_dict(result)

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.2.contacts.5.phones'
    id = '0123456789ABCDEF'
    result = gen_statements(dbreq, schema, path, id, DatabaseInfo('', ''))
    model = {'upd': {}, 'del': {
        'DELETE FROM "person_relative_contact_phones" WHERE (persons_id_oid=(%s)) and (persons_relatives_contacts_idx=(%s)) and (persons_relatives_idx=(%s));': [
            '0123456789ABCDEF', '6', '3']}}
    assert model == result

    schema = json.loads(open('test_data/test_schema5.txt').read())
    path = 'persons.relatives.3.some_table'
    id = 'AABBCCDDEEFF'
    model=  {'upd': {}, 'del': {}}
    result = gen_statements(dbreq, schema, path, id, DatabaseInfo('', ''))
    assert model == result

    database_clear(dbreq)

    print(TEST_INFO, 'gen_statements', 'PASSED')


def check_dict(list1, list2):
    if len(list2) <> len(list2):
        return False
    for it in list1:
        if list1[it] <> list2[it]:
            return False
    return True


def UPDATE_compatator(model_upd, result_upd):
    parsed_model = parse_upd(model_upd)
    parsed_result = parse_upd(result_upd)
    if parsed_model == parsed_result:
        return True
    else:
        return False


def run_tests():
    test_get_ids_list()
    test_get_tables_list()
    test_get_conditions_list()
    test_get_where_templates()
    test_gen_statements()


pp = pprint.PrettyPrinter(indent=4)
run_tests()

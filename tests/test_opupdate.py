#!/usb/bin/env python
"""Tests."""
from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.opupdate import *
from update_test_data import *
import pprint
from bson.json_util import loads
import datetime
import psycopg2
from os import environ
from test_util import sqls_to_dict


TEST_INFO = 'TEST_OPUPDATE'

'update post_adresse_streets set name="STREETNAME" where post_id="aabbccddeeff" and post_adresses_idx=0, idx=7'


def database_prepare():
    connstr = environ['TEST_PSQLCONN']
    connector = psycopg2.connect(connstr)
    return connector

def database_clear( connector ):
    return 0

def test_get_obj_id():
    oplog_data = loads(oplog_u_01)
    model = {"id_oid":"56b8da51f9fcee1b00000006"}
    result = get_obj_id(oplog_data)
    assert result == model
    print(TEST_INFO, 'get_obj_id', 'PASSED')


def test_get_obj_id_recursive():
    model = {'aaa_bbb_ccc_ddd_eeee': 'abcdef'}
    assert model == get_obj_id_recursive(test_data_01, [], [])
    print(TEST_INFO, 'get_obj_id_recursive', 'PASSED')

def d(str_date, tz_info):
    timestamp_fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    l = datetime.datetime.strptime(str_date, timestamp_fmt)
    return datetime.datetime(l.year, l.month, l.day, l.hour, l.minute, l.second, l.microsecond, tzinfo=tz_info)


def test_update():

    dbreq = database_prepare()

    # schema = json.loads(open('/home/volodymyr/git/gizer/test_data/test_schema__.txt').read())
    # oplog_data = loads(test_data_03)
    #
    # result = update(dbreq, schema, oplog_data, '', '')
    # print(result)
    # # model = ['mains', 'main_personal_inf_fl_nam_SSNs', 'main_relatives', 'main_relative_contacts',
    # #          'main_relative_contact_phones', 'main_relative_jobs', 'main_indeces', 'main_dates']
    # # assert sorted(model) == sorted(result)
    # #
    # # exit(0)


    schemas_path = 'test_data/schemas/rails4_mongoid_development'
    schema_engine = get_schema_engines_as_dict(schemas_path)

    tz_info = loads(oplog_tz_info)['tzinfo_obj'].tzinfo

    print('Test #1')
    oplog_data = loads(test_data_05)
    model = [{'do $$    begin    UPDATE rated_post_comment_rates SET user_info_name=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'Vasya', '56b8da59f9fcee1b00000014', u'1', u'2', None, None, None, None, '56b8da59f9fcee1b00000014', None, None, None, u'Vasya', 1, 2)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #2')
    oplog_data = loads(test_data_02)
    model = []
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #3')
    oplog_data = loads(oplog_u_01)
    model = [{'do $$    begin    UPDATE post_comments SET body=(%s), created_at=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'comment6',  d("2016-02-08T19:42:33.589Z", tz_info), '56b8efa9f9fcee1b0000000f', d("2016-02-08T19:42:33.589Z", tz_info), '56b8da51f9fcee1b00000006', u'5', u'comment6', d("2016-02-08T19:42:33.589Z", tz_info), 7, '56b8efa9f9fcee1b0000000f', '56b8da51f9fcee1b00000006', d("2016-02-08T19:42:33.589Z", tz_info), 5)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #4')
    model = [{'do $$    begin    UPDATE test_db.test_schema.post_comments SET body=(%s), created_at=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO test_schema."post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'comment6', d('2016-02-08T19:42:33.589Z', tz_info), '56b8efa9f9fcee1b0000000f', d('2016-02-08T19:42:33.589Z', tz_info), '56b8da51f9fcee1b00000006', u'5', u'comment6', d('2016-02-08T19:42:33.589Z', tz_info), 7, '56b8efa9f9fcee1b0000000f', '56b8da51f9fcee1b00000006', d('2016-02-08T19:42:33.589Z', tz_info), 5)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, 'test_db', 'test_schema')
    assert result == model

    print('Test #5')
    oplog_data = loads(oplog_u_02)
    model = [{'UPDATE posts SET updated_at=(%s) WHERE id_oid=(%s);': [(d('2016-02-08T19:52:23.883Z', tz_info), '56b8da59f9fcee1b00000007',)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #6')
    oplog_data = loads(oplog_u_03)
    model = [
        {'DELETE FROM post_comments WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {'DELETE FROM post_comment_tests WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {u'INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);':
             [(None, d("2016-02-08T19:57:56.678Z", tz_info), 7, '56b8f344f9fcee1b00000018', '56b8da59f9fcee1b00000007', d("2016-02-08T19:57:56.678Z", tz_info), 1,)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #7')
    oplog_data = loads(oplog_u_03)
    model = [
        {'DELETE FROM database.schema.post_comments WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {'DELETE FROM database.schema.post_comment_tests WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {u'INSERT INTO schema."post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);':
             [(None, d("2016-02-08T19:57:56.678Z", tz_info), 7, '56b8f344f9fcee1b00000018', '56b8da59f9fcee1b00000007', d("2016-02-08T19:57:56.678Z", tz_info), 1,)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, 'database', 'schema')
    assert result == model

    oplog_data = loads(oplog_u_04)
    model = [{'do $$    begin    UPDATE post_comments SET body=(%s), created_at=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'commments2222', d('2016-02-08T19:58:06.008Z', tz_info), '56b8f34ef9fcee1b00000019', d('2016-02-08T19:58:06.008Z', tz_info), '56b8da59f9fcee1b00000007', u'1', u'commments2222', d('2016-02-08T19:58:06.008Z', tz_info), 7, '56b8f34ef9fcee1b00000019', '56b8da59f9fcee1b00000007', d('2016-02-08T19:58:06.008Z', tz_info), 1)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    oplog_data = loads(oplog_u_05)
    model = [{'do $$    begin    UPDATE post_comments SET created_at=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$':
                  [(d('2016-02-08T19:58:22.847Z', tz_info), '56b8f35ef9fcee1b0000001a', d('2016-02-08T19:58:22.847Z', tz_info), '56b8da59f9fcee1b00000007', u'2', None, d('2016-02-08T19:58:22.847Z', tz_info), 7, '56b8f35ef9fcee1b0000001a', '56b8da59f9fcee1b00000007', d('2016-02-08T19:58:22.847Z', tz_info), 2)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    oplog_data = loads(oplog_u_06)
    model = [
        {'DELETE FROM post_comments WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {'DELETE FROM post_comment_tests WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {u'INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);':
            [(None, d("2016-02-08T19:57:56.678Z", tz_info), 7, '56b8f344f9fcee1b00000018', '56b8da59f9fcee1b00000007', d("2016-02-08T19:57:56.678Z", tz_info), 1,),
             (None, d("2016-02-08T19:58:22.847Z", tz_info), 7, '56b8f35ef9fcee1b0000001a', '56b8da59f9fcee1b00000007', d("2016-02-08T19:58:22.847Z", tz_info), 2,)
            ]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    oplog_data = loads(oplog_u_07)
    model = [
        {'DELETE FROM post_comments WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {'DELETE FROM post_comment_tests WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {u'INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);':
             [(None, d('2016-02-08T19:57:56.678Z', tz_info), 7, '56b8f344f9fcee1b00000018', '56b8da59f9fcee1b00000007', d('2016-02-08T19:57:56.678Z', tz_info), 1)]}]
    result = update(dbreq, schema_engine[oplog_data['ns'].split('.')[1]], oplog_data, '', '')
    assert result == model

    oplog_data = loads(oplog_u_08)
    model = [{'UPDATE posts SET updated_at=(%s), title=(%s) WHERE id_oid=(%s);': [(d('2016-02-08T20:02:12.985Z', tz_info), 'sada', '56b8f05cf9fcee1b00000010',)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    oplog_data = loads(test_data_03)
    model = [{
            'do $$    begin    UPDATE rated_post_comment_rates SET rate=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(2, '56b8da59f9fcee1b00000012', u'1', u'2', None, None, None, None, '56b8da59f9fcee1b00000012', None, None, None, None, 1, 2)]
        }, {
            'do $$    begin    UPDATE rated_post_comments SET updated_at=(%s) WHERE rated_posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comments" ("body", "created_at", "id_bsontype", "id_oid", "rated_posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(d('2016-02-08T19:58:22.847Z', tz_info), '56b8da59f9fcee1b00000012', u'1', None, None, None, None, '56b8da59f9fcee1b00000012', d('2016-02-08T19:58:22.847Z', tz_info), 1)]
        }, {
            'do $$    begin    UPDATE rated_post_comment_rates SET user_id=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(u'B', '56b8da59f9fcee1b00000012', u'1', u'1', None, None, None, None, '56b8da59f9fcee1b00000012', None, u'B', None, None, 1, 1)]
        }
    ]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    oplog_data = loads(test_data_04)
    model = [{
            'do $$    begin    UPDATE rated_post_comment_rates SET rate=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(67, '56b8da59f9fcee1b00000013', u'1', u'2', None, None, None, None, '56b8da59f9fcee1b00000013', None, None, None, None, 1, 2)]
        }, {
            'DELETE FROM rated_post_comment_rate_item_rates WHERE (rated_posts_comments_idx=(%s)) and (rated_posts_comments_rates_idx=(%s)) and (rated_posts_id_oid=(%s));' : [(u'1', u'2', '56b8da59f9fcee1b00000013')]
        }, {
            u'INSERT INTO "rated_post_comment_rate_item_rates" ("created_at", "description", "id_bsontype", "id_oid", "name", "rated_posts_id_oid", "updated_at", "rated_posts_comments_idx", "rated_posts_comments_rates_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);' : [(None, None, 7, '57557e06cf68790000000000', u'Ivan', '56b8da59f9fcee1b00000013', None, 1, 2, 1), (None, None, 7, '57557e06cf68790000000001', u'Susanin', '56b8da59f9fcee1b00000013', None, 1, 2, 2)]
        }, {
            'do $$    begin    UPDATE rated_post_comment_rates SET user_id=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(u'B', '56b8da59f9fcee1b00000013', u'1', u'1', None, None, None, None, '56b8da59f9fcee1b00000013', None, u'B', None, None, 1, 1)]
        }
    ]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print(TEST_INFO, 'update', 'PASSED')


pp = pprint.PrettyPrinter(indent=4)


test_get_obj_id()
test_get_obj_id_recursive()
test_update()
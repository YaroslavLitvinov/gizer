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


# def test_prepare_unset():
#     dbreq = database_prepare()
#     schemas_path = 'test_data/schemas/rails4_mongoid_development'
#     schema_engine = get_schema_engines_as_dict(schemas_path)
#
#     tz_info = loads(oplog_tz_info)['tzinfo_obj'].tzinfo
#     oplog_data = loads(test_data_06)
#     # model = [{'do $$    begin    UPDATE rated_post_comment_rates SET user_info_name=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'Vasya', '56b8da59f9fcee1b00000014', u'1', u'2', None, None, None, None, '56b8da59f9fcee1b00000014', None, None, None, u'Vasya', 1, 2)]}]
# #    model = [{'do $$    begin    UPDATE rated_post_comment_rates SET user_info_name=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'Vasya', '56b8da59f9fcee1b00000014', u'2', u'3', None, None, None, None, '56b8da59f9fcee1b00000014', None, None, None, u'Vasya', 2, 3)]}]
#     print(update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', ''))


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



    # print('Test #0')
    # oplog_data = loads(test_data_18)
    # model = [{'do $$    begin    UPDATE rated_post_comment_rates SET user_info_name=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'Vasya', '56b8da59f9fcee1b00000014', u'2', u'3', None, None, None, None, '56b8da59f9fcee1b00000014', None, None, None, u'Vasya', 2, 3)]}]
    # result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    # print(result)
    # #assert result == model


    # oplog_data = loads(test_data_14)
    # print(update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', ''))
    # oplog_data = loads(oplog_u_09)
    # print(update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', ''))
    # oplog_data = loads(test_data_02)
    # print(update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', ''))
    # oplog_data = loads(test_data_14)
    # print(update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', ''))
    # oplog_data = loads(test_data_15)
    # print(update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', ''))
    # oplog_data = loads(test_data_16)


    print('Test #1')
    oplog_data = loads(test_data_05)
    #TODO fix insert indexes 1, 2 --> 2, 3
    #FIXED
    # model = [{'do $$    begin    UPDATE rated_post_comment_rates SET user_info_name=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'Vasya', '56b8da59f9fcee1b00000014', u'1', u'2', None, None, None, None, '56b8da59f9fcee1b00000014', None, None, None, u'Vasya', 1, 2)]}]
    model = [{'do $$    begin    UPDATE rated_post_comment_rates SET user_info_name=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'Vasya', '56b8da59f9fcee1b00000014', u'2', u'3', None, None, None, None, '56b8da59f9fcee1b00000014', None, None, None, u'Vasya', 2, 3)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #2')
    oplog_data = loads(test_data_02)
    model = []
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #3')
    oplog_data = loads(oplog_u_01)

    #TODO fix insert indexes 5 --> 6
    #FIXED
    # model = [{'do $$    begin    UPDATE post_comments SET body=(%s), created_at=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'comment6',  d("2016-02-08T19:42:33.589Z", tz_info), '56b8efa9f9fcee1b0000000f', d("2016-02-08T19:42:33.589Z", tz_info), '56b8da51f9fcee1b00000006', u'5', u'comment6', d("2016-02-08T19:42:33.589Z", tz_info), 7, '56b8efa9f9fcee1b0000000f', '56b8da51f9fcee1b00000006', d("2016-02-08T19:42:33.589Z", tz_info), 5)]}]
    model = [{'do $$    begin    UPDATE post_comments SET body=(%s), created_at=(%s), id_bsontype=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'comment6',  d("2016-02-08T19:42:33.589Z", tz_info), 7, '56b8efa9f9fcee1b0000000f', d("2016-02-08T19:42:33.589Z", tz_info), '56b8da51f9fcee1b00000006', u'6', u'comment6', d("2016-02-08T19:42:33.589Z", tz_info), 7, '56b8efa9f9fcee1b0000000f', '56b8da51f9fcee1b00000006', d("2016-02-08T19:42:33.589Z", tz_info), 6)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #4')
    #TODO fix insert indexes 5 --> 6
    #FIXED
    # model = [{'do $$    begin    UPDATE test_db.test_schema.post_comments SET body=(%s), created_at=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO test_schema."post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'comment6', d('2016-02-08T19:42:33.589Z', tz_info), '56b8efa9f9fcee1b0000000f', d('2016-02-08T19:42:33.589Z', tz_info), '56b8da51f9fcee1b00000006', u'5', u'comment6', d('2016-02-08T19:42:33.589Z', tz_info), 7, '56b8efa9f9fcee1b0000000f', '56b8da51f9fcee1b00000006', d('2016-02-08T19:42:33.589Z', tz_info), 5)]}]
    model = [{'do $$    begin    UPDATE test_db.test_schema.post_comments SET body=(%s), created_at=(%s), id_bsontype=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO test_schema."post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'comment6', d('2016-02-08T19:42:33.589Z', tz_info), 7, '56b8efa9f9fcee1b0000000f', d('2016-02-08T19:42:33.589Z', tz_info), '56b8da51f9fcee1b00000006', u'6', u'comment6', d('2016-02-08T19:42:33.589Z', tz_info), 7, '56b8efa9f9fcee1b0000000f', '56b8da51f9fcee1b00000006', d('2016-02-08T19:42:33.589Z', tz_info), 6)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, 'test_db', 'test_schema')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #5')
    oplog_data = loads(oplog_u_02)
    model = [{'UPDATE posts SET updated_at=(%s) WHERE id_oid=(%s);': [(d('2016-02-08T19:52:23.883Z', tz_info), '56b8da59f9fcee1b00000007',)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #5.A')
    oplog_data = loads(oplog_u_02_A)
    model = [{'UPDATE rated_posts SET number=(%s) WHERE id_oid=(%s);': [(None, '56b8da59f9fcee1b00000007',)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)


    print('Test #6')
    #TODO fix insert indexes
    oplog_data = loads(oplog_u_03)
    # model = [
    #     {'DELETE FROM post_comments WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
    #     {'DELETE FROM post_comment_tests WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
    #     {u'INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);':
    #          [(None, d("2016-02-08T19:57:56.678Z", tz_info), 7, '56b8f344f9fcee1b00000018', '56b8da59f9fcee1b00000007', d("2016-02-08T19:57:56.678Z", tz_info), 1,)]}]
    model = [
        {'DELETE FROM post_comments WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {'DELETE FROM post_comment_tests WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {u'INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);':
             [(None, d("2016-02-08T19:57:56.678Z", tz_info), 7, '56b8f344f9fcee1b00000018', '56b8da59f9fcee1b00000007', d("2016-02-08T19:57:56.678Z", tz_info), 1,)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #7')
    # TODO fix insert indexes
    oplog_data = loads(oplog_u_03)
    # model = [
    #     {'DELETE FROM database.schema.post_comments WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
    #     {'DELETE FROM database.schema.post_comment_tests WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
    #     {u'INSERT INTO schema."post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);':
    #          [(None, d("2016-02-08T19:57:56.678Z", tz_info), 7, '56b8f344f9fcee1b00000018', '56b8da59f9fcee1b00000007', d("2016-02-08T19:57:56.678Z", tz_info), 1,)]}]
    model = [
        {'DELETE FROM database.schema.post_comments WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {'DELETE FROM database.schema.post_comment_tests WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {u'INSERT INTO schema."post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);':
             [(None, d("2016-02-08T19:57:56.678Z", tz_info), 7, '56b8f344f9fcee1b00000018', '56b8da59f9fcee1b00000007', d("2016-02-08T19:57:56.678Z", tz_info), 1,)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, 'database', 'schema')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #8')
    # TODO fix insert indexes 1 --> 2
    #FIXED
    oplog_data = loads(oplog_u_04)
    # model = [{'do $$    begin    UPDATE post_comments SET body=(%s), created_at=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'commments2222', d('2016-02-08T19:58:06.008Z', tz_info), '56b8f34ef9fcee1b00000019', d('2016-02-08T19:58:06.008Z', tz_info), '56b8da59f9fcee1b00000007', u'1', u'commments2222', d('2016-02-08T19:58:06.008Z', tz_info), 7, '56b8f34ef9fcee1b00000019', '56b8da59f9fcee1b00000007', d('2016-02-08T19:58:06.008Z', tz_info), 1)]}]
    model = [{'do $$    begin    UPDATE post_comments SET body=(%s), created_at=(%s), id_bsontype=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'commments2222', d('2016-02-08T19:58:06.008Z', tz_info), 7, '56b8f34ef9fcee1b00000019', d('2016-02-08T19:58:06.008Z', tz_info), '56b8da59f9fcee1b00000007', u'2', u'commments2222', d('2016-02-08T19:58:06.008Z', tz_info), 7, '56b8f34ef9fcee1b00000019', '56b8da59f9fcee1b00000007', d('2016-02-08T19:58:06.008Z', tz_info), 2)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #9')
    #TODO fix insert indexes 2 -- > 3
    #FIXED
    oplog_data = loads(oplog_u_05)
    # model = [{'do $$    begin    UPDATE post_comments SET created_at=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$':
    #               [(d('2016-02-08T19:58:22.847Z', tz_info), '56b8f35ef9fcee1b0000001a', d('2016-02-08T19:58:22.847Z', tz_info), '56b8da59f9fcee1b00000007', u'2', None, d('2016-02-08T19:58:22.847Z', tz_info), 7, '56b8f35ef9fcee1b0000001a', '56b8da59f9fcee1b00000007', d('2016-02-08T19:58:22.847Z', tz_info), 2)]}]
    model = [{'do $$    begin    UPDATE post_comments SET created_at=(%s), id_bsontype=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$':
                  [(d('2016-02-08T19:58:22.847Z', tz_info), 7, '56b8f35ef9fcee1b0000001a', d('2016-02-08T19:58:22.847Z', tz_info), '56b8da59f9fcee1b00000007', u'3', None, d('2016-02-08T19:58:22.847Z', tz_info), 7, '56b8f35ef9fcee1b0000001a', '56b8da59f9fcee1b00000007', d('2016-02-08T19:58:22.847Z', tz_info), 3)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #10')
    oplog_data = loads(oplog_u_06)
    # model = [
    #     {'DELETE FROM post_comments WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
    #     {'DELETE FROM post_comment_tests WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
    #     {u'INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);':
    #         [(None, d("2016-02-08T19:57:56.678Z", tz_info), 7, '56b8f344f9fcee1b00000018', '56b8da59f9fcee1b00000007', d("2016-02-08T19:57:56.678Z", tz_info), 1,),
    #          (None, d("2016-02-08T19:58:22.847Z", tz_info), 7, '56b8f35ef9fcee1b0000001a', '56b8da59f9fcee1b00000007', d("2016-02-08T19:58:22.847Z", tz_info), 2,)
    #         ]}]
    model = [
        {'DELETE FROM post_comments WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {'DELETE FROM post_comment_tests WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {u'INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);':
            [(None, d("2016-02-08T19:57:56.678Z", tz_info), 7, '56b8f344f9fcee1b00000018', '56b8da59f9fcee1b00000007', d("2016-02-08T19:57:56.678Z", tz_info), 1,),
             (None, d("2016-02-08T19:58:22.847Z", tz_info), 7, '56b8f35ef9fcee1b0000001a', '56b8da59f9fcee1b00000007', d("2016-02-08T19:58:22.847Z", tz_info), 2,)
            ]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #11')
    oplog_data = loads(oplog_u_07)
    # model = [
    #     {'DELETE FROM post_comments WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
    #     {'DELETE FROM post_comment_tests WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
    #     {u'INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);':
    #          [(None, d('2016-02-08T19:57:56.678Z', tz_info), 7, '56b8f344f9fcee1b00000018', '56b8da59f9fcee1b00000007', d('2016-02-08T19:57:56.678Z', tz_info), 1)]}]
    model = [
        {'DELETE FROM post_comments WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {'DELETE FROM post_comment_tests WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {u'INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);':
             [(None, d('2016-02-08T19:57:56.678Z', tz_info), 7, '56b8f344f9fcee1b00000018', '56b8da59f9fcee1b00000007', d('2016-02-08T19:57:56.678Z', tz_info), 1)]}]
    result = update(dbreq, schema_engine[oplog_data['ns'].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #12')
    oplog_data = loads(oplog_u_08)
    model = [{'UPDATE posts SET updated_at=(%s), title=(%s) WHERE id_oid=(%s);': [(d('2016-02-08T20:02:12.985Z', tz_info), 'sada', '56b8f05cf9fcee1b00000010',)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #13')
    oplog_data = loads(test_data_03)
    # model for starting counting indexes from 0
    # model = [{
    #         'do $$    begin    UPDATE rated_post_comment_rates SET rate=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(2, '56b8da59f9fcee1b00000012', u'1', u'2', None, None, None, None, '56b8da59f9fcee1b00000012', None, None, None, None, 1, 2)]
    #     }, {
    #         'do $$    begin    UPDATE rated_post_comments SET updated_at=(%s) WHERE rated_posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comments" ("body", "created_at", "id_bsontype", "id_oid", "rated_posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(d('2016-02-08T19:58:22.847Z', tz_info), '56b8da59f9fcee1b00000012', u'1', None, None, None, None, '56b8da59f9fcee1b00000012', d('2016-02-08T19:58:22.847Z', tz_info), 1)]
    #     }, {
    #         'do $$    begin    UPDATE rated_post_comment_rates SET user_id=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(u'B', '56b8da59f9fcee1b00000012', u'1', u'1', None, None, None, None, '56b8da59f9fcee1b00000012', None, u'B', None, None, 1, 1)]
    #     }
    # ]
    model = [{
            'do $$    begin    UPDATE rated_post_comments SET updated_at=(%s) WHERE rated_posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comments" ("body", "created_at", "id_bsontype", "id_oid", "rated_posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(d('2016-02-08T19:58:22.847Z', tz_info), '56b8da59f9fcee1b00000012', u'2', None, None, None, None, '56b8da59f9fcee1b00000012', d('2016-02-08T19:58:22.847Z', tz_info), 2)]
        }, {
            'do $$    begin    UPDATE rated_post_comment_rates SET rate=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(2, '56b8da59f9fcee1b00000012', u'2', u'3', None, None, None, None, '56b8da59f9fcee1b00000012', None, None, None, None, 2, 3)]
        }, {
            'do $$    begin    UPDATE rated_post_comment_rates SET user_id=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(u'B', '56b8da59f9fcee1b00000012', u'2', u'2', None, None, None, None, '56b8da59f9fcee1b00000012', None, u'B', None, None, 2, 2)]
        }
    ]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    print('result')
    print(sqls_to_dict(result))
    print('model')
    print(sqls_to_dict(model))
    # print(result)
    # print(model)
    # assert result == model
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #14')
    oplog_data = loads(test_data_04)
    # model = [{
    #         'do $$    begin    UPDATE rated_post_comment_rates SET rate=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(67, '56b8da59f9fcee1b00000013', u'1', u'2', None, None, None, None, '56b8da59f9fcee1b00000013', None, None, None, None, 1, 2)]
    #     }, {
    #         'DELETE FROM rated_post_comment_rate_item_rates WHERE (rated_posts_comments_idx=(%s)) and (rated_posts_comments_rates_idx=(%s)) and (rated_posts_id_oid=(%s));' : [(u'1', u'2', '56b8da59f9fcee1b00000013')]
    #     }, {
    #         u'INSERT INTO "rated_post_comment_rate_item_rates" ("created_at", "description", "id_bsontype", "id_oid", "name", "rated_posts_id_oid", "updated_at", "rated_posts_comments_idx", "rated_posts_comments_rates_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);' : [(None, None, 7, '57557e06cf68790000000000', u'Ivan', '56b8da59f9fcee1b00000013', None, 1, 2, 1), (None, None, 7, '57557e06cf68790000000001', u'Susanin', '56b8da59f9fcee1b00000013', None, 1, 2, 2)]
    #     }, {
    #         'do $$    begin    UPDATE rated_post_comment_rates SET user_id=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(u'B', '56b8da59f9fcee1b00000013', u'1', u'1', None, None, None, None, '56b8da59f9fcee1b00000013', None, u'B', None, None, 1, 1)]
    #     }
    # ]
    model = [{
            'do $$    begin    UPDATE rated_post_comment_rates SET rate=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(67, '56b8da59f9fcee1b00000013', u'2', u'3', None, None, None, None, '56b8da59f9fcee1b00000013', None, None, None, None, 2, 3)]
        }, {
            'DELETE FROM rated_post_comment_rate_item_rates WHERE (rated_posts_comments_idx=(%s)) and (rated_posts_comments_rates_idx=(%s)) and (rated_posts_id_oid=(%s));' : [(u'2', u'3', '56b8da59f9fcee1b00000013')]
        }, {
            u'INSERT INTO "rated_post_comment_rate_item_rates" ("created_at", "description", "id_bsontype", "id_oid", "name", "rated_posts_id_oid", "updated_at", "rated_posts_comments_idx", "rated_posts_comments_rates_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);' : [(None, None, 7, '57557e06cf68790000000000', u'Ivan', '56b8da59f9fcee1b00000013', None, 2, 3, 1), (None, None, 7, '57557e06cf68790000000001', u'Susanin', '56b8da59f9fcee1b00000013', None, 2, 3, 2)]
        }, {
            'do $$    begin    UPDATE rated_post_comment_rates SET user_id=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(u'B', '56b8da59f9fcee1b00000013', u'2', u'2', None, None, None, None, '56b8da59f9fcee1b00000013', None, u'B', None, None, 2, 2)]
        }
    ]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #15')
    oplog_data = loads(test_data_06)
    model = [{'UPDATE rated_post_comments SET id_bsontype=(%s), id_oid=(%s) WHERE idx=(%s) and rated_posts_id_oid=(%s);': [(None, None, '3', 503078)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #16')
    oplog_data = loads(test_data_07)
    model = [{'UPDATE rated_post_comment_rates SET user_info_last_name=(%s), user_info_name=(%s) WHERE idx=(%s) and rated_posts_comments_idx=(%s) and rated_posts_id_oid=(%s);': [(None, None, '9', '3', 503078)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #17')
    oplog_data = loads(test_data_08)
    model = [{'DELETE FROM rated_post_comment_rate_item_rates WHERE (rated_posts_comments_idx=(%s)) and (rated_posts_comments_rates_idx=(%s)) and (rated_posts_id_oid=(%s));': [('3', '10', 503078)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #18')
    oplog_data = loads(test_data_09)
    model = [{'do $$    begin    UPDATE rated_post_comment_tests SET tests=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO rated_post_comment_tests (rated_posts_id_oid, rated_posts_comments_idx, idx, tests) VALUES(%s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(24, 503078, '3', '6', 503078, '3', '6', 24)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #18.A')
    oplog_data = loads(test_data_09_A)
    model = [{'do $$    begin    UPDATE rated_post_comment_tests SET tests=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO rated_post_comment_tests (rated_posts_id_oid, rated_posts_comments_idx, idx, tests) VALUES(%s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(None, 503078, '3', '6', 503078, '3', '6', None)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #19')
    oplog_data = loads(test_data_10)
    model = [{'DELETE FROM rated_post_comment_tests WHERE (rated_posts_comments_idx=(%s)) and (rated_posts_id_oid=(%s));': [('3', 503078)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #20')
    oplog_data = loads(test_data_11)
    model = [{'UPDATE rated_posts SET id_bsontype=(%s), id_oid=(%s) WHERE id_oid=(%s);': [(None, None, 503078)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    # oplog_data = loads(test_data_12)
    # model = [{'UPDATE rated_posts SET id_bsontype=(%s), id_oid=(%s) WHERE rated_posts_id_oid=(%s);': [(None, None, 503078)]}]
    # result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    # print(result)
    # assert result == model

    print('Test #21')
    oplog_data = loads(test_data_13)
    model = [
        {
            'UPDATE rated_posts SET body=(%s), title=(%s) WHERE id_oid=(%s);' : [(u'Glory For Heroes', u'Glory For Ukraine', '56b8da59f9fcee1b00000014')]
        }, {
            'DELETE FROM rated_post_rates WHERE (rated_posts_id_oid=(%s));' : [('56b8da59f9fcee1b00000014', )]
        }, {
            u'INSERT INTO "rated_post_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s);' : [(None, 7, u'aaaaaaaaaaaaassssssssssasdas', 555, '56b8da59f9fcee1b00000014', None, u'444444rrwerr34r', 1), (None, 7, u'aaaaaaaaaaaaasasdsadasdasdasd', 7777, '56b8da59f9fcee1b00000014', None, u'987987978979', 2)]
        }
    ]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #22')
    oplog_data = loads(test_data_14)
    model = [{
            'UPDATE rated_posts SET body=(%s), number=(%s) WHERE id_oid=(%s);' : [(u'SOME text', 33, '56b8da59f9fcee1b00000013')]
        }, {
            'do $$    begin    UPDATE rated_post_tests SET tests=(%s) WHERE rated_posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO rated_post_tests (rated_posts_id_oid, idx, tests) VALUES(%s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(456, '56b8da59f9fcee1b00000013', '5', '56b8da59f9fcee1b00000013', '5', 456)]
        }, {
            'DELETE FROM rated_post_comment_rate_item_rates WHERE (rated_posts_comments_idx=(%s)) and (rated_posts_comments_rates_idx=(%s)) and (rated_posts_id_oid=(%s));' : [('2', '3', '56b8da59f9fcee1b00000013')]
        }, {
            u'INSERT INTO "rated_post_comment_rate_item_rates" ("created_at", "description", "id_bsontype", "id_oid", "name", "rated_posts_id_oid", "updated_at", "rated_posts_comments_idx", "rated_posts_comments_rates_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);' : [(None, None, 7, '57557e06cf68790000000000', u'Ivan', '56b8da59f9fcee1b00000013', None, 2, 3, 1), (None, None, 7, '57557e06cf68790000000001', u'Susanin', '56b8da59f9fcee1b00000013', None, 2, 3, 2)]
        }, {
            'do $$    begin    UPDATE rated_post_comment_rates SET user_id=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(u'B', '56b8da59f9fcee1b00000013', '2', '2', None, None, None, None, '56b8da59f9fcee1b00000013', None, u'B', None, None, 2, 2)]
        }, {
            'do $$    begin    UPDATE rated_post_comment_rates SET rate=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO "rated_post_comment_rates" ("created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "user_info_last_name", "user_info_name", "rated_posts_comments_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(67, '56b8da59f9fcee1b00000013', '2', '3', None, None, None, None, '56b8da59f9fcee1b00000013', None, None, None, None, 2, 3)]
        }
    ]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #23')
    oplog_data = loads(test_data_15)
    model = [
        {
            'DELETE FROM rated_post_enclosed_field_array WHERE (rated_posts_id_oid=(%s));' : [('56b8da59f9fcee1b00000013', )]
        },
        {
            'UPDATE rated_posts SET enclosed_field2=(%s), enclosed_field1=(%s), enclosed_id_bsontype=(%s), enclosed_id_oid=(%s) WHERE id_oid=(%s);' : [(300, u'marty mackfly', 7, '57640cb0cf6879b3fcf0d3f6', '56b8da59f9fcee1b00000013')]
        }
    ]

    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #24')
    oplog_data = loads(test_data_16)
    model = []
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #24.A')
    oplog_data = loads(test_data_17)
    model = []
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model


    print(TEST_INFO, 'update', 'PASSED')


def test_schema_part():
    schemas_path = 'test_data/schemas/rails4_mongoid_development'
    schema_engine = get_schema_engines_as_dict(schemas_path)

    tz_info = loads(oplog_tz_info)['tzinfo_obj'].tzinfo
    paths = []
    schema = schema_engine['rated_posts'].schema
    schema_part_print(schema[0], schema[0], [], paths)

    oplog_data = loads(test_data_14)
    print(oplog_data)
    pp.pprint(normalize_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$set'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1]))
    oplog_data = loads(oplog_u_09)
    print(oplog_data)
    pp.pprint(normalize_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$set'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1]))
    oplog_data = loads(test_data_02)
    print(oplog_data)
    pp.pprint(normalize_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$set'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1]))
    oplog_data = loads(test_data_14)
    print(oplog_data)
    pp.pprint(normalize_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$set'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1]))
    oplog_data = loads(test_data_15)
    print(oplog_data)
    pp.pprint(normalize_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$set'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1]))
    oplog_data = loads(test_data_16)
    print(oplog_data)
    pp.pprint(normalize_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$set'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1]))


    # for el in paths:
    #     print(el)
    #     print(get_part_schema(schema, el.split('.')))


def schema_part_print(schema_stable, schema, path, paths):
    if not type(schema) is dict:
        pass
        # print(schema)
        # gen_p = '.'.join(path+[schema])
        # paths.append( gen_p )
        # print(gen_p)
    else:
        for el in schema:
            gen_p = '.'.join(path + [el])
            paths.append( gen_p )
            # print(gen_p)
            if type(schema[el]) is dict:
                schema_part_print(schema_stable, schema[el], path + [el], paths)
            elif type(schema[el]) is list:
                schema_part_print(schema_stable, schema[el][0], path + [el], paths)


pp = pprint.PrettyPrinter(indent=4)


# test_schema_part()

test_get_obj_id()
test_get_obj_id_recursive()
test_update()

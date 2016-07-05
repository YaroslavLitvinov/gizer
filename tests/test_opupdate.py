#!/usb/bin/env python
"""Tests."""

__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"



from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.opupdate import *
from update_test_data import *
import pprint
from bson.json_util import loads
import datetime
import psycopg2
from os import environ
from test_util import sqls_to_dict, sql_pretty_print


TEST_INFO = 'TEST_OPUPDATE'

def database_prepare():
    connstr = environ['TEST_PSQLCONN']
    connector = psycopg2.connect(connstr)
    return connector


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

    schemas_path = 'test_data/schemas/rails4_mongoid_development'
    schema_engine = get_schema_engines_as_dict(schemas_path)

    tz_info = loads(oplog_tz_info)['tzinfo_obj'].tzinfo


    # oplog_data = loads(test_data_19)
    # model = []
    # result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    # print(result)

    # check_print_dictionary(result,model,1)
    # assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #1')
    oplog_data = loads(test_data_05)
    model = [{'do $$    begin    UPDATE rated_post_comment_rates SET user_info_name=(%s) WHERE idx=(%s) and rated_posts_comments_idx=(%s) and rated_posts_id_oid=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO rated_post_comment_rates (idx, rated_posts_comments_idx, rated_posts_id_oid, user_info_name) VALUES( %s,  %s,  %s,  %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'Vasya', '3', '2', '56b8da59f9fcee1b00000014', '3', '2', '56b8da59f9fcee1b00000014', u'Vasya')]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #2')
    oplog_data = loads(test_data_02)
    model = []
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #3')
    oplog_data = loads(oplog_u_01)
    model = [{'do $$    begin    UPDATE post_comments SET body=(%s), created_at=(%s), id_bsontype=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO post_comments (idx, posts_id_oid, body, created_at, id_bsontype, id_oid, updated_at) VALUES( %s,  %s,  %s,  %s,  %s,  %s,  %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$':
                  [(u'comment6', d("2016-02-08T19:42:33.589Z", tz_info), 7, '56b8efa9f9fcee1b0000000f', d("2016-02-08T19:42:33.589Z", tz_info), '56b8da51f9fcee1b00000006', u'6', '6', '56b8da51f9fcee1b00000006', u'comment6', d('2016-02-08T19:42:33.589Z', tz_info), 7, '56b8efa9f9fcee1b0000000f', d('2016-02-08T19:42:33.589Z', tz_info))]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #4')
    model = [{'do $$    begin    UPDATE test_db.test_schema.post_comments SET body=(%s), created_at=(%s), id_bsontype=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO test_db.test_schema.post_comments (idx, posts_id_oid, body, created_at, id_bsontype, id_oid, updated_at) VALUES( %s,  %s,  %s,  %s,  %s,  %s,  %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'comment6', d('2016-02-08T19:42:33.589Z', tz_info), 7, '56b8efa9f9fcee1b0000000f', d('2016-02-08T19:42:33.589Z', tz_info), '56b8da51f9fcee1b00000006', u'6', '6', '56b8da51f9fcee1b00000006', u'comment6', d('2016-02-08T19:42:33.589Z', tz_info), 7, '56b8efa9f9fcee1b0000000f', d('2016-02-08T19:42:33.589Z', tz_info))]}]
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
    oplog_data = loads(oplog_u_03)
    model = [
        {'DELETE FROM post_comments WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {'DELETE FROM post_comment_tests WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {u'INSERT INTO "post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);':
             [(None, d("2016-02-08T19:57:56.678Z", tz_info), 7, '56b8f344f9fcee1b00000018', '56b8da59f9fcee1b00000007', d("2016-02-08T19:57:56.678Z", tz_info), 1,)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #7')
    oplog_data = loads(oplog_u_03)
    model = [
        {'DELETE FROM database.schema.post_comments WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {'DELETE FROM database.schema.post_comment_tests WHERE (posts_id_oid=(%s));': [('56b8da59f9fcee1b00000007',)]},
        {u'INSERT INTO schema."post_comments" ("body", "created_at", "id_bsontype", "id_oid", "posts_id_oid", "updated_at", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s);':
             [(None, d("2016-02-08T19:57:56.678Z", tz_info), 7, '56b8f344f9fcee1b00000018', '56b8da59f9fcee1b00000007', d("2016-02-08T19:57:56.678Z", tz_info), 1,)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, 'database', 'schema')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #8')
    oplog_data = loads(oplog_u_04)
    model = [{'do $$    begin    UPDATE post_comments SET body=(%s), created_at=(%s), id_bsontype=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO post_comments (idx, posts_id_oid, body, created_at, id_bsontype, id_oid, updated_at) VALUES( %s,  %s,  %s,  %s,  %s,  %s,  %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'commments2222', d('2016-02-08T19:58:06.008Z', tz_info), 7, '56b8f34ef9fcee1b00000019', d('2016-02-08T19:58:06.008Z', tz_info), '56b8da59f9fcee1b00000007', u'2', '2', '56b8da59f9fcee1b00000007', u'commments2222', d('2016-02-08T19:58:06.008Z', tz_info), 7, '56b8f34ef9fcee1b00000019', d('2016-02-08T19:58:06.008Z', tz_info)) ]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #9')
    oplog_data = loads(oplog_u_05)
    model = [{'do $$    begin    UPDATE post_comments SET created_at=(%s), id_bsontype=(%s), id_oid=(%s), updated_at=(%s) WHERE posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO post_comments (idx, posts_id_oid, created_at, id_bsontype, id_oid, updated_at) VALUES( %s,  %s,  %s,  %s,  %s,  %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$':
                  [(d('2016-02-08T19:58:22.847Z', tz_info), 7, '56b8f35ef9fcee1b0000001a', d('2016-02-08T19:58:22.847Z', tz_info), '56b8da59f9fcee1b00000007', u'3', '3', '56b8da59f9fcee1b00000007', d('2016-02-08T19:58:22.847Z', tz_info), 7, '56b8f35ef9fcee1b0000001a', d('2016-02-08T19:58:22.847Z', tz_info))]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #10')
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

    print('Test #11')
    oplog_data = loads(oplog_u_07)
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
    model = [{
            'do $$    begin    UPDATE rated_post_comments SET updated_at=(%s) WHERE rated_posts_id_oid=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO rated_post_comments (idx, rated_posts_id_oid, updated_at) VALUES( %s,  %s,  %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(d('2016-02-08T19:58:22.847Z', tz_info), '56b8da59f9fcee1b00000012', u'2', u'2', '56b8da59f9fcee1b00000012', d('2016-02-08T19:58:22.847Z', tz_info))]
        }, {
            'do $$    begin    UPDATE rated_post_comment_rates SET rate=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO rated_post_comment_rates (idx, rated_posts_comments_idx, rated_posts_id_oid, rate) VALUES( %s,  %s,  %s,  %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(2, '56b8da59f9fcee1b00000012', u'2', u'3', u'3', u'2', '56b8da59f9fcee1b00000012', 2)]
        }, {
            'do $$    begin    UPDATE rated_post_comment_rates SET user_id=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO rated_post_comment_rates (idx, rated_posts_comments_idx, rated_posts_id_oid, user_id) VALUES( %s,  %s,  %s,  %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(u'B', '56b8da59f9fcee1b00000012', u'2', u'2', '2', '2', '56b8da59f9fcee1b00000012', u'B')]
        }
    ]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #14')
    oplog_data = loads(test_data_04)
    model = [
        {
            'DELETE FROM rated_post_comment_rate_item_rates WHERE (rated_posts_comments_idx=(%s)) and (rated_posts_comments_rates_idx=(%s)) and (rated_posts_id_oid=(%s));' : [(u'2', u'3', '56b8da59f9fcee1b00000013')]
        }, {
            u'INSERT INTO "rated_post_comment_rate_item_rates" ("created_at", "description", "id_bsontype", "id_oid", "name", "rated_posts_id_oid", "updated_at", "rated_posts_comments_idx", "rated_posts_comments_rates_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);' : [(None, None, 7, '57557e06cf68790000000000', u'Ivan', '56b8da59f9fcee1b00000013', None, 2, 3, 1), (None, None, 7, '57557e06cf68790000000001', u'Susanin', '56b8da59f9fcee1b00000013', None, 2, 3, 2)]
        }, {
            'do $$    begin    UPDATE rated_post_comment_rates SET rate=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO rated_post_comment_rates (idx, rated_posts_comments_idx, rated_posts_id_oid, rate) VALUES( %s,  %s,  %s,  %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(67, '56b8da59f9fcee1b00000013', u'2', u'3', 3, 2, '56b8da59f9fcee1b00000013', '67')]
        }, {
            'do $$    begin    UPDATE rated_post_comment_rates SET user_id=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO rated_post_comment_rates (idx, rated_posts_comments_idx, rated_posts_id_oid, user_id) VALUES( %s,  %s,  %s,  %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(u'B', '56b8da59f9fcee1b00000013', u'2', u'2', 2, 2, '56b8da59f9fcee1b00000013', 'B')]
        }
    ]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)


    print('Test #15')
    oplog_data = loads(test_data_06)
    model = [{'UPDATE rated_post_comments SET id_bsontype=(%s), id_oid=(%s) WHERE idx=(%s) and rated_posts_id_oid=(%s);': [(None, None, '3', '56b8da59f9fcee1b00000014')]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #16')
    oplog_data = loads(test_data_07)
    model = [{'UPDATE rated_post_comment_rates SET user_info_last_name=(%s), user_info_name=(%s) WHERE idx=(%s) and rated_posts_comments_idx=(%s) and rated_posts_id_oid=(%s);': [(None, None, '9', '3', '56b8da59f9fcee1b00000015')]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #16.A')
    oplog_data = loads(test_data_06_A)
    model = [
        {'DELETE FROM rated_post_enclosed_field_array WHERE (rated_posts_id_oid=(%s));': [('56b8da59f9fcee1b00000015',)]},
        {'UPDATE rated_posts SET enclosed_field1=(%s), enclosed_field2=(%s), enclosed_field3=(%s), enclosed_id_bsontype=(%s), enclosed_id_oid=(%s) WHERE id_oid=(%s);': [(None, None, None, None, None, '56b8da59f9fcee1b00000015')]}
    ]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model


    print('Test #17')
    oplog_data = loads(test_data_08)
    model = [{'DELETE FROM rated_post_comment_rate_item_rates WHERE (rated_posts_comments_idx=(%s)) and (rated_posts_comments_rates_idx=(%s)) and (rated_posts_id_oid=(%s));': [('3', '10', '56b8da59f9fcee1b00000015')]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #18')
    oplog_data = loads(test_data_09)
    model = [{'do $$    begin    UPDATE rated_post_comment_tests SET tests=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO rated_post_comment_tests (rated_posts_id_oid, rated_posts_comments_idx, idx, tests) VALUES(%s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(24, '56b8da59f9fcee1b00000013', '3', '6', '56b8da59f9fcee1b00000013', '3', '6', 24)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #18.A')
    oplog_data = loads(test_data_09_A)
    model = [{'do $$    begin    UPDATE rated_post_comment_tests SET tests=(%s) WHERE rated_posts_id_oid=(%s) and rated_posts_comments_idx=(%s) and idx=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO rated_post_comment_tests (rated_posts_id_oid, rated_posts_comments_idx, idx, tests) VALUES(%s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(None, '56b8da59f9fcee1b00000013', '3', '6', '56b8da59f9fcee1b00000013', '3', '6', None)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

    print('Test #19')
    oplog_data = loads(test_data_10)
    model = [{'DELETE FROM rated_post_comment_tests WHERE (rated_posts_comments_idx=(%s)) and (rated_posts_id_oid=(%s));': [('3', '56b8da59f9fcee1b00000015')]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #20')
    oplog_data = loads(test_data_11)
    model = [{'UPDATE rated_posts SET id_bsontype=(%s), id_oid=(%s) WHERE id_oid=(%s);': [(None, None, '56b8da59f9fcee1b00000015')]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model


    print('Test #20.A')
    oplog_data = loads(test_data_12)
    model = [{'DELETE FROM rated_post_tests WHERE (rated_posts_id_oid=(%s));': [('111111111111111111111111',)]}]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #21')
    oplog_data = loads(test_data_13)
    model = [
        {'DELETE FROM rated_post_rates WHERE (rated_posts_id_oid=(%s));' : [('56b8da59f9fcee1b00000014', )]},
        {u'INSERT INTO "rated_post_rates" ("a_filed_with_id_bsontype", "a_filed_with_id_oid", "another_filed_with_id2_onemore_enclosed_level_bsontype", "another_filed_with_id2_onemore_enclosed_level_oid", "another_filed_with_id2_some_strange_field", "created_at", "id_bsontype", "id_oid", "rate", "rated_posts_id_oid", "updated_at", "user_id", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);': [(None, None, None, None, None, None, 7, u'aaaaaaaaaaaaassssssssssasdas', 555, '56b8da59f9fcee1b00000014', None, u'444444rrwerr34r', 1), (None, None, None, None, None, None, 7, u'aaaaaaaaaaaaasasdsadasdasdasd', 7777, '56b8da59f9fcee1b00000014', None, u'987987978979', 2)]},
        {'UPDATE rated_posts SET body=(%s), title=(%s) WHERE id_oid=(%s);' : [(u'Glory For Heroes', u'Glory For Ukraine', '56b8da59f9fcee1b00000014')]}
    ]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model

    print('Test #22')
    oplog_data = loads(test_data_14)
    model = [
        {'DELETE FROM rated_post_tests WHERE (rated_posts_id_oid=(%s));': [('56b8da59f9fcee1b00000013',)]},
        {u'INSERT INTO "rated_post_tests" ("rated_posts_id_oid", "tests", "idx") VALUES(%s, %s, %s);': [('56b8da59f9fcee1b00000013', 123, 1), ('56b8da59f9fcee1b00000013', 4, 2), ('56b8da59f9fcee1b00000013', 8, 3)]},
        {'DELETE FROM rated_post_enclosed_field_array WHERE (rated_posts_id_oid=(%s));': [('56b8da59f9fcee1b00000013',)]},
        {u'INSERT INTO "rated_post_enclosed_field_array" ("field_array", "rated_posts_id_oid", "idx") VALUES(%s, %s, %s);': [(u'234', '56b8da59f9fcee1b00000013', 1), (u'ertret', '56b8da59f9fcee1b00000013', 2)]},
        {'UPDATE rated_posts SET body=(%s), number=(%s) WHERE id_oid=(%s);': [(u'SOME text', 33, '56b8da59f9fcee1b00000013')]},
        {'DELETE FROM rated_post_comment_rate_item_rates WHERE (rated_posts_comments_idx=(%s)) and (rated_posts_comments_rates_idx=(%s)) and (rated_posts_id_oid=(%s));': [('2', '3', '56b8da59f9fcee1b00000013')]},
        {u'INSERT INTO "rated_post_comment_rate_item_rates" ("created_at", "description", "id_bsontype", "id_oid", "name", "rated_posts_id_oid", "updated_at", "rated_posts_comments_idx", "rated_posts_comments_rates_idx", "idx") VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);': [(None, None, 7, '57557e06cf68790000000000', u'Ivan', '56b8da59f9fcee1b00000013', None, 2, 3, 1), (None, None, 7, '57557e06cf68790000000001', u'Susanin', '56b8da59f9fcee1b00000013', None, 2, 3, 2)]},
        {'do $$    begin    UPDATE rated_post_comment_rates SET rate=(%s) WHERE idx=(%s) and rated_posts_comments_idx=(%s) and rated_posts_id_oid=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO rated_post_comment_rates (idx, rated_posts_comments_idx, rated_posts_id_oid, rate) VALUES(%s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$' : [(67, '3', '2', '56b8da59f9fcee1b00000013', '3', '2', '56b8da59f9fcee1b00000013', 67)]},
        {'do $$    begin    UPDATE rated_post_comment_rates SET user_id=(%s) WHERE idx=(%s) and rated_posts_comments_idx=(%s) and rated_posts_id_oid=(%s);    IF FOUND THEN        RETURN;    END IF;    BEGIN        INSERT INTO rated_post_comment_rates (idx, rated_posts_comments_idx, rated_posts_id_oid, user_id) VALUES(%s, %s, %s, %s);        RETURN;    EXCEPTION WHEN unique_violation THEN    END;    end    $$': [(u'B', '2', '2', '56b8da59f9fcee1b00000013', '2', '2', '56b8da59f9fcee1b00000013', u'B')]}
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
    assert sqls_to_dict(result) == sqls_to_dict(model)


    print('Test #23.A')
    oplog_data = loads(test_data_15_A)
    model = [
        {
            'DELETE FROM rated_post_enclosed_field_array WHERE (rated_posts_id_oid=(%s));' : [('56b8da59f9fcee1b00000013', )]
        },
        {
            'UPDATE rated_posts SET enclosed_field2=(%s), enclosed_field1=(%s), enclosed_id_bsontype=(%s), enclosed_id_oid=(%s) WHERE id_oid=(%s);' : [(300, u'marty mackfly', 7, '57640cb0cf6879b3fcf0d3f6', '56b8da59f9fcee1b00000013')]
        }
    ]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert sqls_to_dict(result) == sqls_to_dict(model)

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

    print('Test #25')
    oplog_data = loads(test_data_08_all)
    model = [
        {'UPDATE rated_post_comments SET id_bsontype=(%s), id_oid=(%s) WHERE idx=(%s) and rated_posts_id_oid=(%s);': [(None, None, '3', '56b8da59f9fcee1b00000015')]},
        {'DELETE FROM rated_post_enclosed_field_array WHERE (rated_posts_id_oid=(%s));': [('56b8da59f9fcee1b00000015',)]},
        {'UPDATE rated_posts SET enclosed_field1=(%s), enclosed_field2=(%s), enclosed_field3=(%s), enclosed_id_bsontype=(%s), enclosed_id_oid=(%s) WHERE id_oid=(%s);': [(None, None, None, None, None, '56b8da59f9fcee1b00000015')]},
        {'DELETE FROM rated_post_comment_rate_item_rates WHERE (rated_posts_comments_idx=(%s)) and (rated_posts_comments_rates_idx=(%s)) and (rated_posts_id_oid=(%s));': [('3', '10', '56b8da59f9fcee1b00000015')]},
        {'UPDATE rated_post_comment_rates SET user_info_last_name=(%s), user_info_name=(%s) WHERE idx=(%s) and rated_posts_comments_idx=(%s) and rated_posts_id_oid=(%s);': [(None, None, '9', '3', '56b8da59f9fcee1b00000015')]}
    ]
    result = update(dbreq, schema_engine[oplog_data["ns"].split('.')[1]], oplog_data, '', '')
    assert result == model


    print(TEST_INFO, 'update', 'PASSED')


def test_oplog_normalization():
    schemas_path = 'test_data/schemas/rails4_mongoid_development'
    schema_engine = get_schema_engines_as_dict(schemas_path)

    oplog_data = loads(test_data_14)
    model = [
        OplogBranch(oplog_path='', normalized_path=u'body', data=u'SOME text', conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000013'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'body'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'tests', data=[123, 4, 8], conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000013'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'tests'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'comments.1.rates.2.item_rates', data=[{u'_id': bson.objectid.ObjectId('57557e06cf68790000000000'), u'name': u'Ivan'}, {u'_id': bson.objectid.ObjectId('57557e06cf68790000000001'), u'name': u'Susanin'}], conditions_list={'target': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013', u'rated_posts_comments_idx': '2', 'idx': '3'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013', u'rated_posts_comments_idx': '2', u'rated_posts_comments_rates_idx': '3'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts.comments.1.rates.2', column=u'item_rates'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'comments.1.rates.1.user_id', data=u'B', conditions_list={'target': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013', u'rated_posts_comments_idx': '2', 'idx': '2'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013', u'rated_posts_comments_idx': '2', u'rated_posts_comments_rates_idx': '2'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts.comments.1.rates.1', column=u'user_id'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'number', data=33, conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000013'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'number'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed.field_array', data=[u'234', u'ertret'], conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000013'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed.field_array'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'comments.1.rates.2.rate', data=67, conditions_list={'target': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013', u'rated_posts_comments_idx': '2', 'idx': '3'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013', u'rated_posts_comments_idx': '2', u'rated_posts_comments_rates_idx': '3'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts.comments.1.rates.2', column=u'rate'), object_id_field=None)
    ]
    result = normalize_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$set'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1])
    assert result == model

    oplog_data = loads(oplog_u_09)
    model = [
        OplogBranch(oplog_path='', normalized_path=u'comments.1.updated_at', data=u'2016-02-08T19:57:56.678Z', conditions_list={'target': {u'posts_id_oid': '56b8da59f9fcee1b00000007', 'idx': '2'}, 'child': {u'posts_id_oid': '56b8da59f9fcee1b00000007', u'posts_comments_idx': '2'}}, parsed_path=ParsedObjPath(table_path=u'posts.comments.1', column=u'updated_at'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'comments.1.created_at', data=u'2016-04-08T19:57:56.678Z', conditions_list={'target': {u'posts_id_oid': '56b8da59f9fcee1b00000007', 'idx': '2'}, 'child': {u'posts_id_oid': '56b8da59f9fcee1b00000007', u'posts_comments_idx': '2'}}, parsed_path=ParsedObjPath(table_path=u'posts.comments.1', column=u'created_at'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'comments.1._id.oid', data='56b8f344f9fcee1b00000018', conditions_list={'target': {u'posts_id_oid': '56b8da59f9fcee1b00000007', 'idx': '2'}, 'child': {u'posts_id_oid': '56b8da59f9fcee1b00000007', u'posts_comments_idx': '2'}}, parsed_path=ParsedObjPath(table_path=u'posts.comments.1', column=u'_id.oid'), object_id_field=bson.objectid.ObjectId('56b8f344f9fcee1b00000018')),
        OplogBranch(oplog_path='', normalized_path=u'comments.1._id.bsontype', data=7, conditions_list={'target': {u'posts_id_oid': '56b8da59f9fcee1b00000007', 'idx': '2'}, 'child': {u'posts_id_oid': '56b8da59f9fcee1b00000007', u'posts_comments_idx': '2'}}, parsed_path=ParsedObjPath(table_path=u'posts.comments.1', column=u'_id.bsontype'), object_id_field=None)
    ]
    result = normalize_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$set'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1])
    assert result == model


    oplog_data = loads(test_data_02)
    model = []
    result = normalize_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$set'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1])
    assert result == model

    oplog_data = loads(test_data_06)
    model = [
        OplogBranch(oplog_path='', normalized_path=u'comments.2._id.oid', data=None, conditions_list={'target': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000014', 'idx': '3'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000014', u'rated_posts_comments_idx': '3'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts.comments.2', column=u'_id.oid'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'comments.2._id.bsontype', data=None, conditions_list={'target': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000014', 'idx': '3'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000014', u'rated_posts_comments_idx': '3'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts.comments.2', column=u'_id.bsontype'), object_id_field=None)
    ]
    result = normalize_unset_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$unset'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1])
    assert result == model


    oplog_data = loads(test_data_06_A)
    model = [
        OplogBranch(oplog_path='', normalized_path=u'enclosed.field2', data=None, conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000015'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed.field2'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed.field3', data=None, conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000015'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed.field3'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed.field1', data=None, conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000015'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed.field1'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed.field_array', data=[], conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000015'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed.field_array'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed._id.oid', data=None, conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000015'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed._id.oid'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed._id.bsontype', data=None, conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000015'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed._id.bsontype'), object_id_field=None)
    ]
    result = normalize_unset_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$unset'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1])
    assert result == model

    oplog_data = loads(test_data_07)
    model = [
        OplogBranch(oplog_path='', normalized_path=u'comments.2.rates.8.user_info.last_name', data=None, conditions_list={'target': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015', u'rated_posts_comments_idx': '3', 'idx': '9'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015', u'rated_posts_comments_idx': '3', u'rated_posts_comments_rates_idx': '9'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts.comments.2.rates.8', column=u'user_info.last_name'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'comments.2.rates.8.user_info.name', data=None, conditions_list={'target': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015', u'rated_posts_comments_idx': '3', 'idx': '9'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015', u'rated_posts_comments_idx': '3', u'rated_posts_comments_rates_idx': '9'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts.comments.2.rates.8', column=u'user_info.name'), object_id_field=None)
    ]
    result = normalize_unset_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$unset'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1])
    assert result == model


    oplog_data = loads(test_data_08_all)
    model = [
        OplogBranch(oplog_path='', normalized_path=u'comments.2._id.oid', data=None, conditions_list={'target': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015', 'idx': '3'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015', u'rated_posts_comments_idx': '3'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts.comments.2', column=u'_id.oid'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'comments.2._id.bsontype', data=None, conditions_list={'target': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015', 'idx': '3'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015', u'rated_posts_comments_idx': '3'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts.comments.2', column=u'_id.bsontype'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'comments.2.rates.9.item_rates', data=[], conditions_list={'target': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015', u'rated_posts_comments_idx': '3', 'idx': '10'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015', u'rated_posts_comments_idx': '3', u'rated_posts_comments_rates_idx': '10'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts.comments.2.rates.9', column=u'item_rates'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'comments.2.rates.8.user_info.last_name', data=None, conditions_list={'target': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015', u'rated_posts_comments_idx': '3', 'idx': '9'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015', u'rated_posts_comments_idx': '3', u'rated_posts_comments_rates_idx': '9'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts.comments.2.rates.8', column=u'user_info.last_name'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'comments.2.rates.8.user_info.name', data=None, conditions_list={'target': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015', u'rated_posts_comments_idx': '3', 'idx': '9'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015', u'rated_posts_comments_idx': '3', u'rated_posts_comments_rates_idx': '9'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts.comments.2.rates.8', column=u'user_info.name'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed.field2', data=None, conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000015'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed.field2'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed.field3', data=None, conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000015'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed.field3'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed.field1', data=None, conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000015'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed.field1'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed.field_array', data=[], conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000015'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed.field_array'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed._id.oid', data=None, conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000015'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed._id.oid'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed._id.bsontype', data=None, conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000015'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000015'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed._id.bsontype'), object_id_field=None)
    ]
    result = normalize_unset_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$unset'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1])
    assert result == model


    oplog_data = loads(test_data_15)
    model = [
        OplogBranch(oplog_path='', normalized_path=u'enclosed.field2', data=300, conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000013'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed.field2'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed.field1', data=u'marty mackfly', conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000013'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed.field1'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed._id.oid', data='57640cb0cf6879b3fcf0d3f6', conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000013'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed._id.oid'), object_id_field=bson.objectid.ObjectId('57640cb0cf6879b3fcf0d3f6')),
        OplogBranch(oplog_path='', normalized_path=u'enclosed._id.bsontype', data=7, conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000013'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed._id.bsontype'), object_id_field=None),
        OplogBranch(oplog_path='', normalized_path=u'enclosed.field_array', data=[], conditions_list={'target': {u'id_oid': '56b8da59f9fcee1b00000013'}, 'child': {u'rated_posts_id_oid': '56b8da59f9fcee1b00000013'}}, parsed_path=ParsedObjPath(table_path=u'rated_posts', column=u'enclosed.field_array'), object_id_field=None)
    ]
    result = normalize_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$set'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1])
    assert result == model

    oplog_data = loads(test_data_16)
    model = []
    result = normalize_oplog_recursive(schema_engine[oplog_data["ns"].split('.')[1]].schema, oplog_data['o']['$set'],[],[], get_obj_id(oplog_data), oplog_data["ns"].split('.')[1])
    assert result == model
    print(TEST_INFO, 'oplog normalization', 'PASSED')


def check_print_dictionary(result, model, print_input=0):
    if result != []:
        d_res = sqls_to_dict(result)
    else:
        d_res = []
    if model != []:
        d_mod = sqls_to_dict(model)
    else:
        d_mod = []
    if print_input:
        print('\ninput data')
        print('result')
        for el in sorted(result):
            if type(result) is dict:
                print(el)
                print(result[el])
            else:
                print(el)
        print('\nmodel')
        for el in sorted(model):
            if type(model) is dict:
                print(el)
                print(model[el])
            else:
                print(el)

    print('\nparsed data')
    print('result')
    for el in sorted(d_res):
        print(el)
        print(d_res[el])
    print('\nmodel')
    for el in sorted(d_mod):
        print(el)
        print(d_mod[el])

def sorted_noramlized_oplod_list(n_list):
    return sorted(n_list, key=lambda x: x.normalized_path)

def schema_part_paths(schema, path, paths):
    for el in schema:
        gen_p = '.'.join(path + [el])
        if type(schema[el]) is dict:
            schema_part_paths(schema[el], path + [el], paths)
        elif type(schema[el]) is list:
            paths.append({gen_p:[]})
        else:
            paths.append({gen_p:None})
    return paths

pp = pprint.PrettyPrinter(indent=4)


# test_schema_part()
test_get_obj_id()
test_oplog_normalization()
test_update()

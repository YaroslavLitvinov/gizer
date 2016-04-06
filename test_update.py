#!/usb/bin/env python
"""Tests."""
from mongo_to_hive_mapping import schema_engine

from update import *
import textwrap
from mongo_to_hive_mapping.schema_engine import *
from gizer.opinsert import *
from update_test_data import *

TEST_INFO = 'TEST_OPUPDATE'

# payload1 = {
#     "$set": {
#       "comments.5": {
#         "_id": { "$oid": "56b8efa9f9fcee1b0000000f" },
#         "body": "comment6",
#         "updated_at": "2016-02-08T19:42:33.589Z",
#         "created_at": "2016-02-08T19:42:33.589Z"
#       }
#     }
#   }
#
# 'UPDATE post_comments SET id_oid="56b8efa9f9fcee1b0000000f", body="comment6", updated_at="2016-02-08T19:42:33.589Z", created_at="2016-02-08T19:42:33.589Z" WHERE posts_id="00aabbccddeeff" and idx=5'
#
#
# # payload1 = {
# #     "$set": {
# #         "comments.5": {
# #             "_id": "56b8efa9f9fcee1b0000000f",
# #             "body": "comment6",
# #             "updated_at": "2016-02-08T19:42:33.589Z",
# #             "created_at": "2016-02-08T19:42:33.589Z",
# #         }
# #     }
# # }
#
# payload2 = {
#     "$set": {
#         "updated_at": "2016-02-08T19:52:23.883Z"
#     }
# }
#
# payload3 = {
#     "$set": {
#         "comments": [
#             {
#                 "_id": {
#                     "oid":"56b8f344f9fcee1b00000018",
#                     "bsontype":4
#                         },
#                 "updated_at": "2016-02-08T19:57:56.678Z",
#                 "created_at": "2016-02-08T19:57:56.678Z"
#             },
#             {
#                 "_id": {
#                     "oid":"56b8f35ef9fcee1b0000001a",
#                     "bsontype":6
#                 },
#                 "updated_at": "2016-02-08T19:58:22.847Z",
#                 "created_at": "2016-02-08T19:58:22.847Z"
#             }
#         ]
#     }
# }
#
# payload4 = {
#     "$set": {
#       "comments": [
#         {
#           "_id": "56b8f344f9fcee1b00000018",
#           "updated_at": "2016-02-08T19:57:56.678Z",
#           "created_at": "2016-02-08T19:57:56.678Z"
#         },
#         {
#           "_id": "56b8f35ef9fcee1b0000001a",
# 		  "users":[
# 			{
# 				"account":"00111210123sdf",
# 				"name":"Vladimir Vladimirovich Huilo",
# 				"dateofbirth":"01.01.1900"
# 			},
# 			{
# 				"account":"00111265465465",
# 				"name":"Potap i Nastya",
# 				"dateofbirth":"01.01.1800"
# 			},
# 		  ],
#           "updated_at": "2016-02-08T19:58:22.847Z",
#           "created_at": "2016-02-08T19:58:22.847Z"
#         }
#       ]
#     }
#   }
#
# payload5 = {
#     "$set": {
#         "addresses.0.streets.7": {
#             "name": "STREETNAME"
#         }
#     }
# }
#
#
'update post_adresse_streets set name="STREETNAME" where post_id="aabbccddeeff" and post_adresses_idx=0, idx=7'


def test_get_obj_id():
    oplog_data = oplog_u_01
    model = {"id_oid":"56b8da51f9fcee1b00000006"}
    result = get_obj_id(oplog_data)
    assert result == model
    print(TEST_INFO, 'get_obj_id', 'PASSED')


def test_get_obj_id_recursive():
    model = {'aaa_bbb_ccc_ddd_eeee': 'abcdef'}
    assert model == get_obj_id_recursive(test_data_01, [], [])
    print(TEST_INFO, 'get_obj_id_recursive', 'PASSED')


def test_update_new():
    print('######################')
    print(update_new(schema, oplog_u_01))
    print('######################')
    print(update_new(schema, oplog_u_02))
    print('######################')
    print(update_new(schema, oplog_u_03))
    print('######################')
    print(update_new(schema, oplog_u_04))
    print('######################')
    print(update_new(schema, oplog_u_05))
    print('######################')
    print(update_new(schema, oplog_u_06))
    print('######################')
    print(update_new(schema, oplog_u_07))
    print('######################')
    print(update_new(schema, oplog_u_08))



def test_update():
    """Test update callback."""
    ns = 'posts'
    #print (list(update.update(ns, payload2, schema)))

    print( (update(ns, schema, oplog_u_02["o"], '001122334455667788')))
    print( (update(ns, schema, oplog_u_04["o"], '001122334455667788')))
    print( (update(ns, schema, oplog_u_05["o"], 'aabbccddeeff')))

    # tables = schema_engine.create_tables_load_bson_data(SchemaEngine(ns, schema), payload1['$set'])
    # print(tables.data)
    # print(tables.tables.keys())
    # print(generate_insert_queries(tables.tables['posts_comments']))
    # print( tables )



#     assert list(update.update(ns, schema, payload3, '001122334455667788')) == [
#         #'update users set updated_at = %s where users._id = %s',
#         'LOOP\n\
#     update users set updated_at = %s where users._id = %s\n\
#     IF found THEN\n\
#         RETURN;\n\
#     END IF;\n\
#     BEGIN\n\
#         \n\n\
#         RETURN;\n\
#     EXCEPTION WHEN unique_violation THEN\n\
#     END;\n\
# END LOOP;\n'
#     ]
#     assert list(update.update(ns, tables.tables['users'])) == [
#         ''
#     ]


def test_extract_query_objects_from():
    """Test query-objects generator."""
    assert list(update.extract_query_objects_from(payload1, [])) == \
        [(["$set", "posts.3.comments.5"], {
            "_id": "56b8efa9f9fcee1b0000000f",
            "body": "comment6",
            "updated_at": "2016-02-08T19:42:33.589Z",
            "created_at": "2016-02-08T19:42:33.589Z",
        })]

    assert list(update.extract_query_objects_from(payload2, [])) == \
        [(["$set"], {
            "updated_at": "2016-02-08T19:52:23.883Z",
        })]

    assert list(update.extract_query_objects_from(payload3, [])) == \
        [(["$set", "comments"], [
            {
                "_id": "56b8f344f9fcee1b00000018",
                "updated_at": "2016-02-08T19:57:56.678Z",
                "created_at": "2016-02-08T19:57:56.678Z"
            },
            {
                "_id": "56b8f35ef9fcee1b0000001a",
                "updated_at": "2016-02-08T19:58:22.847Z",
                "created_at": "2016-02-08T19:58:22.847Z"
            }
        ])]


def test_generate_id_select_query():
    """Test id retrieval generation."""
    assert update.generate_id_select_query('users', 'posts.3.comments.5') == \
        textwrap.dedent('''\
            select users_posts_comments._id from users_posts_comments
            where users_posts_comments.user_id = %s and
            users_posts_comments.users_idx = %s and
            users_posts_comments.users_posts_idx = %s''').replace('\n', ' ')


def test_generate_update_query():
    """Test update query generation."""
    payload1 = {
        "_id": "56b8efa9f9fcee1b0000000f",
        "body": "comment6",
        "updated_at": "2016-02-08T19:42:33.589Z",
        "created_at": "2016-02-08T19:42:33.589Z"
    }
    payload2 = {
        "updated_at": "2016-02-08T19:52:23.883Z"
    }

    payload4 = {
        "name": "STREETNAME"
    }
    assert update.generate_update_query('users', 'posts.3.comments.5', payload1) == \
        textwrap.dedent(
        """\
        update users_posts_comments
         set body = %s,
         created_at = %s,
         _id = %s,
         updated_at = %s
         where
         users_posts_comments.user_id = %s and
         users_posts_comments.users_idx = %s and
         users_posts_comments.users_posts_idx = %s
         """).replace('\n', '')

    assert update.generate_update_query('users', '', payload2) == \
        textwrap.dedent(
        """\
        update users
         set updated_at = %s
         where
         users._id = %s""").replace('\n', '')

    assert update.generate_update_query('users', 'address.street', payload4) == \
        textwrap.dedent(
        """\
        update users
         set address_street_name = %s
         where
         users._id = %s""").replace('\n', '')

#test_update()
test_get_obj_id()
test_get_obj_id_recursive()
test_update_new()
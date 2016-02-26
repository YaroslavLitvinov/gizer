#!/usb/bin/env python
"""Tests."""

import update
import textwrap


payload1 = {
    "$set": {
        "posts.3.comments.5": {
            "_id": "56b8efa9f9fcee1b0000000f",
            "body": "comment6",
            "updated_at": "2016-02-08T19:42:33.589Z",
            "created_at": "2016-02-08T19:42:33.589Z",
        }
    }
}

payload2 = {
    "$set": {
        "updated_at": "2016-02-08T19:52:23.883Z"
    }
}

payload3 = {
    "$set": {
        "comments": [
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
        ]
    }
}

payload4 = {
    "$set": {
        "address.0.street": {
            "name": "STREETNAME"
        }
    }
}

schema = {
    "comments": [{
        "_id": {
            "oid": "STRING",
            "bsontype": "INT"
        },
        "body": "STRING",
        "updated_at": "TIMESTAMP",
        "created_at": "TIMESTAMP"
    }],
    "title": "STRING",
    "body": "STRING",
    "user_id": "STRING",
    "updated_at": "TIMESTAMP",
    "created_at": "TIMESTAMP",
    "_id": {
        "oid": "STRING",
        "bsontype": "INT"
    }
}


def test_update():
    """Test update callback."""
    ns = 'users'
    assert list(update.update(ns, payload2, schema)) == [
        'update users set updated_at = %s where users._id = %s'
    ]
    assert list(update.update(ns, payload1, schema)) == [
        ''
    ]


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

#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from bson.json_util import loads
from gizer.all_schema_engines import get_schema_files, get_schema_engines_as_dict
from mongo_reader.prepare_mongo_request import prepare_mongo_request
from mongo_schema.schema_engine import SchemaEngine

custom_schema_object_id_as_string = '{\
    "id": "STRING",\
    "comment": "STRING"\
}'


def test_prepare_request_with_id_as_object_id_or_int():
    dbname = 'rails4_mongoid_development'
    db_schemas_path = '/'.join(['test_data', 'schemas', dbname])
    schemas = get_schema_engines_as_dict(db_schemas_path)

    id_val = loads('{ "$oid": "56b8f05cf9fcee1b00000010" }')
    req = prepare_mongo_request(schemas['posts'], id_val)
    print req
    assert(req == {"_id":id_val})

    id_val2 = 22
    req2 = prepare_mongo_request(schemas['guests'], id_val2)
    print req2
    assert(req2 == {'id': 22})
    #assert(files==['guests.js', 'posts.js', 'posts2.js', 'rated_posts.js'])


def test_prepare_request_with_id_as_string():
    collection_name = 'foo'
    schema = [loads(custom_schema_object_id_as_string)]
    schema_engine = SchemaEngine(collection_name, schema)
    
    id_val = "hello_id"
    req = prepare_mongo_request(schema_engine, id_val)
    print req
    assert(req == {'id': 'hello_id'})


if __name__=='__main__':
    test_prepare_request_with_id_as_object_id_or_int()
    test_prepare_request_with_id_as_string()

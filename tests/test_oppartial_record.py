#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import bson
from bson.json_util import loads
#from gizer.oppartial_record import get_record_with_data
from gizer.oppartial_record import get_tables_data_from_oplog_set_command
from gizer.all_schema_engines import get_schema_engines_as_dict



def test_complete_partial_record():
    object_id_bson_raw_data = '{\
"_id": { "$oid": "56b8da59f9fcee1b00000007" }\
}'
    array_bson_raw_data = '{\
"comments": [{\
"_id": {"$oid": "56b8f344f9fcee1b00000018"},\
"updated_at": "2016-02-08T19:57:56.678Z",\
"created_at": "2016-02-08T19:57:56.678Z"}]\
}'

    dbname = 'rails4_mongoid_development'
    db_schemas_path = '/'.join(['test_data', 'schemas', dbname])
    schemas = get_schema_engines_as_dict(db_schemas_path)
    schema_engine = schemas['posts']
    
    bson_data = loads(array_bson_raw_data)
    object_id_bson_data = loads(object_id_bson_raw_data)
    tables = get_tables_data_from_oplog_set_command(\
        schema_engine, bson_data, object_id_bson_data)
    assert(len(tables)==1)

def test_complete_partial_record2():
    object_id_bson_raw_data = '{\
"_id": { "$oid": "56b8da59f9fcee1b00000007" }\
}'
    array_bson_raw_data = '{"comments.2.tests.1": 1000}'

    dbname = 'rails4_mongoid_development'
    db_schemas_path = '/'.join(['test_data', 'schemas', dbname])
    schemas = get_schema_engines_as_dict(db_schemas_path)
    schema_engine = schemas['posts']
    
    bson_data = loads(array_bson_raw_data)
    object_id_bson_data = loads(object_id_bson_raw_data)
    tables_tuple = get_tables_data_from_oplog_set_command(\
        schema_engine, bson_data, object_id_bson_data)
    tables = tables_tuple[0]
    initial_indexes = tables_tuple[1]
    print "tables", tables.keys()
    print "initial_indexes", initial_indexes
    tests_t = tables['post_comment_tests']
    print "columns", tests_t.sql_column_names
    assert(initial_indexes=={u'posts_comments_tests': 1, u'posts_comments': 2})
    assert(tests_t.sql_columns['tests'].values[0]==1000)
    assert(len(tables)==1)

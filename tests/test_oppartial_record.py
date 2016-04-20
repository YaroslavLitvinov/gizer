#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import bson
from bson.json_util import loads
from gizer.oppartial_record import complete_partial_record
from gizer.all_schema_engines import get_schema_engines_as_dict

array_bson_raw_data = '{\
"comments": [{\
"_id": {"$oid": "56b8f344f9fcee1b00000018"},\
"updated_at": "2016-02-08T19:57:56.678Z",\
"created_at": "2016-02-08T19:57:56.678Z"}]\
}'

def test_complete_partial_record():
    dbname = 'rails4_mongoid_development'
    db_schemas_path = '/'.join(['test_data', 'schemas', dbname])
    schemas = get_schema_engines_as_dict(db_schemas_path)
    schema_engine = schemas['posts']
    
    bson_expected = loads(array_bson_raw_data)
    for name_and_path, value in bson_expected.iteritems():
        bson_rec = complete_partial_record(schema_engine, name_and_path, value)
        #tricky comparison of just text values
        print bson_rec
        print bson_expected
        assert(bson_rec == bson_expected)


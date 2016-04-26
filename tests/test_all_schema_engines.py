#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from gizer.all_schema_engines import get_schema_files, get_schema_engines_as_dict

def test_get_schema_engines_as_dist():
    dbname = 'rails4_mongoid_development'
    db_schemas_path = '/'.join(['test_data', 'schemas', dbname])
    files = get_schema_files(db_schemas_path)
    assert(files==['posts.js'])
    schemas = get_schema_engines_as_dict(db_schemas_path)
    assert(schemas['posts'].schema != None)



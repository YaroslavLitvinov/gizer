#!/usr/bin/env python

from mongo_to_hive_mapping import schema_engine
import os

def get_schema_files(schemas_dirpath):
    """ get list of js / json files resided in dirpath param. """
    res = []
    for fname in os.listdir(schemas_dirpath):
        if fname.endswith('json') or fname.endswith('js'):
            res.append(fname)
    return res
        
def get_schema_engines_as_dict(schemas_dirpath):
    """ Load schema engines into dict.
    Basename of schema file should be the name of collection"""
    js_schema_files = get_schema_files(schemas_dirpath)
    schemas = {}
    for fname in js_schema_files:
        collection_name = os.path.splitext(os.path.basename(fname))[0]
        schema_path = os.path.join(schemas_dirpath, fname)
        schemas[collection_name] = \
            schema_engine.create_schema_engine(collection_name, schema_path)
    return schemas

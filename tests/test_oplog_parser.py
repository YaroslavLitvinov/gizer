#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import psycopg2
from gizer.psql_requests import PsqlRequests
from gizer.oplog_parser import create_truncate_psql_objects
from gizer.oplog_parser import sync_oplog
from mock_mongo_reader import MongoReaderMock

# THis schema must be precreated before running tests
TMP_SCHEMA_NAME = 'operational'
MAIN_SCHEMA_NAME = ''

def initial_load(dbreq, schemas_path, psql_schema):
    create_truncate_psql_objects(dbreq, schemas_path, psql_schema)

def check_oplog_sync(oplog_ts_to_test):
    connstr = os.environ['TEST_PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))

    #create test mongo reader
    mongo_reader = None
    with open('test_data/posts_data_target_oplog_sync.js') as opfile:
        posts_data = opfile.read()
        mongo_reader = MongoReaderMock(posts_data)
        opfile.close()
    # create test olpog reader
    oplog_reader = None
    with open('test_data/test_oplog.js') as opfile:
        oplog_data = opfile.read()
        oplog_reader = MongoReaderMock(oplog_data)
        opfile.close()

    # oplog_ts_to_test is timestamp starting from which oplog records
    # should be applied to psql tables to locate ts which corresponds to
    # initially loaded psql data;
    # None - means oplog records should be tested starting from beginning
    schemas_path = "./test_data/schemas/rails4_mongoid_development"
    psql_schema_to_apply_ops = TMP_SCHEMA_NAME
    psql_schema_initial_load = MAIN_SCHEMA_NAME
    initial_load(dbreq, schemas_path, MAIN_SCHEMA_NAME)
    sync_res = sync_oplog(oplog_ts_to_test, dbreq, mongo_reader, oplog_reader,
                          schemas_path, psql_schema_to_apply_ops,
                          psql_schema_initial_load)
    while True:
        if sync_res is False or sync_res is True:
            break
        else:
            oplog_ts_to_test = sync_res
        sync_res = sync_oplog(oplog_ts_to_test, dbreq, mongo_reader,
                              oplog_reader, schemas_path,
                              psql_schema_to_apply_ops,
                              psql_schema_initial_load)
    return sync_res

def test_oplog_sync():
    res = check_oplog_sync(None)
    assert(res == True)
    res = check_oplog_sync('6249008760904220673')
    assert(res == True)
    res = check_oplog_sync('6249012068029138000')
    assert(res == False)


if __name__ == '__main__':
    test_oplog_sync()

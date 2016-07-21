#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import sys
import psycopg2
import logging
import pymongo
from logging import getLogger
from collections import namedtuple
from bson.json_util import loads
from gizer.psql_requests import PsqlRequests
from gizer.oplog_highlevel import OplogHighLevel
from gizer.psql_objects import compare_psql_and_mongo_records
from gizer.oplog_parser import EMPTY_TS
from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.psql_objects import insert_tables_data_into_dst_psql
from gizer.psql_objects import create_truncate_psql_objects
from mongo_schema.schema_engine import create_tables_load_bson_data
from mock_mongo_reader import MongoReaderMock
from mock_mongo_reader import MockReaderDataset
from gizer.etlstatus_table import timestamp_str_to_object


SCHEMAS_PATH = "./test_data/schemas/rails4_mongoid_development"
# THis schema must be precreated before running tests
MAIN_SCHEMA_NAME = ''

DO_OPLOG_APPLY=1
DO_OPLOG_SYNC=2

OplogTest = namedtuple('OplogTest', ['ts_synced',
                                     'before',
                                     'oplog_dataset_path_list',
                                     'after'])

def data_mock(mongo_data_path_list, collection):
    print mongo_data_path_list
    reader = None
    list_of_test_datasets = []
    for path_and_exception in mongo_data_path_list:
        mongo_data_path = path_and_exception[0]
        with open(mongo_data_path) as opfile:
            dataset = MockReaderDataset(opfile.read(), path_and_exception[1])
            list_of_test_datasets.append(dataset)
            opfile.close()
    reader = MongoReaderMock(list_of_test_datasets, collection)
    getLogger(__name__).info("prepared %d dataset/s" % len(list_of_test_datasets))
    return reader

def load_mongo_data_to_psql(schema_engine, mongo_data_path, psql, psql_schema):
    getLogger(__name__).info("Load initial data from %s" \
                                         % (mongo_data_path))
    with open(mongo_data_path, "r") as input_f:
        raw_bson_data = input_f.read()
        for one_record in loads(raw_bson_data):
            tables = create_tables_load_bson_data(schema_engine, [one_record])
            getLogger(__name__).info("Loaded tables=%s" % tables.tables)
            insert_tables_data_into_dst_psql(psql, tables, psql_schema, '')
            psql.cursor.execute('COMMIT')


def run_oplog_engine_check(oplog_test, what_todo, schemas_path):
    connstr = os.environ['TEST_PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))

    psql_schema = MAIN_SCHEMA_NAME

    schema_engines = get_schema_engines_as_dict(schemas_path)
    getLogger(__name__).info("Loading oplog data...")
    oplog_reader = data_mock(oplog_test.oplog_dataset_path_list, None)

    create_truncate_psql_objects(dbreq, schemas_path, psql_schema)
    dbreq.cursor.execute('COMMIT')
    # do pseudo "init load", ignore inject_exception on this step
    for name, mongo_data_path in oplog_test.before.iteritems():
        load_mongo_data_to_psql(schema_engines[name],
                                mongo_data_path[0], dbreq, psql_schema)
    # recreate connection / cursor as rollback won't work after commit
    del dbreq
    dbreq = PsqlRequests(psycopg2.connect(connstr))
    mongo_readers_after = {}
    getLogger(__name__).info("Loading mongo data after initload")
    for name, mongo_data_path in oplog_test.after.iteritems():
        # pass just one dataset as collection's test mongo data
        mongo_readers_after[name] = data_mock([mongo_data_path], name)

    ohl = OplogHighLevel(dbreq, mongo_readers_after, oplog_reader,
                 schemas_path, schema_engines, psql_schema)

    #start syncing from very start of oplog
    try:
        if what_todo is DO_OPLOG_APPLY:
            res = ohl.do_oplog_apply(start_ts=None, doing_sync=False)
            return res.res
        elif what_todo is DO_OPLOG_SYNC:
            ts_synced = ohl.do_oplog_sync(None)
            getLogger(__name__).info("sync res expected to be ts_synced=%s" \
                                         % oplog_test.ts_synced)
            if ts_synced == timestamp_str_to_object(oplog_test.ts_synced):
                return True
            elif ts_synced == True and oplog_test.ts_synced is None:
                return True
            else:
                return False
        else:
            assert(0)
    except:
        # close psql connection to have ability to run next tests
        dbreq.conn.close()
        raise


def check_dataset(name, operation, start_ts, oplog_params, params):
    print '\ntest ', name, operation
    location_fmt = 'test_data/'+name+'/%s_collection_%s.js'
    before_params = {}
    for collection in params:
        before_params[collection] \
            = (location_fmt % ('before', collection), None)
    after_params = {}
    for collection in params:
        after_params[collection] \
            = (location_fmt % ('after', collection), params[collection])
    oplog_test \
        = OplogTest(start_ts, # None = expected as already synchronized, 
                    # use None with DO_OPLOG_APPLY param
                    before_params, oplog_params, after_params)
    res = run_oplog_engine_check(oplog_test, operation, SCHEMAS_PATH)
    return res

def test_oplog_sync():
    logging.basicConfig(level=logging.DEBUG,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')

    # test applying oplog ops to initial data 'before_data' and then compare it 
    # with final 'after_data'
    assert(check_dataset('oplog1', DO_OPLOG_APPLY, None,
                         [('test_data/oplog1/oplog1.js', None),
                          ('test_data/oplog1/oplog2.js', None)],
                         {'posts': None, 'guests': None}) == True)

    # test syncing oplog ops. specified DO_OPLOG_SYNC param.
    # initdata 'before_data' is slightly ovarlaps with oplog ops data.
    # Sync point when located should be equal to timestamp param
    assert(check_dataset('oplog2', DO_OPLOG_SYNC, 'Timestamp(1164278289, 1)',
                         [('test_data/oplog2/oplog.js', None),
                          ('test_data/oplog2/\
oplog_simulate_added_after_initload.js',
                           None)],
                         {'posts': None, 'guests': None}) == True)

    # test syncing oplog ops. specified DO_OPLOG_SYNC param.
    # initdata 'before_data' is slightly ovarlaps with oplog ops data.
    # Sync point when located should be equal to timestamp param
    assert(check_dataset('oplog3', DO_OPLOG_SYNC, 'Timestamp(1000000001, 1)',
                         [('test_data/oplog3/oplog.js', None)],
                         {'posts': None, 'posts2': None, 'rated_posts': None,
                          'guests': None}) == True)

    # inject error, it must do not raise error but to keep the same state
    # return True, and as there no records were processed. It is supposed 
    # that in next time it's will run normally
    assert(check_dataset('oplog1', DO_OPLOG_APPLY, None,
                         [('test_data/oplog1/oplog1.js', None),
                          ('test_data/oplog1/oplog2.js', 
                           pymongo.errors.OperationFailure)],
                         {'posts': None, 'guests': None}) == True)

    # raise pymongo.errors.OperationFailure exception which must be handled.
    # Should not lead to error and Timestamp should not be changed.
    # Must return True, just emulate case when no records were processed.
    # It is supposed that in next time it's will run normally 
    assert(check_dataset('oplog1', DO_OPLOG_APPLY, None,
                         [('test_data/oplog1/oplog1.js', None),
                          ('test_data/oplog1/oplog2.js', None)],
                         {'posts': pymongo.errors.OperationFailure,
                          'guests': None}) == True)

    # raise pymongo.errors.AutoReconnect exception which must be handled.
    # Should not lead to error and Timestamp should not be changed.
    # Must return True, just emulate case when no records were processed.
    # It is supposed that in next time it's will run normally 
    assert(check_dataset('oplog1', DO_OPLOG_APPLY, None,
                         [('test_data/oplog1/oplog1.js', None),
                          ('test_data/oplog1/oplog2.js', None)],
                         {'posts': pymongo.errors.AutoReconnect,
                          'guests': None}) == True)

    # inject error, it must raise an error which should not be bypassed
    # False expected
    try:
        check_dataset('oplog1', DO_OPLOG_APPLY, None,
                      [('test_data/oplog1/oplog1.js', pymongo.errors.InvalidURI),
                       ('test_data/oplog1/oplog2.js', None)],
                      {'posts': None,
                       'guests': None})
    except:
        pass
    else:
        assert(0)

    # inject error, it must raise an error which should not be bypassed
    # False expected
    try:
        check_dataset('oplog1', DO_OPLOG_APPLY, None,
                      [('test_data/oplog1/oplog1.js', None),
                       ('test_data/oplog1/oplog2.js', None)],
                      {'posts': pymongo.errors.InvalidURI,
                       'guests': None})
    except:
        pass
    else:
        assert(0)

    # inject error and error must be returnrned for sync operation
    # Sync does not handling exceptions.
    assert(check_dataset('oplog2', DO_OPLOG_SYNC, 'Timestamp(1164278289, 1)',
                         [('test_data/oplog2/oplog.js', None),
                          ('test_data/oplog2/\
oplog_simulate_added_after_initload.js',
                           pymongo.errors.OperationFailure)],
                         {'posts': None, 'guests': None}) == False)


def test_compare_empty_compare_psql_and_mongo_records():
    connstr = os.environ['TEST_PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))
    empty_mongo = ('test_data/oplog1/before_collection_posts.js', None)
    mongo_reader = data_mock([empty_mongo], None)
    schemas_path = "./test_data/schemas/rails4_mongoid_development"
    schema_engines = get_schema_engines_as_dict(schemas_path)

    #cmpare non existing record
    res = compare_psql_and_mongo_records(
        dbreq, mongo_reader, schema_engines['posts'], 
        "111111111111111111111110", MAIN_SCHEMA_NAME)
    assert(res == True)


if __name__ == '__main__':
    """ Test external data by providing path to schemas folder, 
    data folder as args """
    test_oplog_sync()
    exit(0)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    schemas_path = sys.argv[1]
    data_path = sys.argv[2]
    mongo_oplog = os.path.join(data_path, 'mongo_oplog.json')

    empty_data_before = {}
    data_after = {}
    schema_engines = get_schema_engines_as_dict(schemas_path)
    for schema_name in schema_engines:
        path_with_data = os.path.join(data_path, 'mongo_%s.json' % schema_name)
        data_after[schema_name] = path_with_data
    oplog_test1 \
        = OplogTest(None, 
                    empty_data_before,
                    [mongo_oplog],
                    data_after)
    res = run_oplog_engine_check(oplog_test1, schemas_path)
    assert(res == True)

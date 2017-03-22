#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import sys
import psycopg2
import logging
import bson
import pymongo
import gizer
from logging import getLogger
from collections import namedtuple
from bson.json_util import loads
from gizer.psql_requests import PsqlRequests
from gizer.oplog_sync_alligned_data import OplogSyncAllignedData
from gizer.oplog_sync_unalligned_data import OplogSyncUnallignedData
from gizer.oplog_parser import EMPTY_TS
from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.psql_objects import insert_tables_data_into_dst_psql
from gizer.psql_objects import create_truncate_psql_objects
from mongo_schema.schema_engine import create_tables_load_bson_data
from mongo_schema.schema_engine import log_table_errors
from mock_mongo_reader import MongoReaderMock
from mock_mongo_reader import MockReaderDataset
from gizer.etlstatus_table import timestamp_str_to_object as ts_obj
from gizer.log import save_loglevel

SCHEMAS_PATH = "./test_data/schemas/rails4_mongoid_development"
# THis schema must be precreated before running tests
MAIN_SCHEMA_NAME = ''

OplogTest = namedtuple('OplogTest', ['before',
                                     'oplog_dataset',
                                     'after',
                                     'max_tries',
                                     'skip_sync'])
SYNC_TRY_COUNT = 10

def data_mock(mongo_data_path_list, collection):
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

def data_mock_no_exception(mongo_data_path_list, collection):
    reader = None
    list_of_test_datasets = []
    for path in mongo_data_path_list:
        mongo_data_path = path[0]
        with open(mongo_data_path) as opfile:
            dataset = MockReaderDataset(opfile.read(), None)
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
            log_table_errors('test info msg:', tables.errors)
            getLogger(__name__).info("Loaded tables=%s" % tables.tables)
            insert_tables_data_into_dst_psql(psql, tables, psql_schema, '')
            psql.cursor.execute('COMMIT')


def get_readers(oplog_test, enable_exceptions):
    oplog_readers = {}
    mongo_readers_after = {}
    for name, oplog_datas in oplog_test.oplog_dataset.iteritems():
        if enable_exceptions:
            oplog_readers[name] = data_mock(oplog_datas, None)
        else:
            oplog_readers[name] = data_mock_no_exception(oplog_datas, None)

    for name, mongo_data_path in oplog_test.after.iteritems():
        # pass just one dataset as collection's test mongo data
        if enable_exceptions:
            mongo_readers_after[name] = data_mock([mongo_data_path], name)
        else:
            mongo_readers_after[name] = data_mock_no_exception([mongo_data_path], name)
    return (oplog_readers, mongo_readers_after)

def run_oplog_engine_check(oplog_test, schemas_path):
    connstr = os.environ['TEST_PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))

    psql_schema = MAIN_SCHEMA_NAME

    schema_engines = get_schema_engines_as_dict(schemas_path)
    getLogger(__name__).info("Loading oplog data...")

    oplog_readers, mongo_readers_after = get_readers(oplog_test, 
                                                     enable_exceptions=False)

    create_truncate_psql_objects(dbreq, schemas_path, psql_schema)
    dbreq.cursor.execute('COMMIT')
    # do pseudo "init load", ignore inject_exception on this step
    for name, mongo_data_path in oplog_test.before.iteritems():
        load_mongo_data_to_psql(schema_engines[name],
                                mongo_data_path[0], dbreq, psql_schema)
    # recreate connection / cursor as rollback won't work after commit
    del dbreq
    dbreq = PsqlRequests(psycopg2.connect(connstr))
    dbreq_etl = PsqlRequests(psycopg2.connect(connstr))
    getLogger(__name__).info("Loading mongo data after initload")

    try:
        gizer.oplog_sync_unalligned_data.DO_OPLOG_REREAD_MAXCOUNT \
            = oplog_test.max_tries
        gizer.oplog_sync_alligned_data.DO_OPLOG_REREAD_MAXCOUNT \
            = oplog_test.max_tries
        # for better coverage
        gizer.oplog_sync_unalligned_data.SYNC_REC_COUNT_IN_ONE_BATCH = 2

        if not oplog_test.skip_sync:
            unalligned_sync \
                = OplogSyncUnallignedData(dbreq_etl, dbreq, 
                                          mongo_readers_after, oplog_readers,
                                          schemas_path, schema_engines, psql_schema,
                                          3)

            #start syncing from very start of oplog
            ts_synced = unalligned_sync.sync(None)
            getLogger(__name__).info("Sync done ts_synced: %s" % str(ts_synced))
            getLogger(__name__).info("statistic %s" % str(unalligned_sync.statistic()))
            del unalligned_sync
            # sync failed
            if ts_synced is None:
                return False

            del dbreq
            dbreq = PsqlRequests(psycopg2.connect(connstr))

            oplog_readers, mongo_readers_after = get_readers(oplog_test, 
                                                             enable_exceptions=True)
        else:
            # ignore sync results
            ts_synced = {}
            for name in oplog_readers:
                ts_synced[name] = None

        alligned_sync \
            = OplogSyncAllignedData(dbreq, mongo_readers_after, oplog_readers,
                                    schemas_path, schema_engines, psql_schema,
                                    3, False)
        res = alligned_sync.sync(ts_synced)
        if res:
            getLogger(__name__).info("Test passed")

    except:
        # close psql connection to have ability to run next tests
        dbreq.conn.close()
        raise
    if res:
        return True
    else:
        return False

def check_dataset(skip_sync, name, oplog_params, params,
                  max_tries=SYNC_TRY_COUNT):
    print '\ntest ', name
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
        = OplogTest(before_params, oplog_params, after_params,
                    max_tries, skip_sync)
    res = run_oplog_engine_check(oplog_test, SCHEMAS_PATH)
    return res


# following group of tests just testing oplog sync + oplog use capabilites
# At first for every shard in dataset sync process must locate sync point.
# After that data is populating into psql by oplog use process.
# If data populated to postgres is matching to actual data in mongodb then
# oplog use process finish succesfully. Test for dataset will passed only 
# if both operations sync & use are succesfully completed.
# Every dataset is consists from three parts: 
# before_collection_xxxx - emulate mongo records populated to psql by 
#                          init load (can be empty)
# oplog (oplog-shard1/2) - oplog records which are available in shards
# after_collection_xxxx - emulate actual state for collection on moment 
#                         when init load and oplog read are completed
# test applying oplog ops to initial data 'before_data' and then compare it 
# with final 'after_data'

# pymongo.errors.OperationFailure, pymongo.errors.AutoReconnect exceptions 
# must be handled safely if occurs.
# Should not lead to error and current Timestamps should not be changed.
# psql data must be left unchanged. In real life OperationFailure just is an
# internal mongodb error and should not lead to etl error.


def test_oplog1():
    logging.basicConfig(level=logging.DEBUG,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    save_loglevel()
    # dataset test
    oplog1 = {'shard1': [('test_data/oplog1/oplog1.js', None), # ntry 0
                         ('test_data/oplog1/oplog2.js', None)  # ntry 1
                         ],
              'shard2': [('test_data/oplog1/shard2-oplog1.js', None)
                         ]
             }
    assert(check_dataset(False, 'oplog1', oplog1,
                         {'posts': None, 'guests': None}) == True)

def test_oplog2():
    logging.basicConfig(level=logging.INFO,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    save_loglevel()
    # dataset test
    oplog2 = {'single-oplog': [('test_data/oplog2/oplog.js', None),
                               ('test_data/oplog2/\
oplog_simulate_added_after_initload.js',
                                None)],
              }
    assert(check_dataset(False, 'oplog2', oplog2,
                         {'posts': None, 'guests': None}) == True)

def test_oplog3():
    logging.basicConfig(level=logging.INFO,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    save_loglevel()
    # dataset test
    oplog3 = {'single-oplog': [('test_data/oplog3/oplog.js', None)]}
    assert(check_dataset(False, 'oplog3', oplog3,
                         {'posts': None, 'posts2': None, 'rated_posts': None,
                          'guests': None}) == True)

def test_oplog4():
    logging.basicConfig(level=logging.DEBUG,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    save_loglevel()
    oplog4 = {'single-oplog': [('test_data/oplog4/oplog1.js', None), # ntry 0
                               ('test_data/oplog4/oplog2.js', None), # ntry 1
                               ('test_data/oplog4/oplog3.js', None), # ntry 2
                               ('test_data/oplog4/oplog4.js', None), # ntry 3
                               ('test_data/oplog4/oplog5.js', None), # ntry 4
                               ('test_data/oplog4/oplog6.js', None), # ntry 5
                               ('test_data/oplog4/oplog7.js', None), # ntry 6
                               ('test_data/oplog4/oplog8.js', None), # ntry 7
                               ('test_data/oplog4/oplog9.js', None) # ntry 8
                               ]}
    assert(check_dataset(False, 'oplog4', oplog4, 
                         {'posts': None} # don't raise error while reading posts
                         ) == True)

def test_oplog5():
    logging.basicConfig(level=logging.INFO,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    save_loglevel()
    # dataset test should not fail, oplog recover in action
    oplog4 = {'single-oplog': [('test_data/oplog4/oplog1.js', None), # ntry 0
                               ('test_data/oplog4/oplog2.js', None), # ntry 1
                               ('test_data/oplog4/oplog3.js', None), # ntry 2
                               ('test_data/oplog4/oplog4.js', None), # ntry 3
                               ('test_data/oplog4/oplog5.js', None), # ntry 4
                               ('test_data/oplog4/oplog6.js', None), # ntry 5
                               ('test_data/oplog4/oplog7.js', None), # ntry 6
                               ('test_data/oplog4/oplog8.js', None) # ntry 7
                               ]}
    assert(check_dataset(True, 'oplog4', oplog4, 
                         {'posts': None}, # don't raise error while reading posts
                         10 # - max ntrys count to re-read oplog
                         ) == True)

def test_oplog6():
    logging.basicConfig(level=logging.INFO,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    save_loglevel()
    # dataset test
    oplog21 = {'single-oplog': [('test_data/oplog2/oplog.js', None),
                               ('test_data/oplog2/\
oplog_simulate_added_after_initload.js',
                                pymongo.errors.OperationFailure)]}
    assert(check_dataset(False, 'oplog2', oplog21,
                         {'posts': None, 'guests': None}) == True)

    # dataset test
    oplog22 = {'single-oplog': [('test_data/oplog2/oplog.js', None),
                               ('test_data/oplog2/\
oplog_simulate_added_after_initload.js',
                                pymongo.errors.NetworkTimeout)]}
    assert(check_dataset(False, 'oplog2', oplog22,
                         {'posts': None, 'guests': None}) == True)

    # dataset test
    oplog32 = {'single-oplog': [('test_data/oplog3/oplog.js', None)]}
    assert(check_dataset(False, 'oplog3', oplog32,
                         {'posts': pymongo.errors.OperationFailure, 'posts2': None, 
                          'rated_posts': None, 'guests': None}) == True)

def test_oplog7():
    logging.basicConfig(level=logging.INFO,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    save_loglevel()
    # dataset test
    oplog7 = {'single-oplog': [('test_data/oplog7/oplog.js', None)]}
    assert(check_dataset(False, 'oplog7', oplog7, {'posts': None}) == True)

def test_oplog8():
    logging.basicConfig(level=logging.INFO,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    save_loglevel()
    # dataset test
    oplog31 = {'single-oplog': [('test_data/oplog3/oplog.js', None)]}
    assert(check_dataset(False, 'oplog3', oplog31,
                         {'posts': pymongo.errors.AutoReconnect, 'posts2': None, 
                          'rated_posts': None, 'guests': None}) == True)

    # dataset test should fail
    try:
        oplog32 = {'single-oplog': [('test_data/oplog3/oplog.js', 
                                         pymongo.errors.InvalidURI)]}
        check_dataset(False, 'oplog3', oplog32,
                      {'posts': None, 'posts2': None, 
                          'rated_posts': None, 'guests': None})
    except:
        pass
    else:
        assert(0)

    # dataset test should fail
    try:
        check_dataset(True, 'oplog1', oplog31,
                      {'posts': pymongo.errors.InvalidURI,
                       'guests': None})
    except:
        pass
    else:
        assert(0)


def test_oplog9():
    logging.basicConfig(level=logging.INFO,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    save_loglevel()
    # dataset test
    oplog21 = {'single-oplog': [('test_data/oplog2/oplog.js', None),
                                ('test_data/oplog2/\
oplog_simulate_added_after_initload.js',
                                 pymongo.errors.OperationFailure)]
               }
    assert(check_dataset(False, 'oplog2', oplog21,
                         {'posts': None, 'guests': None}) == True)




if __name__ == '__main__':
    """ Test external data by providing path to schemas folder, 
    data folder as args """
    logging.basicConfig(level=logging.DEBUG,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    save_loglevel()

    schemas_path = sys.argv[1]
    data_path = sys.argv[2]
    mongo_oplog = os.path.join(data_path, 'mongo_oplog.json')

    empty_data_before = {}
    data_after = {}
    schema_engines = get_schema_engines_as_dict(schemas_path)
    for schema_name in schema_engines:
        path_with_data = os.path.join(data_path, 'mongo_%s.json' % schema_name)
        data_after[schema_name] = (path_with_data, None)
    oplog_test1 \
        = OplogTest(empty_data_before,
                    {'oplog': [(mongo_oplog, None)]},
                    data_after,
                    SYNC_TRY_COUNT, True)
    res = run_oplog_engine_check(oplog_test1, schemas_path)
    assert(res == True)

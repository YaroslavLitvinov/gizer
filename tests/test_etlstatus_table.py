#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import psycopg2
from bson.json_util import loads
from gizer.etlstatus_table import *

def check_start(obj, status, error, ts):
    assert(obj.status == status)
    assert(obj.ts == ts)
    assert(obj.time_start != None)
    assert(obj.time_end == None)
    assert(obj.error == error)

def check_end(obj, recs_count, queries_count, status, error, ts):
    assert(obj.status == status)
    assert(obj.recs_count == recs_count)
    assert(obj.queries_count == queries_count)
    assert(obj.time_end != None)
    assert(obj.ts == ts)
    assert(obj.error == error)

def test_psql_etl_status_table():
    connstr = os.environ['TEST_PSQLCONN']
    conn = psycopg2.connect(connstr)
    cursor = conn.cursor()
    status_table = PsqlEtlStatusTable(cursor, '', ['shard'], recreate=True)
    status_manager = PsqlEtlStatusTableManager(status_table)
    recent_status = status_table.get_recent()
    assert(recent_status == None)

    ts1 = timestamp_str_to_object('Timestamp(1464278289, 1)')
    status_manager.init_load_start(ts1)
    # check init_load is in progress
    recent_status = status_table.get_recent()
    check_start(recent_status, STATUS_INITIAL_LOAD, None, ts1)

    status_manager.init_load_finish(error=False)
    # check init_load is finished successfully
    recent_status = status_table.get_recent()
    check_end(recent_status, None, None,  STATUS_INITIAL_LOAD, False, ts1)

    status_manager.init_load_finish(error=True)
    # check init_load is finished unsuccessfully
    recent_status = status_table.get_recent()
    check_end(recent_status, None, None, STATUS_INITIAL_LOAD, True, ts1)

    ts2 = timestamp_str_to_object('Timestamp(1464279389, 1)')
    status_manager.oplog_sync_start(ts2)
    # check oplog_sync_start is in progress
    recent_status = status_table.get_recent()
    check_start(recent_status, STATUS_OPLOG_SYNC, None, ts2)

    status_manager.oplog_sync_finish(None, None, ts2, error=False)
    # check oplog_sync is finished successfully
    recent_status = status_table.get_recent()
    check_end(recent_status, None, None, STATUS_OPLOG_SYNC, False, ts2)

    status_manager.oplog_sync_finish(None, None, ts2, error=True)
    # check oplog_sync is finished unsuccessfully
    recent_status = status_table.get_recent()
    check_end(recent_status, None, None, STATUS_OPLOG_SYNC, True, ts2)

    ts3 = timestamp_str_to_object('Timestamp(1464289389, 1)')
    status_manager.oplog_use_start(ts3, None)
    # check oplog_sync_apply is in progress
    recent_status = status_table.get_recent()
    check_start(recent_status, STATUS_OPLOG_APPLY, None, ts3)

    status_manager.oplog_use_finish(None, None, ts3, None, True)
    # check oplog_sync_apply is finished unsuccessfully
    recent_status = status_table.get_recent()
    check_end(recent_status, None, None, STATUS_OPLOG_APPLY, True, ts3)

    status_manager.oplog_use_finish(1, 10, ts3, None, False)
    # check oplog_sync_apply is finished successfully
    recent_status = status_table.get_recent()
    check_end(recent_status, 1, 10, STATUS_OPLOG_APPLY, False, ts3)

    status_manager.oplog_resync_finish(1, 10, ts3, False)
    # check oplog_sync_apply is finished successfully
    recent_status = status_table.get_recent()
    check_end(recent_status, 1, 10, STATUS_OPLOG_RESYNC, False, ts3)


def test_psql_etl_status_table2():
    connstr = os.environ['TEST_PSQLCONN']
    conn = psycopg2.connect(connstr)
    cursor = conn.cursor()
    status_table = PsqlEtlStatusTable(cursor, '', ['shard1', 'shard2'], 
                                      recreate=True)
    status_manager = PsqlEtlStatusTableManager(status_table)
    recent_status = status_table.get_recent()
    assert(recent_status == None)

    ts1 = {'shard1': timestamp_str_to_object('Timestamp(1464278289, 1)'),
           'shard2': timestamp_str_to_object('Timestamp(1464278289, 3)')}
    status_manager.init_load_start(ts1)
    # check init_load is in progress
    recent_status = status_table.get_recent()
    check_start(recent_status, STATUS_INITIAL_LOAD, None, ts1)

    status_manager.oplog_sync_start(ts1)
    # check oplog_sync_start is in progress
    recent_status = status_table.get_recent()
    check_start(recent_status, STATUS_OPLOG_SYNC, None, ts1)

    ts2 = {'shard1': timestamp_str_to_object('Timestamp(1464278289, 4)'),
           'shard2': timestamp_str_to_object('Timestamp(1464278289, 5)')}
    status_manager.oplog_sync_finish(None, None, ts2, error=False)
    # check oplog_sync is finished successfully
    recent_status = status_table.get_recent()
    check_end(recent_status, None, None, STATUS_OPLOG_SYNC, False, ts2)

    status_manager.oplog_sync_finish(1, 10, ts2, error=True)
    # check oplog_sync is finished unsuccessfully
    recent_status = status_table.get_recent()
    check_end(recent_status, 1, 10, STATUS_OPLOG_SYNC, True, ts2)

if __name__ == '__main__':
    test_psql_etl_status_table()
    test_psql_etl_status_table2()

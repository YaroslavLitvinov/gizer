#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import psycopg2
from gizer.etlstatus_table import *

def check_start(obj, status, error, ts):
    assert(obj.status == status)
    assert(obj.ts == ts)
    assert(obj.time_start != None)
    assert(obj.time_end == None)
    assert(obj.error == error)

def check_end(obj, status, error, ts):
    assert(obj.status == status)
    assert(obj.time_end != None)
    assert(obj.ts == ts)
    assert(obj.error == error)

def test_psql_etl_status_table():
    connstr = os.environ['TEST_PSQLCONN']
    conn = psycopg2.connect(connstr)
    cursor = conn.cursor()
    status_table = PsqlEtlStatusTable(cursor, 'operational', recreate=True)
    status_manager = PsqlEtlStatusTableManager(status_table)
    recent_status = status_table.get_recent()
    assert(recent_status == None)

    status_manager.init_load_start("1111")
    # check init_load is in progress
    recent_status = status_table.get_recent()
    check_start(recent_status, STATUS_INITIAL_LOAD, None, "1111")

    status_manager.init_load_finish(is_error=False)
    # check init_load is finished successfully
    recent_status = status_table.get_recent()
    check_end(recent_status, STATUS_INITIAL_LOAD, False, "1111")

    status_manager.init_load_finish(is_error=True)
    # check init_load is finished unsuccessfully
    recent_status = status_table.get_recent()
    check_end(recent_status, STATUS_INITIAL_LOAD, True, "1111")

    status_manager.oplog_sync_start("111111")
    # check oplog_sync_start is in progress
    recent_status = status_table.get_recent()
    check_start(recent_status, STATUS_OPLOG_SYNC, None, "111111")

    status_manager.oplog_sync_finish("222222", is_error=False)
    # check oplog_sync is finished successfully
    recent_status = status_table.get_recent()
    check_end(recent_status, STATUS_OPLOG_SYNC, False, "222222")

    status_manager.oplog_sync_finish("222222", is_error=True)
    # check oplog_sync is finished unsuccessfully
    recent_status = status_table.get_recent()
    check_end(recent_status, STATUS_OPLOG_SYNC, True, "222222")

    status_manager.oplog_use_start("1234")
    # check oplog_sync_apply is in progress
    recent_status = status_table.get_recent()
    check_start(recent_status, STATUS_OPLOG_APPLY, None, "1234")

    status_manager.oplog_use_finish("1234", True)
    # check oplog_sync_apply is finished unsuccessfully
    recent_status = status_table.get_recent()
    check_end(recent_status, STATUS_OPLOG_APPLY, True, "1234")

    status_manager.oplog_use_finish("1234", False)
    # check oplog_sync_apply is finished successfully
    recent_status = status_table.get_recent()
    check_end(recent_status, STATUS_OPLOG_APPLY, False, "1234")

#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import psycopg2
from gizer.etlstatus_table import *

def test_psql_etl_status_table():
    connstr = os.environ['TEST_PSQLCONN']
    conn = psycopg2.connect(connstr)
    cursor = conn.cursor()
    status_table = PsqlEtlStatusTable(cursor, 'operational', recreate=True)
    status_manager = PsqlEtlStatusTableManager(status_table)
    recent_status = status_table.get_recent()
    assert(recent_status == None)
    status_manager.init_load_start("11111")
    # check init_load is in progress
    recent_status = status_table.get_recent()
    assert(recent_status.status == STATUS_INITIAL_LOAD)
    assert(recent_status.ts == "11111")
    assert(recent_status.time_end == None)
    assert(recent_status.error == None)
    status_manager.init_load_finish(is_error=False)
    # check init_load is finished successfully
    recent_status = status_table.get_recent()
    assert(recent_status.time_end != None)
    assert(recent_status.ts != None)
    assert(recent_status.error == False)

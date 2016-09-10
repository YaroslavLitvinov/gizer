#!/usr/bin/env python

""" Copy mongo data to psql by using two strategies:
1. Do initial load - copy data using trunk&load process, which rewriting
destination data every time.
2. If mongodb oplog - 'operational log' is enabled - patch psql data by oplog
operations, so it's should not overwrite dest data. If initial load is complete
but sync point is not yet located then synchronization process will be started.
The sync point - 'oplog timestamp' is the result of syncronization. That means
all data from oplog can be applied to psql data starting from that timestamp.
If sync is failed or data verification is failed at patch applying it's will
start initial load again. Every application session will log status data into
psql table 'qmetlstatus' in public schema."""

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import sys
import argparse
import configparser
import datetime
import logging
from os import system
from logging import getLogger
from collections import namedtuple
from mongo_reader.reader import MongoReader
from mongo_reader.reader import mongo_reader_from_settings
from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.etlstatus_table import STATUS_INITIAL_LOAD
from gizer.etlstatus_table import STATUS_OPLOG_SYNC
from gizer.etlstatus_table import STATUS_OPLOG_APPLY
from gizer.etlstatus_table import STATUS_OPLOG_RESYNC
from gizer.etlstatus_table import PsqlEtlStatusTable
from gizer.etlstatus_table import PsqlEtlStatusTableManager
from gizer.oplog_sync_alligned_data import OplogSyncAllignedData
from gizer.oplog_sync_unalligned_data import OplogSyncUnallignedData
from gizer.psql_requests import PsqlRequests
from gizer.psql_requests import psql_conn_from_settings
from gizer.opconfig import psql_settings_from_config
from gizer.opconfig import mongo_settings_from_config
from gizer.opconfig import load_mongo_replicas_from_setting
from gizer.log import save_loglevel

def sectkey(section_name, base_key_name):
    """ Return key config value. Key name in file must be concatenation 
    of both params divided by hyphen """
    return "%s-%s".format(section_name, base_key_name)

def getargs():
    """ get args from cmdline """
    default_request = '{}'
    parser = argparse.ArgumentParser()

    args = parser.parse_args()
    if args.js_request is None:
        args.js_request = default_request

    return args


def create_logger(logspath, name):
    today = datetime.datetime.now()
    logfilename='{date}-{name}.log'.format(name=name,
                                           date=today.strftime('%Y-%m-%d'))
    logfilename = os.path.join(logspath, logfilename)
    logging.basicConfig(filename=logfilename,
                        level=logging.INFO,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    save_loglevel()
    logger = getLogger(__name__)
    logger.info('Created')


def reinit_conn(config_settings, psql, status_manager):
    # recreate conn used by status_manager, 
    # for long running sync/apply
    psql.reinit(psql_conn_from_settings(config_settings))
    status_manager.status_table.replace_conn(psql)

def main():
    """ main """

    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", action="store",
                        help="Config file with settings",
                        type=file, required=True)
    args = parser.parse_args()
    
    config = configparser.ConfigParser()
    config.read_file(args.config_file)

    schemas_path = config['misc']['schemas-dir']
    logspath = config['misc']['logs-dir']

    oplog_settings = load_mongo_replicas_from_setting(config, 'mongo-oplog')

    mongo_settings = mongo_settings_from_config(config, 'mongo')
    psql_settings = psql_settings_from_config(config, 'psql')

    mongo_readers = {}
    schema_engines = get_schema_engines_as_dict(schemas_path)
    for collection_name in schema_engines:
        reader = mongo_reader_from_settings(mongo_settings, collection_name, {})
        mongo_readers[collection_name] = reader
        mongo_readers[collection_name].set_name(collection_name)

    # create oplog read transport/s
    oplog_readers = {}
    for oplog_name, settings_list in oplog_settings.iteritems():
        # settings list is a replica set (must be at least one in list)
        oplog_readers[oplog_name] = \
            mongo_reader_from_settings(settings_list, 'oplog.rs', {})
        oplog_readers[oplog_name].set_name(oplog_name)

    psql_qmetl = PsqlRequests(psql_conn_from_settings(psql_settings))
    psql_main = PsqlRequests(psql_conn_from_settings(psql_settings))

    status_table = PsqlEtlStatusTable(psql_qmetl.cursor, 
                                      config['psql']['psql-schema-name'],
                                      sorted(oplog_settings.keys()))
    status_manager = PsqlEtlStatusTableManager(status_table)

    psql_schema = config['psql']['psql-schema-name']

    res = 0
    status = status_table.get_recent()
    if status:
        if (status.status == STATUS_INITIAL_LOAD \
                or status.status == STATUS_OPLOG_RESYNC) \
                and status.time_end and not status.error:
            create_logger(logspath, 'oplogsync')
            psql_sync = psql_main
            # intial load done, save oplog sync status and do oplog sync.
            status_manager.oplog_sync_start(status.ts)
            unalligned_sync = OplogSyncUnallignedData(
                psql_qmetl, psql_sync, mongo_readers, oplog_readers,
                schemas_path, schema_engines, psql_schema)
            try:
                ts = unalligned_sync.sync(status.ts)
                stat = unalligned_sync.statistic()
                reinit_conn(psql_settings, psql_qmetl, status_manager)
                if ts: # sync ok
                    status_manager.oplog_sync_finish(stat[0], stat[1], ts, False)
                    res = 0
                else: # error
                    status_manager.oplog_sync_finish(stat[0], stat[1], None, True)
                    res = -1
            except Exception, e:
                getLogger(__name__).error(e, exc_info=True)
                getLogger(__name__).error('ROLLBACK CLOSE')
                psql_sync.conn.rollback()
                reinit_conn(psql_settings, psql_qmetl, status_manager)
                status_manager.oplog_sync_finish(None, True)
                res = -1

        elif (status.status == STATUS_OPLOG_SYNC or \
              status.status == STATUS_OPLOG_APPLY) \
            and status.time_end and not status.error:
            create_logger(logspath, 'oploguse')
            # sync done, now apply oplog pacthes to main psql
            # save oplog sync status
            getLogger(__name__).\
                info('Sync point is ts:{ts}'.format(ts=status.ts))
            status_manager.oplog_use_start(status.ts)
            alligned_sync = \
                OplogSyncAllignedData(psql_main, mongo_readers, oplog_readers,
                                      schemas_path, schema_engines, psql_schema)
            try:
                ts_res = alligned_sync.sync(status.ts)
                stat = alligned_sync.statistic()
                reinit_conn(psql_settings, psql_qmetl, status_manager)
                if ts_res == 'resync':
                    # some records recovered must do resync at next step
                    status_manager.oplog_resync_finish(stat[0], stat[1],
                                                       status.ts, False)
                    res= 0
                elif ts_res: # oplog apply ok
                    status_manager.oplog_use_finish(stat[0], stat[1],
                                                    ts_res, False)
                    res= 0
                else: # error
                    status_manager.oplog_use_finish(stat[0], stat[1], None, True)
                    res = -1
            except Exception, e:
                getLogger(__name__).error(e, exc_info=True)
                getLogger(__name__).error('ROLLBACK CLOSE')
                psql_main.conn.rollback()
                reinit_conn(psql_settings, psql_qmetl, status_manager)
                status_manager.oplog_use_finish(None, None, None, True)
                res = -1
        else:
            # initial load is not performed 
            # or not time_end for any other state, or error, do exit
            res = -1
    else:
        # initial load is not performed 
        res = -1

    getLogger(__name__).info('exiting with code %d' % res)
    return res

if __name__ == "__main__":
    main()

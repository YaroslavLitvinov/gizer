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

from os import system
import psycopg2
import argparse
import configparser
from collections import namedtuple
from mongo_reader.reader import MongoReader
from mongo_reader.reader import mongo_reader_from_settings
from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.etlstatus_table import STATUS_INITIAL_LOAD
from gizer.etlstatus_table import STATUS_OPLOG_SYNC
from gizer.etlstatus_table import STATUS_OPLOG_APPLY
from gizer.etlstatus_table import PsqlEtlStatusTable
from gizer.etlstatus_table import PsqlEtlStatusTableManager
from gizer.oplog_highlevel import OplogHighLevel
from gizer.psql_requests import PsqlRequests
from gizer.psql_requests import psql_conn_from_settings
from gizer.opconfig import psql_settings_from_config
from gizer.opconfig import mongo_settings_from_config


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

    mongo_settings = mongo_settings_from_config(config, 'mongo')
    oplog_settings = mongo_settings_from_config(config, 'mongo-oplog')
    psql_settings = psql_settings_from_config(config, 'psql')
    tmp_psql_settings = psql_settings_from_config(config, 'tmp-psql')

    mongo_readers = {}
    schema_engines = get_schema_engines_as_dict(schemas_path)
    for collection_name in schema_engines:
        reader = mongo_reader_from_settings(mongo_settings, collection_name, {})
        mongo_readers[collection_name] = reader
    oplog_reader = mongo_reader_from_settings(oplog_settings, 'oplog.rs', {})

    psql_main = PsqlRequests(psql_conn_from_settings(psql_settings))
    psql_op = PsqlRequests(psql_conn_from_settings(tmp_psql_settings))

    status_table = PsqlEtlStatusTable(psql_main.cursor, 
                                      config['psql']['psql-schema-name'])
    status_manager = PsqlEtlStatusTableManager(status_table)

    tmp_schema = config['tmp-psql']['tmp-psql-operational-schema-name']
    main_schema = config['psql']['psql-schema-name']

    res = 0
    status = status_table.get_recent()
    if status:
        if status.status == STATUS_INITIAL_LOAD \
           and status.time_end and not status.error:
            # intial load done, now do oplog sync, in this stage will be used
            # temporary psql instance as result of operation is not a data
            # commited to DB, but only single timestamp from oplog.
            # save oplog sync status
            print "do_oplog_sync"
            status_manager.oplog_sync_start(status.ts)

            ohl = OplogHighLevel(psql_op, mongo_readers, oplog_reader,
                 schemas_path, schema_engines, tmp_schema, main_schema)
            ts = ohl.do_oplog_sync(status.ts)
            if ts: # sync ok
                status_manager.oplog_sync_finish(ts, False)
                res = 0
            else: # error
                status_manager.oplog_sync_finish(None, True)
                res = -1

        elif (status.status == STATUS_OPLOG_SYNC or \
              status.status == STATUS_OPLOG_APPLY) \
            and status.time_end and not status.error:
            # sync done, now apply oplog pacthes to main psql
            # save oplog sync status
            print "do_oplog_use"
            status_manager.oplog_use_start(status.ts)
            ohl = OplogHighLevel(psql_main, mongo_readers, oplog_reader,
                 schemas_path, schema_engines, main_schema, None)
            ts_res = ohl.apply_oplog_recs_after_ts(status.ts)
            if ts_res.res: # oplog apply ok
                status_manager.oplog_use_finish(ts_res.ts, False)
            else: # error
                status_manager.oplog_use_finish(ts_res.ts, True)
        else:
            # initial load is not performed 
            # or not time_end for any other state, or error, do exit
            res = -1
    else:
        # initial load is not performed 
        res = -1

    print res
    return res

if __name__ == "__main__":
    main()

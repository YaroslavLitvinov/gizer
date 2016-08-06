#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from os import system
import psycopg2
import argparse
import configparser
from pymongo import DESCENDING
from collections import namedtuple
from datetime import datetime
from mongo_reader.reader import mongo_reader_from_settings
from gizer.psql_requests import PsqlRequests
from gizer.psql_requests import psql_conn_from_settings
from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.etlstatus_table import PsqlEtlStatusTable
from gizer.etlstatus_table import PsqlEtlStatusTableManager
from gizer.etlstatus_table import STATUS_INITIAL_LOAD
from gizer.etlstatus_table import STATUS_OPLOG_SYNC
from gizer.etlstatus_table import STATUS_OPLOG_APPLY
from gizer.opconfig import psql_settings_from_config
from gizer.opconfig import load_mongo_replicas_from_setting

def getargs():
    """ get args from cmdline """
    default_request = '{}'
    parser = argparse.ArgumentParser()

    parser.add_argument("-psql-schema-name", help="", type=str)
    parser.add_argument("-psql-table-name-prefix", help="", type=str)

    args = parser.parse_args()
    return args


def main():
    """ main """

    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", action="store",
                        help="Config file with settings",
                        type=file, required=True)
    parser.add_argument("-init-load-status", action="store_true",
                        help="will get exit status=0 if init load not needed,\
or status=-1 if otherwise; Also print 1 - if in progress, 0 - if not.")
    parser.add_argument("-init-load-start-save-ts", action="store_true",
                        help='Save latest oplog timestamp to psql etlstatus table')
    parser.add_argument("-init-load-finish",
                        help='values are: "ok" or "error"', type=str)
    args = parser.parse_args()
    
    config = configparser.ConfigParser()
    config.read_file(args.config_file)

    psql_settings = psql_settings_from_config(config, 'psql')
    psql_main = PsqlRequests(psql_conn_from_settings(psql_settings))

    status_table = PsqlEtlStatusTable(psql_main.cursor,
                                      config['psql']['psql-schema-name'])
    res = 0    
    if args.init_load_status:
        status = status_table.get_recent()
        if status:
            if (status.status == STATUS_OPLOG_SYNC or \
                status.status == STATUS_OPLOG_APPLY or \
                status.status == STATUS_INITIAL_LOAD) and not status.error:
                delta = datetime.now() - status.time_start
                # if operation is running to long
                if status.time_end:
                    res = 0
                elif delta.total_seconds() < 32400: # < 9 hours
                    res = 0
                    if not status.time_end:
                        print 1
                    else:
                        print 0
                else:
                    # takes to much time -> do init load
                    res = -1
            else:
                # error status -> do init load
                res = -1
        else:
            # empty status table -> do init load
            res = -1
    elif args.init_load_start_save_ts:
        # create oplog read transport/s to acquire ts
        oplog_settings = load_mongo_replicas_from_setting(config, 'mongo-oplog')
        ts_list = []
        for oplog_name, settings_list in oplog_settings.iteritems():
            print 'Fetch timestamp from oplog: %s' % oplog_name
            # settings list is a replica set (must be at least one in list)
            reader = mongo_reader_from_settings(settings_list, 'oplog.rs', {})
            reader.connauthreq()
            collection = reader.client['local']['oplog.rs']
            cursor = collection.find({})
            cursor.sort('ts', DESCENDING)
            cursor.limit(1)
            obj = cursor.next()
            ts_list.append( str(obj['ts']) )
            print 'get ts: %s from oplog: %s' % (str(obj['ts']), oplog_name)
        max_ts = sorted(ts_list)[-1]
        print "Initload timestamp:", max_ts

        status_manager = PsqlEtlStatusTableManager(status_table)
        status_manager.init_load_start(max_ts)
    elif args.init_load_finish:
        status_manager = PsqlEtlStatusTableManager(status_table)
        if args.init_load_finish == "ok":
            status_manager.init_load_finish(False) # ok
        else:
            status_manager.init_load_finish(True) # error

    return res


if __name__ == "__main__":
    exit(main())

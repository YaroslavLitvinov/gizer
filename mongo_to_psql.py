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
from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.etlstatus_table import STATUS_INITIAL_LOAD
from gizer.etlstatus_table import STATUS_OPLOG_SYNC
from gizer.etlstatus_table import STATUS_OPLOG_APPLY
from gizer.etlstatus_table import PsqlEtlStatusTable
from gizer.etlstatus_table import PsqlEtlStatusTableManager
from gizer.oplog_sync import do_oplog_sync
from gizer.oplog_parser import apply_oplog_recs_after_ts
from gizer.psql_requests import PsqlRequests

MongoSettings = namedtuple('MongoSettings',
                           ['ssl', 'host', 'port', 'dbname',
                            'user', 'passw'])
PsqlSettings = namedtuple('PsqlSettings',
                          ['host', 'port', 'dbname',
                           'user', 'passw', 
                           'schema', 'operational_schema'])


class SectionKey:
    def __init__(self, section_name):
        self.section_name = section_name
    def key(self, base_key_name):
        return "%s-%s" % (self.section_name, base_key_name)

def sectkey(section_name, base_key_name):
    """ Return key config value. Key name in file must be concatenation 
    of both params divided by hyphen """
    return "%s-%s".format(section_name, base_key_name)

def mongo_settings_from_config(config, section_name):
    mongo = SectionKey(section_name)
    conf = config[section_name]
    return MongoSettings(ssl=conf[mongo.key('ssl')],
                         host=conf[mongo.key('host')],
                         port=conf[mongo.key('port')],
                         dbname=conf[mongo.key('dbname')],
                         user=conf[mongo.key('user')],
                         passw=conf[mongo.key('pass')])

def psql_settings_from_config(config, section_name):
    psql = SectionKey(section_name)
    conf = config[section_name]
    return PsqlSettings(host=conf[psql.key('host')],
                        port=conf[psql.key('port')],
                        dbname=conf[psql.key('dbname')],
                        user=conf[psql.key('user')],
                        passw=conf[psql.key('pass')],
                        schema=conf[psql.key('schema-name')],
                        operational_schema\
                        =conf[psql.key('operational-schema-name')])

def psql_conn(settings):
    psql_fmt = "host={host} port={port} "
    psql_fmt += "dbname={dbname} user={user} password={passw}"
    psql_str = psql_fmt.format(host=settings.host,
                               port=settings.port,
                               dbname=settings.dbname,
                               user=settings.user,
                               passw=settings.passw)
    return psycopg2.connect()


def mongo_reader(settings, collection_name, request):
    return MongoReader(settings.ssl,
                       settings.host,
                       settings.port,
                       settings.dbname,
                       collection_name,
                       settings.user,
                       settings.passw,
                       request)

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

    #parser = argparse.ArgumentParser()
    mongo_settings = mongo_settings_from_config(config, 'mongo')
    psql_settings = psql_settings_from_config(config, 'psql')
    tmp_psql_settings = psql_settings_from_config(config, 'tmp-psql')

    mongo_readers = {}
    schema_engines = get_schema_engines_as_dict(config['misc']['schemas-dir'])
    for collection_name in schema_engines:
        reader = mongo_reader(mongo_settings, collection_name, '{}')
        mongo_readers[collection_name] = reader
    oplog_reader = mongo_reader(mongo_settings, 'oplog.rs', '{}')

    print psql_settings
    psql_main = PsqlRequests(psql_conn(psql_settings))
    psql_op = PsqlRequests(psql_conn(tmp_psql_settings))

    status_table = PsqlEtlTable(psql_main.cursor, 
                                config['psql']['psql-schema-name'])
    status_manager = PsqlEtlStatusTableManager(status_table)

    tmp_schema = config['operational-psql']['operational-psql-schema']
    main_schema = config['psql']['psql-schema']
    
    res = 0
    status = status_table.get_recent_status()
    if status:
        if status.status == STATUS_INITIAL_LOAD \
           and status.time_end and not status.error:
            # intial load done, now do oplog sync, in this stage will be used
            # temporary psql instance as result of operation is not a data
            # commited to DB, but only single timestamp from oplog.
            # save oplog sync status
            status_manager.oplog_sync_start(status.ts)
            ts = do_oplog_sync(status.ts, psql_op, tmp_schema, main_schema,
                               oplog_reader, mongo_readers, args.schemas_path)
            if ts: # sync ok
                status_manager.oplog_sync_finish(ts, False)
                res = 0
            else: # error
                status_manager.oplog_sync_finish(None, True)
                res = -1

        elif (status.status == STATUS_OPLOG_SYNC or \
              status.status == STATUS_OPLOG_USE) \
            and status.time_end and not status.error:
            # sync done, now apply oplog pacthes to main psql
            # save oplog sync status
            status_manager.oplog_use_start(status.ts)
            ts_res = apply_oplog_recs_after_ts(status.ts, 
                                               psql_main, 
                                               mongo_readers, 
                                               oplog_reader, 
                                               args.schemas_path,
                                               main_schema)
            if ts_res.res: # oplog apply ok
                status_manager.oplog_use_finish(ts_res.ts, False)
            else: # error
                status_manager.oplog_use_finish(ts_res.ts, True)
        else:
            # initial load is not performed 
            # or not time_end for any other state, or error, do exit
            res = -1

    return res

if __name__ == "__main__":
    main()

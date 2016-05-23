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
import configparser
import psycopg2
from collections import namedtuple
from mongo_reader.reader import MongoReader
from gizerp.psql_requests import PsqlRequests
from gizer.all_schema_engines import get_schema_engines_as_dict
from gizer.etlstatus_table import STATUS_INITIAL_LOAD
from gizer.etlstatus_table import STATUS_OPLOG_SYNC
from gizer.etlstatus_table import STATUS_OPLOG_APPLY
from gizer.etlstatus_table import PsqlEtlStatusTable
from gizer.etlstatus_table import PsqlEtlStatusTableManager
from gizer.oplog_sync import do_oplog_sync
from gizer.oplog_parser import apply_oplog_recs_after_ts

def getargs():
    """ get args from cmdline """
    default_request = '{}'
    parser = argparse.ArgumentParser()

    parser.add_argument("--schemas_path", action="store",
                        help="Path with js schemas of mongodb",
                        required=True)
    parser.add_argument("-js-request",
                        help='Mongo db search request in json format. \
default=%s' % (default_request),
                        type=str)
    parser.add_argument("-psql-schema-name", help="", type=str)
    parser.add_argument("-psql-table-name-prefix", help="", type=str)
    parser.add_argument("--ddl-statements-file",
                        help="File to save create table statements",
                        type=argparse.FileType('w'), required=True)
    parser.add_argument("-stats-file",
                        help="File to write written record counts",
                        type=argparse.FileType('w'))
    parser.add_argument("--csv-path",
                        help="base path for results",
                        type=str, required=True)

    args = parser.parse_args()
    if args.js_request is None:
        args.js_request = default_request

    return args


class SectionKey:
    def __init__(self, section_name):
        self.section_name = section_name
    def key(self, base_key_name):
        return "%s-%s".format(self.section_name, base_key_name)

def sectkey(section_name, base_key_name):
    """ Return key config value. Key name in file must be concatenation 
    of both params divided by hyphen """
    return "%s-%s".format(section_name, base_key_name)

def mongo_config(config, section_name):
    mongo = SectionKey(section_name)
    mongo_config = config[section_name]
    psql_str = psql_fmt.format(host=conf[psql.key('host')],
                               port=conf[psql.key('port')],
                               dbname=conf[psql.key('dbname')],
                               user=conf[psql.key('user')],
                               pass=conf[psql.key('pass')])


def psql_conn(config, section_name):
    conf = config[self.section_name]
    psql = SectionKey(section_name)
    psql_fmt = "host={host} port={port} "
    psql_fmt += "dbname={dbanme} user={user} password={pass}"
    psql_str = psql_fmt.format(host=conf[psql.key('host')],
                               port=conf[psql.key('port')],
                               dbname=conf[psql.key('dbname')],
                               user=conf[psql.key('user')],
                               pass=conf[psql.key('pass')])
    conn = psycopg2.connect()

def mongo_reader(config, section_name, collection_name, request):
    conf = config[section_name]
    mongo = SectionKey(section_name)
    return MongoReader(conf.getboolean(mongo.key('ssl')),
                       conf[mongo.key('host')],
                       conf[mongo.key('port')],
                       mongo['mongo-dbname'],
                       collection_name,
                       conf[mongo.key('user')],
                       conf[mongo.key('pass')],
                       request)
    

def run_initial_load(config, collection_name):
    conf = config[section_name]
    mongo = SectionKey(section_name)
    fmt = 'python mongo_reader.py --host {host} -cn {dbname}.{collection_name} -user {user} -passw {pass} -ifs {schema_file}.json -psql-schema-name {schema_name} --ddl-statements-file {collection_name}.sql --csv-path {tmp_dir} -psql-table-name-prefix 2016_04_11_ -stats-file quotes.stat'
    cmd = fmt.format(host=conf[mongo.key('host')],
                     dbname=conf[mongo.key('dbname')],
                     collection_name=collection_name,
                     user=conf[mongo.key('user')],
                     pass=conf[mongo.key('pass')],
                     schema_file={schemas-dir}/{collection_name}
                     schema_name=os.path.join(config['misc']['schemas-dir'],
                                              collection_name),
                     tmp_dir=config['misc']['tmp-dir'],
                     
                      
            ))
    if conf[mongo.key('ssl')].getboolean():
        cmd += ' --ssl'

def main():
    """ main """

    config = configparser.ConfigParser()
    config.read_file(open('config.ini'))

    args = getargs()

    mongo_readers = {}
    schema_engines = get_schema_engines_as_dict(args.schemas_path)
    for collection_name in schema_engines:
        reader = mongo_reader(config, 'mongo', collection_name, '{}')
        mongo_readers[collection_name] = reader
    oplog_reader = mongo_reader(config, 'mongo', 'oplog.rs', '{}')

    psql_main = PsqlRequests(psql_conn(config, 'psql'))
    psql_op = PsqlRequests(psql_conn(config, 'operational-psql'))

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
            # intial load done, now do oplog sync
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

        elif (status.status == STATUS_OPLOG_SYNC \
              or status.status == STATUS_OPLOG_USE) \
            and status.time_end and not status.error)):
            # sync done, now do oplog pacthes apply
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

    elif status and status.status == STATUS_INITIAL_LOAD:
        # it's can't use oplog as initial load is not performed
        # ts was saved previously, so already prepared to start initial load
        return -1
    else:
        # it's can't use oplog as initial load is not performed
        # and ts was not saved yet (step just before initial load)
        status = PsqlEtlTable.Status()
        status_table.save_status(status)
        return -1
if __name__ == "__main__":
    main()

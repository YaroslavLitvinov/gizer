#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from logging import getLogger
from collections import namedtuple
from datetime import datetime
from bson.json_util import loads

# initial load completed --init-load-save-ts ts, status=1
# init load fail --init-load-fail, status=0
# sync ok --ts-sync-ok ts, status=2
# sync bad --ts-sync-fail ts, status=0
# oplog handle ok --ts-hanlde-ok ts, status=3
# oplog handle bad --ts-hanlde-fail ts, status=0

STATUS_INITIAL_LOAD = 0
STATUS_OPLOG_SYNC = 1
STATUS_OPLOG_APPLY = 2

def timestamp_str_to_object(timestamp_str):
    """ Return bson Timestamp object
    timestamp_str -- str like 'Timestamp(1464278289, 1)' """
    if timestamp_str == 'None':
        return None
    if timestamp_str and len(timestamp_str):
        spl = timestamp_str.split(',')
        tmstmp = spl[0].split('(')[1].strip()
        increm = spl[1].split(')')[0].strip()
        fmt = '{"$timestamp": {"t": %s, "i": %s}}'
        res_str = fmt % (tmstmp, increm)
        return loads(res_str)
    else:
        return None

def add_update_arg(values_str, name, value):
    """ update query helper """
    fmt = '{name}={value} '
    if value is not None:
        if len(values_str):
            values_str += ', '
        if type(value) is str or type(value) is datetime:
            value = "'" + (str(value)) + "'"
        values_str += fmt.format(name=name, value=value)
    return values_str


class PsqlEtlStatusTable(object):
    Status = namedtuple('Status', ['comment',
                                   'time_start',
                                   'time_end',
                                   'recs_count',
                                   'queries_count',
                                   'ts',
                                   'status',
                                   'error'])
    # Status.ts - is a list or just bson.BSON.Timestamp() python object,
    # which is stored in DB as string. In case if ts is a list of timestamps
    # then delimeter ';' will be used when saving string representation to DB.
    # for example: "Timestamp(1464278289, 1)" or
    # "Timestamp(1464278289, 1); Timestamp(1464278289, 2)"

    def __init__(self, cursor, schema_name, shards_list, recreate=False):
        self.cursor = cursor
        self.shards_list = sorted(shards_list)
        if len(schema_name):
            self.schema_name = schema_name + '.'
        else:
            self.schema_name = ''
        if recreate:
            self.drop_table()
        self.create_table()

    def replace_conn(self, psql_requests):
        self.cursor = psql_requests.cursor

    def drop_table(self):
        fmt = 'DROP TABLE IF EXISTS {schema}qmetlstatus;'
        self.cursor.execute(fmt.format(schema=self.schema_name))

    def create_table(self):
        fmt = 'CREATE TABLE IF NOT EXISTS {schema}qmetlstatus (\
        "comment" TEXT, "time_start" TIMESTAMP, "time_end" TIMESTAMP, \
        "recs_count" INT, "queries_count" INT,\
        "ts" TEXT, "status" INT, "error" BOOLEAN);'
        self.cursor.execute(fmt.format(schema=self.schema_name))

    def ts_str(self, bson_ts):
        """ bson_ts either bson Timestamp or dict with timestamps """
        res = ''
        if type(bson_ts) is dict:
            sorted_keys = sorted(bson_ts.keys())
            for key in sorted_keys:
                if res:
                    res += ';'
                res += self.ts_str(bson_ts[key])
        else:
            res = str(bson_ts)
        return res

    def ts_from_str(self, ts_str):
        ts_list = ts_str.split(';')
        assert len(ts_list) == len(self.shards_list)
        if len(ts_list) == 1:
            ts = timestamp_str_to_object(ts_list[0])
        else:
            ts = {}
            for idx in xrange(len(ts_list)):
                ts[self.shards_list[idx]] \
                    = timestamp_str_to_object(ts_list[idx])
        return ts

    def get_recent(self):
        fmt = 'SELECT * from {schema}qmetlstatus order by time_start \
desc limit 1;'
        self.cursor.execute(fmt.format(schema=self.schema_name))
        rec = self.cursor.fetchone()
        if rec is not None:
            return PsqlEtlStatusTable.Status(comment=rec[0],
                                             time_start=rec[1],
                                             time_end=rec[2],
                                             recs_count=rec[3],
                                             queries_count=rec[4],
                                             ts=self.ts_from_str(rec[5]),
                                             status=rec[6],
                                             error=rec[7])
        else:
            return None

    def save_new(self, status):
        fmt = 'INSERT INTO {schema}qmetlstatus VALUES(\
%s, %s, %s, %s, %s, %s, %s, %s);'
        operation_str = fmt.format(schema=self.schema_name)
        self.cursor.execute(operation_str,
                            (status.comment,
                             status.time_start,
                             status.time_end,
                             status.recs_count,
                             status.queries_count,
                             self.ts_str(status.ts),
                             status.status,
                             status.error))
        self.cursor.execute('COMMIT;')

    def update_latest(self, recs_count, queries_count, time_end, ts, error):
        """ time_end, ts, error """
        fmt1 = 'UPDATE {schema}qmetlstatus SET {values} \
WHERE time_start = (select max(time_start) from {schema}qmetlstatus);'
        values = ''
        values = add_update_arg(values, 'time_end', time_end)
        if recs_count is not None:
            values = add_update_arg(values, 'recs_count', recs_count)
        if queries_count is not None:
            values = add_update_arg(values, 'queries_count', queries_count)
        if ts:
            values = add_update_arg(values, 'ts', self.ts_str(ts))
        values = add_update_arg(values, 'error', error)
        res = fmt1.format(schema=self.schema_name, values=values)
        getLogger(__name__).info('qmetlstatus update query: %s', res)
        self.cursor.execute(res)
        self.cursor.execute('COMMIT;')


class PsqlEtlStatusTableManager(object):
    def __init__(self, status_table):
        self.status_table = status_table
    def init_load_start(self, oplog_ts):
        """ ts -- caller must provide actual latest oplog ts """
        status = PsqlEtlStatusTable.Status(comment='init load',
                                           time_start=datetime.now(),
                                           time_end=None,
                                           recs_count=None,
                                           queries_count=None,
                                           ts=oplog_ts,
                                           status=STATUS_INITIAL_LOAD,
                                           error=None)
        self.status_table.save_new(status)

    def init_load_finish(self, is_error):
        self.status_table.update_latest(recs_count=None,
                                        queries_count=None,
                                        time_end=datetime.now(),
                                        ts=None,
                                        error=is_error)

    def oplog_sync_start(self, ts):
        """ ts -- ts is sync point to start sync.
        from latest record from etl status table """
        status = PsqlEtlStatusTable.Status(comment='oplog sync',
                                           time_start=datetime.now(),
                                           time_end=None,
                                           recs_count=None,
                                           queries_count=None,
                                           ts=ts,
                                           status=STATUS_OPLOG_SYNC,
                                           error=None)
        self.status_table.save_new(status)

    def oplog_sync_finish(self, recs_count, queries_count, ts, is_error):
        self.status_table.update_latest(recs_count=recs_count,
                                        queries_count=queries_count,
                                        time_end=datetime.now(),
                                        ts=ts,
                                        error=is_error)

    def oplog_use_start(self, ts):
        """ ts -- lates succesfully handled ts """
        status = PsqlEtlStatusTable.Status(comment='oplog use',
                                           time_start=datetime.now(),
                                           time_end=None,
                                           recs_count=None,
                                           queries_count=None,
                                           ts=ts,
                                           status=STATUS_OPLOG_APPLY,
                                           error=None)
        self.status_table.save_new(status)

    def oplog_use_finish(self, recs_count, queries_count, ts, is_error):
        self.oplog_sync_finish(recs_count, queries_count, ts, is_error)


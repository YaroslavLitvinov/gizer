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
    if timestamp_str and len(timestamp_str):
        spl = timestamp_str.split(',')
        tmstmp = spl[0].split('(')[1].strip()
        increm = spl[1].split(')')[0].strip()
        fmt = '{"$timestamp": {"t": %s, "i": %s}}'
        res_str = fmt % (tmstmp, increm)
        return loads(res_str)
    else:
        return None

class PsqlEtlStatusTable:
    Status = namedtuple('Status', ['comment', 'time_start', 'time_end',
                                   'recs_count', 'queries_count',
                                   'ts', 'status', 'error'])
    # Status.ts - is a bson.BSON.Timestamp() python object, but in db will 
    # be saved as string, for example: "Timestamp(1464278289, 1)"
    # bson object can be loaded from: {"$timestamp": {"t": 1464278289, "i": 1}}

    def __init__(self, cursor, schema_name, recreate=False):
        self.cursor = cursor
        if len(schema_name):
            self.schema_name = schema_name + '.'
        if recreate:
            self.drop_table()
        self.create_table()

    def drop_table(self):
        fmt = 'DROP TABLE IF EXISTS {schema}qmetlstatus;'
        self.cursor.execute( fmt.format(schema=self.schema_name) )
        
    def create_table(self):
        fmt = 'CREATE TABLE IF NOT EXISTS {schema}qmetlstatus (\
        "comment" TEXT, "time_start" TIMESTAMP, "time_end" TIMESTAMP, \
        "recs_count" INT, "queries_count" INT,\
        "ts" TEXT, "status" INT, "error" BOOLEAN);'
        self.cursor.execute( fmt.format(schema=self.schema_name) )
        fmt = "COMMENT ON COLUMN {schema}qmetlstatus.status IS \
'0 - oplog ts is saved in order to prepare to initial load, \
1 - next must do initial load, \
2 - next must do oplog sync, \
3 - next must apply oplog ops.';"
        self.cursor.execute( fmt.format(schema=self.schema_name) )
        fmt = "COMMENT ON COLUMN {schema}qmetlstatus.ts IS \
        'oplog timestamp';"
        self.cursor.execute( fmt.format(schema=self.schema_name) )


    def get_recent(self):
        fmt = 'SELECT * from {schema}qmetlstatus order by time_start \
desc limit 1;'
        self.cursor.execute( fmt.format(schema=self.schema_name) )
        rec = self.cursor.fetchone()
        if rec is not None:
            tmstmp = timestamp_str_to_object(rec[5])
            return PsqlEtlStatusTable.Status(comment=rec[0], 
                                             time_start=rec[1], 
                                             time_end=rec[2], 
                                             recs_count=rec[3],
                                             queries_count=rec[4],
                                             ts=tmstmp,
                                             status=rec[6], 
                                             error=rec[7])
        else:
            return None
        
    def save_new(self, status):
        fmt = 'INSERT INTO {schema}qmetlstatus VALUES(\
%s, %s, %s, %s, %s, %s, %s, %s);'
        operation_str = fmt.format(schema=self.schema_name)
        self.cursor.execute( operation_str,
                             (status.comment, 
                              status.time_start,
                              status.time_end,
                              status.recs_count, 
                              status.queries_count,
                              str(status.ts),
                              status.status,
                              status.error) )
        self.cursor.execute('COMMIT;')

    def add_update_arg(self, values_str, name, value):
        fmt = '{name}={value} '
        if value is not None:
            if len(values_str):
                values_str += ', '
            if type(value) is str or type(value) is datetime:
                value = "'" + (str(value)) + "'"
            values_str += fmt.format(name=name, value=value)
        return values_str

    def update_latest(self, recs_count, queries_count, time_end, ts, error):
        """ time_end, ts, error """
        fmt1 = 'UPDATE {schema}qmetlstatus SET {values} \
WHERE time_start = (select max(time_start) from {schema}qmetlstatus);'
        values = ''
        values = self.add_update_arg(values, 'time_end', time_end)
        if recs_count is not None:
            values = self.add_update_arg(values, 'recs_count', recs_count)
        if queries_count is not None:
            values = self.add_update_arg(values, 'queries_count', queries_count)
        if ts:
            values = self.add_update_arg(values, 'ts', str(ts))
        values = self.add_update_arg(values, 'error', error)
        res = fmt1.format(schema=self.schema_name, values=values)
        getLogger(__name__).info('qmetlstatus update query: %s' % res)
        self.cursor.execute( res )
        self.cursor.execute('COMMIT;')


class PsqlEtlStatusTableManager:
    def __init__(self, status_table):
        self.status_table = status_table
    def init_load_start(self, latest_oplog_ts):
        """ ts -- caller must provide actual latest oplog ts """
        status = PsqlEtlStatusTable.Status(comment='init load',
                                           time_start=datetime.now(),
                                           time_end=None,
                                           recs_count=None,
                                           queries_count=None,
                                           ts=latest_oplog_ts,
                                           status=STATUS_INITIAL_LOAD,
                                           error = None)
        self.status_table.save_new(status)

    def init_load_finish(self, is_error):
        self.status_table.update_latest(recs_count=None,
                                        queries_count=None,
                                        time_end=datetime.now(), 
                                        ts=None, 
                                        error=is_error)
        
    def oplog_sync_start(self, ts_candidate):
        """ ts_candidate -- ts is sync point to start sync.
        from latest record from etl status table """
        status = PsqlEtlStatusTable.Status(comment='oplog sync',
                                           time_start=datetime.now(),
                                           time_end=None,
                                           recs_count=None,
                                           queries_count=None,
                                           ts=ts_candidate,
                                           status=STATUS_OPLOG_SYNC,
                                           error = None)
        self.status_table.save_new(status)

    def oplog_sync_finish(self, ts, is_error):
        self.status_table.update_latest(recs_count=None,
                                        queries_count=None,
                                        time_end=datetime.now(),
                                        ts=ts,
                                        error=is_error)

    def oplog_use_start(self, ts_latest_synced):
        """ ts_latest_synced -- lates succesfully handled ts """
        status = PsqlEtlStatusTable.Status(comment='oplog use',
                                           time_start=datetime.now(),
                                           time_end=None,
                                           recs_count=None,
                                           queries_count=None,
                                           ts=ts_latest_synced,
                                           status=STATUS_OPLOG_APPLY,
                                           error = None)
        self.status_table.save_new(status)

    def oplog_use_finish(self, recs_count, queries_count, ts, is_error):
        # on error ts must be specified, on which fail is occured
        self.status_table.update_latest(recs_count=recs_count,
                                        queries_count=queries_count,
                                        time_end=datetime.now(),
                                        ts=ts, 
                                        error=is_error)


#!/usr/bin/env python

""" Oplog parser, and patcher of end data by oplog operations.
Oplog synchronization with initially loaded data stored in psql.
OplogParser -- class for basic oplog parsing
do_oplog_apply -- handling oplog and applying oplog ops func
sync_oplog -- find syncronization point in oplog for initially loaded data."""

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import bson
import sys
from os import environ
from bson.json_util import loads
from collections import namedtuple
from gizer.oppartial_record import get_tables_data_from_oplog_set_command
from gizer.psql_objects import load_single_rec_into_tables_obj
from gizer.psql_objects import insert_rec_from_one_tables_set_to_another
from gizer.psql_objects import create_psql_tables
from gizer.oplog_handlers import cb_insert
from gizer.oplog_handlers import cb_update
from gizer.oplog_handlers import cb_delete
from gizer.etlstatus_table import timestamp_str_to_object
from gizer.all_schema_engines import get_schema_engines_as_dict
from mongo_reader.prepare_mongo_request import prepare_mongo_request
from mongo_reader.prepare_mongo_request import prepare_oplog_request
from mongo_schema.schema_engine import create_tables_load_bson_data

EMPTY_TS = 'empty_ts'

ItemInfo = namedtuple('ItemInfo', ['schema_name',
                                   'schema_engine',
                                   'ts',
                                   'rec_id'])

def message(mes, cr='\n'):
    sys.stderr.write( mes + cr)

class OplogParser:
    """ parse oplog data, apply oplog operations, execute resulted queries
    and verify patched results """
    def __init__(self, reader, schemas_path,
                 cb_bef, cb_ins, cb_upd, cb_del):
        self.reader = reader
        self.first_handled_ts = None
        self.schema_engines = get_schema_engines_as_dict(schemas_path)
        self.item_info = None
        self.cb_before = cb_bef
        self.cb_insert = cb_ins
        self.cb_update = cb_upd
        self.cb_delete = cb_del
        # fix bug when Self.OplogParser creating multiple times at one session
        if hasattr(cb_before, "ids"):
            cb_before.ids = []

    def next_verified(self):
        """ next oplog records for one of ops=u,i,d """
        item = self.reader.next()
        while item:
            if item['op'] == 'i' or item['op'] == 'u' or item['op'] == 'd':
                return item
            item = self.reader.next()
        return None

    def next(self):
        item = self.next_verified()
        res = None
        if item:
            if self.first_handled_ts is None:
                self.first_handled_ts = item['ts']
            ts_field = item["ts"]
            ns_field = item["ns"]
            o_field = item["o"]
            # get rec_id
            rec_id = None
            if item["op"] == "u":
                rec_id = str(item['o2'].values()[0])
            else:
                if '_id' in item['o']:
                    rec_id = item['o']['_id']
                elif 'id' in item['o']:
                    rec_id = item['o']['id']
                else:
                    assert(0)

            #if type(rec_id) is bson.objectid.ObjectId:
            #    rec_id = str(rec_id)

            db_and_collection = item["ns"].split('.')
            # dbname = db_and_collection[0]
            schema_name = db_and_collection[1]
            if schema_name not in self.schema_engines:
                message("ONLY FOR DEBUG: skip ts=" + str(ts_field) +
                        " for collection " + schema_name)
                return None
            schema = self.schema_engines[schema_name]
            # save rec_id
            self.item_info = ItemInfo(schema_name,
                                      schema,
                                      item['ts'],
                                      rec_id)

            message("op=" + item["op"] + ", ts=" + str(item['ts']) +
                    ", name=" + schema_name + ", rec_id=" + str(rec_id))

            if self.cb_before and self.cb_before.cb:
                self.cb_before.cb(self.cb_before.ext_arg,
                                  schema,
                                  item)
            if item["op"] == "i":
                # insert is ALWAYS expects array of records
                res = self.cb_insert.cb(self.cb_insert.ext_arg,
                                        ts_field, ns_field, schema,
                                        [o_field])
            elif item["op"] == "u":
                o2_id = item["o2"]
                res = self.cb_update.cb(self.cb_update.ext_arg,
                                        schema, item)
            elif item["op"] == "d":
                res = self.cb_delete.cb(self.cb_delete.ext_arg,
                                        ts_field, ns_field, schema,
                                        o_field)
        return res


def cb_before(ext_arg, schema_engine, item):
    """ Needed for oplog syncing.
    When handling oplog records during oplog sync,
    it's can be needed at first to copy data into
    operational database. Hadnled oplog records must exec
    queries in operational database also."""
    if not hasattr(cb_before, "ids"):
        cb_before.ids = []
    psql = ext_arg[0]
    src_schema_name = ext_arg[1]
    dst_schema_name = ext_arg[2]

    tables_obj = create_tables_load_bson_data(schema_engine,
                                              None)
    try:
        if item["op"] == "u":
            rec_id = str(item['o2'].values()[0])
            if rec_id not in cb_before.ids:
                # copy record from main tables to operational
                insert_rec_from_one_tables_set_to_another(psql,
                                                          rec_id,
                                                          tables_obj,
                                                          src_schema_name,
                                                          dst_schema_name)
                cb_before.ids.append(rec_id)
        elif item["op"] == "d":
            rec_id = str(item['o'].values()[0])
            if rec_id not in cb_before.ids:
                # copy record from main tables to operational
                insert_rec_from_one_tables_set_to_another(psql,
                                                          rec_id,
                                                          tables_obj,
                                                          src_schema_name,
                                                          dst_schema_name)
                cb_before.ids.append(rec_id)
        elif item["op"] == "i":
            # do not prepare
            pass
    except:
        # create skeleton of original psql tables as initial load
        # was not executed previously.
        drop = True
        create_psql_tables(tables_obj, psql, src_schema_name, '', drop)


def exec_insert(psql, oplog_query):
    # create new connection and cursor
    query = oplog_query.query
    fmt_string = query[0]
    if 'PG_TIMEZONE' in environ:
        psql.cursor.execute("SET TIMEZONE TO '%s';" % environ['PG_TIMEZONE'] )
    for sqlparams in query[1]:
        psql.cursor.execute(fmt_string, sqlparams)




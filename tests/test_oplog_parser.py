#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import psycopg2
from gizer.psql_requests import PsqlRequests
from gizer.oplog_parser import OplogParser
from gizer.oplog_parser import OplogQuery
from gizer.opinsert import generate_insert_queries
from gizer.opcreate import generate_drop_table_statement
from gizer.opcreate import generate_create_table_statement
from gizer.oppartial_record import get_tables_data_from_oplog_set_command
from mongo_schema.schema_engine import create_tables_load_bson_data

def exec_insert(dbreq, oplog_query):
     query = oplog_query.query
     fmt_string = query[0]
     for sqlparams in query[1]:
          dbreq.cursor.execute(fmt_string, sqlparams)

def test_oplog_parser():

    def test_cb_insert(ts, ns, schema_engine, bson_data):
         tables = create_tables_load_bson_data(schema_engine, bson_data)
         posts_table = tables.tables['posts']
         assert(posts_table)
         assert(posts_table.sql_column_names == [u'body', 
                                                 u'created_at', 
                                                 u'id_bsontype', 
                                                 u'id_oid', 
                                                 u'title', 
                                                 u'updated_at', 
                                                 u'user_id', 
                                                 'idx'])
         res = []
         for name, table in tables.tables.iteritems():
              res.append(OplogQuery("i", generate_insert_queries(table, "", "")))
         return res

    def test_cb_update(ts, ns, schema_engine, bson_data, bson_parent_id):
         # for set.name = "comments" don't neeed max indexes at all,
         # just use default indexes to add data to parent (parent_id)
         # for set.name = "comments.2" use provided index=2
         if ts == '6249012828238249985' or ts == '6249012068029138593':
              # TODO 2016 apr 20: use index of parent record
              # overwrite (insert) compete array
              print bson_data
              tables, initial_indexes \
                  = get_tables_data_from_oplog_set_command(schema_engine, 
                                                           bson_data,
                                                           bson_parent_id)
              res = []
              for name, table in tables.iteritems():
                   res.append(OplogQuery("ui", \
                        generate_insert_queries(table, "", "", initial_indexes)))
              assert(res!=[])
              return res
         else:
              return [OplogQuery("u", "query")]
         
    def test_cb_delete(ts, ns, schema, bson_data):
         return [OplogQuery("d", "query")]

    insert_count = 0
    delete_count = 0
    update_count = 0

    connstr = os.environ['PSQLCONN']
    dbreq = PsqlRequests(psycopg2.connect(connstr))

    p = OplogParser("./test_data/schemas/rails4_mongoid_development",
                    test_cb_insert, test_cb_update, test_cb_delete)
    p.load_file('test_data/test_oplog.js')

    #create/truncate psql table
    posts_no_data = create_tables_load_bson_data(p.schema_engines['posts'], None)
    for table_name, table in posts_no_data.tables.iteritems():
        drop_table = generate_drop_table_statement(table, '', '')
        create_table = generate_create_table_statement(table, '', '')
        dbreq.cursor.execute(drop_table)
        dbreq.cursor.execute(create_table)

    #go over oplog, handle oplog records and execute final queries
    oplog_queries = p.next()
    while oplog_queries != None:
         for oplog_query in oplog_queries:
              print oplog_query
              if oplog_query.op == "i":
                   insert_count += 1
              elif oplog_query.op == "u" or oplog_query.op == "ui":
                   update_count += 1
              elif oplog_query.op == "d":
                   delete_count += 1
              if oplog_query.op == "i" or oplog_query.op == "ui":
                   exec_insert(dbreq, oplog_query)
         oplog_queries = p.next()

    assert(insert_count==1)
    assert(delete_count==1)
    assert(update_count==8)

    dbreq.cursor.execute('COMMIT')
    

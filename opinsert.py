#!/usr/bin/env python

import json
import bson
from bson.json_util import loads
from bson_processing import BsonProcessing


def callback_internal(table, schema, data, fieldname):
    res = []
    columns=[]
    #print 'callback', schema, data
    for type_item in schema:
        if type(schema[type_item]) is dict:
            for dict_item in schema[type_item]:
                columns.append('_'.join([fieldname, dict_item]))
        elif type(schema[type_item]) is not list:
            columns.append(type_item)
    for record in data:
        values=[]
        for type_item in schema:
            if type(schema[type_item]) is dict:
                for dict_item in schema[type_item]:
                    values.append(record[type_item][dict_item])
            elif type(schema[type_item]) is not list:
                values.append(str(record[type_item]))
        res.append( "INSERT INTO {table}({columns})\nVALUES({values});"\
                    .format(table=table, 
                            columns=','.join(columns), 
                            values=','.join(values)))
    return res


def get_insert_queries(ns, schema, objdata):
    """ Get sql insert statements based on args.
    Args: 
    ns -- database.collection name
    schema -- json schema
    objdata - js data"""

    res = []
    bt = BsonProcessing(callback_internal, res)
    collection_name = ns.split('.')[1]
    tables = {}
    bt.get_tables_structure([schema], [objdata], collection_name, "", tables)
    return res


def opinsert_callback(ns, schema, objdata):
    return get_insert_queries(ns, schema, objdata)

   



if __name__ == "__main__":
    pass

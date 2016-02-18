#!/usr/bin/env python

import json
import bson
from bson.json_util import loads
from bson_processing import BsonProcessing


def callback_internal(table, schema, data, fieldname):
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
        print "INSERT INTO {table}({columns})\nVALUES({values});"\
            .format(table=table, columns=','.join(columns), values=','.join(values))



def opinsert_callback(ns, schema, objdata):
    """ Get sql insert statements based on args.
    Args: 
    ns -- database.collection name
    schema -- json schema
    objdata - js data"""

    bt = BsonProcessing(callback_internal)
    collection_name = ns.split('.')[1]
    tables = {}
    #print collection_name, [schema], [objdata]
    bt.get_tables_structure([schema], [objdata], collection_name, "", tables)


if __name__ == "__main__":
    pass

#!/usr/bin/env python

import json
import bson
from bson.json_util import loads


def generate_insert_queries(table):
    """ get tuple: (format string, [(list,of,values,as,tuples)]) \
    @param table object schema_engine.SqlTable"""
    queries = []
    fmt_string = "INSERT INTO TABLE %s (%s) VALUES(%s);" \
                 % (table.table_name, \
                    ', '.join(table.sql_column_names), \
                    ', '.join(['%s' for i in table.sql_column_names]))
    firstcolname = table.sql_column_names[0]
    reccount = len(table.sql_columns[firstcolname].values)
    for val_i in xrange(reccount):
        values = [table.sql_columns[i].values[val_i] for i in table.sql_column_names]
        queries.append( tuple(values) )
    return (fmt_string, queries)


if __name__ == "__main__":
    pass

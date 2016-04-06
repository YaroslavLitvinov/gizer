#!/usr/bin/env python
"""Update callback."""

import itertools

from gizer.opinsert import *
from mongo_to_hive_mapping import schema_engine
from util import table_name_from, columns_from, tables_from, column_prefix_from
from mongo_to_hive_mapping.schema_engine import *
from opdelete import op_delete_stmts as delete
from d_utils import *
import collections


UPSERT_TMLPT = """\
LOOP
    {update}
    IF found THEN
        RETURN;
    END IF;
    BEGIN
        {insert}
        RETURN;
    EXCEPTION WHEN unique_violation THEN
    END;
END LOOP;
"""

UPDATE_TMPLT = "UPDATE {table} SET {statements} WHERE {conditions}"


def update(collection, schema, data, obj_id):
    """Update callback."""
    # first we need to detect what kind of update we have
    # options are:
    # 1. plain update of root document
    # 2. plain update of nested document
    # 3. update of nested doc that would require cascade delete of previos records
    # 4. update of nested doc that really insert and not update (append element in array)
    # We could generate 'upsert' for every nested update
    # We could easily see if update requires cascade delete (type of payload = list)
    upd_stmnts = {}
    #print (len(extract_query_objects_from(data, [])))
    for (q, obj) in extract_query_objects_from(data, []):
        q = '.'.join(q[1:])
        # there should be only iteration, but for correctness..
        tables = table_name_from(collection, q)
        indices = indices_from_query(q)
        if not tables:
            # we have simple root collection update
            return generate_update_query(collection, q, obj)
            #raise StopIteration

        # now check for cascade deletes need
        if type(obj) == list:
            # just call stubs, figure out args later
            #for stmt in delete(schema, collection, obj_id):
            generate_insert_queries(tables.tables[collection])
            return delete(schema, collection, obj_id)

            #for stmt in insert(collection, schema, obj):
            #    yield stmt
            #raise StopIteration

        # now we have to generate upsert stmt

        #table_obj = schema_engine.create_tables_load_bson_data(SchemaEngine(collection, schema), data['$set']).tables[collection]
        #print(table_obj.tables.viewitems())
        print('gen query')
        print(data)
        print(q)
        print(obj)
        print(collection)
        print(indices)
        print(tables)
        return {'upd':{generate_update_query(collection, q, obj):[obj[k] for k in obj] + [obj_id] + indices}}
        # yield UPSERT_TMLPT.format(
        #     update=generate_update_query(collection, q, obj),
        #     insert='\n'#.join(flatten(generate_insert_queries(table_obj)))
        # )

# def update_lst(collection, schema, data, obj_id):
#     stmt_list = []
#     for stmt in update(collection, schema, data, obj_id):
#         stmt_list.append(stmt)
#     return stmt

def update_new (schema, oplog_data):
    # plain update of root document
    doc_id = get_obj_id(oplog_data)
    u_data = oplog_data["o"]["$set"]
    target_table_name = oplog_data["ns"]

    k = u_data.iterkeys().next()
    updating_obj = k.split('.')
    # simple object update
    if not updating_obj[-1].isdigit():
        q_columns = get_query_columns_with_nested(schema,u_data, '', {})
        upd_stmnt = UPDATE_TMPLT.format( table = target_table_name, statements = ', '.join(['{column}=%s'.format(column=col) for col in q_columns]), conditions = '{column}=%s'.format(column = doc_id.iterkeys().next()))
        upd_values = [q_columns[col] for col in q_columns] + [doc_id.itervalues().next()]
    if type(u_data[k]) is dict:
        upd_stmnt = 'nested update'
        upd_values = ''
    elif type(u_data[k]) is list:
        upd_stmnt = 'array update'
        upd_values = ''
    return {upd_stmnt:upd_values}


def get_obj_id(oplog_data):
    return get_obj_id_recursive(oplog_data["o2"], [], [])

def get_obj_id_recursive(data, name, value_id):
    if type(data) is dict:
        next_column = data.iterkeys().next()
    name.append(get_field_name_without_underscore(next_column))
    if type(data[next_column]) is dict:
        ret = get_obj_id_recursive(data[next_column], name, value_id)
    else:
        value_id.append(data[next_column])
    return {'_'.join(name):value_id[0]}

def get_query_columns_with_nested (schema, u_data, parent_path, columns_list):
    for k in u_data:
        if parent_path <> '':
            column_name = parent_path + '_' + get_field_name_without_underscore(k)
        else:
            column_name = get_field_name_without_underscore(k)
        if type(u_data[k]) is dict:
            get_query_columns_with_nested(schema, u_data[k], column_name, columns_list).copy()
        else:
            columns_list[column_name] = u_data[k]
    return columns_list

def flatten(listOfLists):
    "Flatten one level of nesting"
    return itertools.chain.from_iterable(listOfLists)



# def delete(collection, obj_id, schema):
#     delete(schema, collection, obj_id)
#     return 'delete stub'


def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks."""
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)


def extract_query_objects_from(payload, query):
    """Generate query, obj pair from payload."""
    if type(payload) == list:
        yield (query, payload)
        raise StopIteration
    obj_children = [(k, v) for k, v in payload.items() if type(v) == dict or type(v) == list]
    if len(obj_children) == 0:
        yield (query, payload)
    else:
        for k, v in obj_children:
            for (q, obj) in extract_query_objects_from(v, query + [k]):
                yield (q, obj)


def tables_from_query(query):
    """Extract all chained tables from query."""
    return map(lambda x: x[0], grouper(query.split('.'), 2)) if query else []


def indices_from_query(query):
    """Extract all chained table indices from query."""
    return map(lambda x: x[1], grouper(query.split('.'), 2)) if query else []


def target_table(collection, query):
    """Target table name from root collection and query."""
    return table_name_from(collection, query)


def generate_update_query(collection, query, payload):
    """Generate update sql query."""
    source_table = target_table(collection, query)
    prefix = column_prefix_from(collection, query)
    set_values = ['{0} = {1}'.format(c, '%s') for c in columns_from(prefix, payload)]

    return 'update {table} set {values} {where}'.format(
        table=source_table,
        values=', '.join(set_values),
        where=generate_where_clause(collection, query)
    )


def to_singular(plural):
    """Convert plural name to singular form."""
    return plural[:-1]


def generate_where_clause(collection, query):
    """Generate WHERE SQL clause."""
    root_table = target_table(collection, query)
    wheres = ['where {table}.{root}_id = %s'.format(
        table=root_table,
        root=to_singular(collection if collection != root_table else '')
    )]

    idx_tables = list(tables_from(collection, query))[:-1]
    print('idx_tables', idx_tables)
    for table in idx_tables:
        wheres.append('{table}.{prev}_idx = %s'.format(
            table=target_table(collection, query),
            prev=table,
        ))

    return ' and '.join(wheres)


def generate_id_select_query(collection, query):
    """Generate SQL query.

    Generates id retrieval SQL query given root collection name
    and mongo update parameter query.
    """
    def abbreviate(table):
        return table[0]

    where_stmt = generate_where_clause(collection, query)

    source_table = target_table(collection, query)
    select_field = '{0}._id'.format(source_table)

    return '''select {field} from {table} {where}'''.format(
        field=select_field,
        table=source_table,
        where=where_stmt
    )

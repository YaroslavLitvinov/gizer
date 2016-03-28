#!/usr/bin/env python
"""Update callback."""

import itertools

from opinsert import get_insert_queries as insert
from util import table_name_from, columns_from, tables_from, column_prefix_from

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


def update(collection, payload, schema):
    """Update callback."""
    # first we need to detect what kind of update we have
    # options are:
    # 1. plain update of root document
    # 2. plain update of nested document
    # 3. update of nested doc that would require cascade delete of previos records
    # 4. update of nested doc that really insert and not update (append element in array)
    # We could generate 'upsert' for every nested update
    # We could easily see if update requires cascade delete (type of payload = list)
    for (q, obj) in extract_query_objects_from(payload, []):
        q = '.'.join(q[1:])
        # there should be only iteration, but for correctness..
        tables = table_name_from(collection, q)
        indices = indices_from_query(q)
        if not tables:
            # we have simple root collection update
            yield generate_update_query(collection, q, obj)
            raise StopIteration

        # now check for cascade deletes need
        if type(obj) == list:
            # just call stubs, figure out args later
            for stmt in delete(collection, obj, schema):
                yield stmt
            for stmt in insert(collection, schema, obj):
                yield stmt
            raise StopIteration

        # now we have to generate upsert stmt
        yield UPSERT_TMLPT.format(
            update=generate_update_query(collection, q, obj),
            insert='\n'.join(flatten(insert('.' + collection, schema, obj)))
        )


def flatten(listOfLists):
    "Flatten one level of nesting"
    return itertools.chain.from_iterable(listOfLists)


def delete(collection, payload, schema):
    """Stub."""
    return 'delete stub'


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

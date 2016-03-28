#!/usr/bin/env
"""Common utils."""

import re


def tables_from(collection, query):
    """Table names from collection and query."""
    params = re.findall(r'(.+?)\.\d+\.?', query)  # split by '.5' pattern
    params = map(lambda x: x.split('.')[0], params)
    buf = []
    for table in [collection] + params:
        buf.append(table)
        yield '_'.join(buf)


def table_name_from(collection, query):
    """Table name from query."""
    return list(tables_from(collection, query))[-1]


def column_prefix_from(collection, query):
    """Column name from query."""
    params = query.split('.')
    for index, el in enumerate(reversed(params)):
        if el.isdigit():
            params = params[len(params) - index:]
            break
    return '_'.join(params)


def columns_from(prefix, obj):
    """Column names for object with prefix."""
    for k, v in obj.items():
        yield '{0}{1}'.format(
            '{0}_'.format(prefix) if prefix else '',
            k
        )

__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"

import string


def get_field_name_without_underscore(field_name):
    for i in range(len(field_name)):
        if field_name[i].isalpha():
            s = field_name[i:]
            break
    return s


def isIdField(field_name):
    if field_name in ['id', 'oid', '_id', '_oid', '_id_oid', 'id_oid']:
        return True
    else:
        return False


def get_postgres_type(type_name):
    # TODO should be case insensitive
    # TODO should be replaced
    return {
        'STRING': 'text',
        'INT': 'integer',
        'BOOL': 'boolean',
        'LONG': 'bigint'
    }[type_name]


def get_table_name_from_list(spath):
    spathl = spath[:]
    for it in spathl:
        if it.isdigit():
            spathl.remove(it)
    if len(spathl) > 1:
        return '_'.join(['_'.join((el[:-1]) for el in spathl[:-1]), spathl[-1]])
    else:
        return spathl[-1]


def get_root_table_from_path(path):
    spath = path.split('.')
    collection_path = []
    remove_last = False
    for it in spath:
        if it.isdigit():
            remove_last = True
            break
        collection_path.append(it)
    if remove_last and 1 < len(collection_path):
        del collection_path[len(collection_path) - 1]
    return '_'.join(collection_path)


def get_indexes_dictionary(path):
    index_dict = {}
    spath = path.split('.')
    iter_i = reversed(xrange(len(spath)))
    for i in iter_i:
        if spath[i].isdigit():
            table_name = get_table_name_from_list(spath)
            index_dict[table_name] = spath[i]
            del spath[i]
            del spath[i - 1]
            next(iter_i)
    return index_dict


def get_last_idx_from_path(path):
    spath = path.split('.')
    if spath[-1].isdigit():
        return spath[-1]
    else:
        return None

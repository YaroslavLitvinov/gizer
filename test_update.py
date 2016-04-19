#!/usb/bin/env python
"""Tests."""
from mongo_to_hive_mapping import schema_engine

from update import *
import textwrap
from mongo_to_hive_mapping.schema_engine import *
from gizer.opinsert import *
from update_test_data import *
import pprint

TEST_INFO = 'TEST_OPUPDATE'

'update post_adresse_streets set name="STREETNAME" where post_id="aabbccddeeff" and post_adresses_idx=0, idx=7'

def test_get_obj_id():
    oplog_data = oplog_u_01
    model = {"id_oid":"56b8da51f9fcee1b00000006"}
    result = get_obj_id(oplog_data)
    assert result == model
    print(TEST_INFO, 'get_obj_id', 'PASSED')


def test_get_obj_id_recursive():
    model = {'aaa_bbb_ccc_ddd_eeee': 'abcdef'}
    assert model == get_obj_id_recursive(test_data_01, [], [])
    print(TEST_INFO, 'get_obj_id_recursive', 'PASSED')


def test_update_new():
    print('######################')
    print(update_new(schema, oplog_u_01))
    print('######################')
    print(update_new(schema, oplog_u_02))
    print('######################')
    print(update_new(schema, oplog_u_03))
    print('######################')
    print(update_new(schema, oplog_u_04))
    print('######################')
    print(update_new(schema, oplog_u_05))
    print('######################')
    print(update_new(schema, oplog_u_06))
    print('######################')
    print(update_new(schema, oplog_u_07))
    print('######################')
    print(update_new(schema, oplog_u_08))


pp = pprint.PrettyPrinter(indent=4)


test_get_obj_id()
test_get_obj_id_recursive()
test_update_new()
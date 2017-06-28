#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016-2017, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"


import sys
import os
import configparser
from io import BytesIO
from pprint import PrettyPrinter
from gizer.opconfig import mongo_settings_from_config
from gizer.opconfig import psql_settings_from_config
from gizer.opconfig import get_structure
from gizer.opconfig import load_mongo_replicas_from_setting

config_file_sections = ['mongo', 'mongo-oplog-shard1-rs1', 'mongo-oplog-shard1-rs2', 'mongo-oplog-shard2-rs1', 'psql', 'psql-tmp', 'bmp-psql', 'misc']
config_file_sections2 = ['mongo', 'mongo-oplog']
config_file_sections3 = ['mongo', 'mongo-oplog-rs1', 'mongo-oplog-rs2']

def test_betterize_coverage_opconfig():
    test_config_file = BytesIO()
    append_psql_setting(test_config_file, 'psql')
    test_config_file.seek(0)
    # config file processing
    config = configparser.ConfigParser()
    config.read_file(test_config_file)
    # test config
    psql_settings_from_config(config, 'psql')

def test_config_sections():
    pp = PrettyPrinter()
    res = get_structure(config_file_sections, '-')
    pp.pprint(res)
    assert(res['mongo'] == ['mongo', 'mongo-oplog'])
    assert(res['mongo-oplog'] == ['mongo-oplog-shard1', 'mongo-oplog-shard2'])
    assert(res['mongo-oplog-shard1'] == ['mongo-oplog-shard1-rs1', 'mongo-oplog-shard1-rs2'])
    assert(res['mongo-oplog-shard2'] == ['mongo-oplog-shard2-rs1'])
    assert(res['misc'] == ['misc'])
    assert(res['psql'] == ['psql', 'psql-tmp'])
    assert(res['bmp'] == ['bmp-psql'])

    res = get_structure(config_file_sections2, '-')
    pp.pprint(res)
    assert(res['mongo'] == ['mongo', 'mongo-oplog'])


    res = get_structure(config_file_sections3, '-')
    pp.pprint(res)
    assert(res['mongo'] == ['mongo', 'mongo-oplog'])
    assert(res['mongo-oplog'] == ['mongo-oplog-rs1', 'mongo-oplog-rs2'])

def append_psql_setting(output, name):
    output_fmt = '\
\n[{name}]\
\n{name}-host={name}\
\n{name}-port=1234\
\n{name}-dbname=test\
\n{name}-schema-name=test_schema\
\n{name}-user=test\
\n{name}-pass=test'
    output.write(output_fmt.format(name=name))


def append_to_file_mongo_setting(output, name):
    output_fmt = '\
\n[{name}]\
\n{name}-host={name}\
\n{name}-params=test\
\n{name}-port=1234\
\n{name}-ssl=0\
\n{name}-dbname=test\
\n{name}-user=test\
\n{name}-pass=test'
    output.write(output_fmt.format(name=name))


def test_config_load1():
    """ test config single mongo instance with single replica """
    print test_config_load1.__name__
    test_config_file = BytesIO()
    append_to_file_mongo_setting(test_config_file, 'mongo-oplog')
    test_config_file.seek(0)
    # config file processing
    config = configparser.ConfigParser()
    config.read_file(test_config_file)
    pp = PrettyPrinter()
    all_settings = load_mongo_replicas_from_setting(config, 
                                                    'mongo-oplog')
    pp.pprint(all_settings)
    assert(all_settings.keys() == ['mongo-oplog'])
    assert(1 == len(all_settings['mongo-oplog']))
    assert('mongo-oplog' == all_settings['mongo-oplog'][0].host)
    try:
        wrong_settings = load_mongo_replicas_from_setting(config, 
                                                          'mongo-uplog')
        testok = 0
    except:
        testok = 1
    assert testok == 1

def test_config_load2():
    """ test config single mongo instance with many replicas """
    print test_config_load2.__name__
    test_config_file = BytesIO()
    append_to_file_mongo_setting(test_config_file, 'mongo-oplog-rs1')
    append_to_file_mongo_setting(test_config_file, 'mongo-oplog-rs2')
    append_to_file_mongo_setting(test_config_file, 'mongo-oplog-rs3')
    test_config_file.seek(0)
    # config file processing
    config = configparser.ConfigParser()
    config.read_file(test_config_file)
    pp = PrettyPrinter()
    all_settings = load_mongo_replicas_from_setting(config, 
                                                    'mongo-oplog')
    pp.pprint(all_settings)
    print all_settings.keys()
    assert(sorted(all_settings.keys()) == sorted(['mongo-oplog']))
    assert(3 == len(all_settings['mongo-oplog']))
    assert('mongo-oplog-rs1' == all_settings['mongo-oplog'][0].host)
    assert('mongo-oplog-rs2' == all_settings['mongo-oplog'][1].host)
    assert('mongo-oplog-rs3' == all_settings['mongo-oplog'][2].host)
    mongo_settings_from_config(config, 'mongo-oplog-rs1')

def test_config_load3():
    """ test config many shards with many replicas """
    print test_config_load3.__name__
    test_config_file = BytesIO()
    # non ordered
    append_to_file_mongo_setting(test_config_file, 'mongo-oplog-shard2-rs1')
    append_to_file_mongo_setting(test_config_file, 'mongo-oplog-shard2-rs2')
    append_to_file_mongo_setting(test_config_file, 'mongo-oplog-shard1-rs1')
    append_to_file_mongo_setting(test_config_file, 'mongo-oplog-shard1-rs2')
    append_to_file_mongo_setting(test_config_file, 'mongo-oplog-shard3-rs1')
    append_to_file_mongo_setting(test_config_file, 'mongo-oplog-shard3-rs2')
    append_to_file_mongo_setting(test_config_file, 'mongo-oplog-shard1-rs3')
    append_to_file_mongo_setting(test_config_file, 'mongo-oplog-shard2-rs3')
    append_to_file_mongo_setting(test_config_file, 'mongo-oplog-shard3-rs3')

    test_config_file.seek(0)
    # config file processing
    config = configparser.ConfigParser()
    config.read_file(test_config_file)
    pp = PrettyPrinter()
    all_settings = load_mongo_replicas_from_setting(config, 
                                                    'mongo-oplog')
    pp.pprint(all_settings)
    assert(3 == len(all_settings.keys()))
    assert(sorted(all_settings.keys()) == \
               sorted(['mongo-oplog-shard1', 
                       'mongo-oplog-shard2', 
                       'mongo-oplog-shard3']))

if __name__ == '__main__':
    test_config_sections()
    test_config_load1()
    test_config_load2()
    test_config_load3()

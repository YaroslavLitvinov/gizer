#!/usr/bin/env python

""" config parsers on top of config object.
For reading oplog/mongo/psql settings """

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from collections import namedtuple

MongoSettings = namedtuple('MongoSettings',
                           ['ssl', 'host', 'port', 'params',
                            'dbname', 'user', 'passw'])
PsqlSettings = namedtuple('PsqlSettings',
                          ['host', 'port', 'dbname',
                           'user', 'passw',
                           'schema'])

class SectionKey(object):
    def __init__(self, section_name):
        self.section_name = section_name
    def key(self, base_key_name):
        return "%s-%s" % (self.section_name, base_key_name)


class StructureAcquire(object):
    def __init__(self, divided_by):
        self.divided_by = divided_by

    def last(self, key):
        splits = key.split(self.divided_by)
        return self.divided_by.join(splits[:-1])

    def collect_recursively(self, key):
        contents = {}
        parent = self.last(key)
        if len(parent):
            contents = self.collect_recursively(parent)
            contents[parent] = key
        else:
            contents[key] = key
        return contents


def get_structure(sections_list, delim):
    res = {}
    sta = StructureAcquire(delim)
    for item in sections_list:
        for key, val in sta.collect_recursively(item).iteritems():
            if key not in res:
                res[key] = [val]
            elif val not in res[key]:
                res[key].append(val)
    return res

def get_config_structure(config):
    return get_structure(config.sections(), '-')

def mongo_settings_from_config(config, section_name):
    """ load mongo connection settings from config object """
    conf = config[section_name]
    mongo = SectionKey(section_name)
    return MongoSettings(ssl=conf.getboolean(mongo.key('ssl')),
                         host=conf[mongo.key('host')].strip(),
                         port=conf[mongo.key('port')].strip(),
                         params=conf[mongo.key('params')].strip(),
                         dbname=conf[mongo.key('dbname')].strip(),
                         user=conf[mongo.key('user')].strip(),
                         passw=conf[mongo.key('pass')].strip())

def psql_settings_from_config(config, section_name):
    """ load psql connection settings from config object """
    conf = config[section_name]
    psql = SectionKey(section_name)
    return PsqlSettings(host=conf[psql.key('host')].strip(),
                        port=conf[psql.key('port')].strip(),
                        dbname=conf[psql.key('dbname')].strip(),
                        user=conf[psql.key('user')].strip(),
                        passw=conf[psql.key('pass')].strip(),
                        schema=conf[psql.key('schema-name')].strip())



def load_mongo_replicas_from_setting(config, mongo_section):
    """ Return
    {'some-name' : [ MongoSetting(), ..., MongoSetting() ],
     'some-name2' : [ MongoSetting() ]} """
    sections = config.sections()
    all_settings = {}
    conf_struct = get_config_structure(config)
    if mongo_section in sections:
        # single mongo instance with single replica
        settings = [mongo_settings_from_config(config, mongo_section)]
        all_settings[mongo_section] = settings
    else:
        settings = []
        if not mongo_section in conf_struct:
            Exception("Wrong config.")
        # single mongo instance with many replicas
        for setting_name in conf_struct[mongo_section]:
            if setting_name in sections:
                settings.append(mongo_settings_from_config(config,
                                                           setting_name))
        if len(settings):
            all_settings[mongo_section] = settings
        if not len(settings):
            # many shards with many replicas
            for setting_name in conf_struct[mongo_section]:
                dict_set = load_mongo_replicas_from_setting(config,
                                                            setting_name)
                if len(dict_set[setting_name]):
                    all_settings[setting_name] = dict_set[setting_name]
        if not len(all_settings):
            Exception('To complicated config file')
    return all_settings

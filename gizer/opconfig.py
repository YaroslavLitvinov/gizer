#!/usr/bin/env python

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
                           'schema', 'operational_schema'])

class SectionKey:
    def __init__(self, section_name):
        self.section_name = section_name
    def key(self, base_key_name):
        return "%s-%s" % (self.section_name, base_key_name)


def mongo_settings_from_config(config, section_name):
    mongo = SectionKey(section_name)
    conf = config[section_name]
    return MongoSettings(ssl=conf.getboolean(mongo.key('ssl')),
                         host=conf[mongo.key('host')].strip(),
                         port=conf[mongo.key('port')].strip(),
                         params=conf[mongo.key('params')].strip(),
                         dbname=conf[mongo.key('dbname')].strip(),
                         user=conf[mongo.key('user')].strip(),
                         passw=conf[mongo.key('pass')].strip())

def psql_settings_from_config(config, section_name):
    psql = SectionKey(section_name)
    conf = config[section_name]
    return PsqlSettings(host=conf[psql.key('host')].strip(),
                        port=conf[psql.key('port')].strip(),
                        dbname=conf[psql.key('dbname')].strip(),
                        user=conf[psql.key('user')].strip(),
                        passw=conf[psql.key('pass')].strip(),
                        schema=conf[psql.key('schema-name')].strip(),
                        operational_schema\
                        =conf[psql.key('operational-schema-name')].strip())



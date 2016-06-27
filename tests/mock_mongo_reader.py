#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import bson
from bson.json_util import loads

class MongoReaderMock:
    """ Similiar interface to MongoReader. For test purposes. """
    def __init__(self, raw_bson_data, query = None):
        self.array_data = loads(raw_bson_data)
        self.query = query
        self.rec_i = 0
        self.failed = False

    def connauthreq(self):
        return None

    def make_new_request(self, query):
        self.query = query

    def next(self):
        rec = None
        print "reader.next query=", self.query
        if self.query and len(self.query):
            for item in self.array_data:
                if ('id' in item and item['id'] == self.query['id']) or \
                   ('_id' in item and item['_id'] == self.query['_id']):
                    rec = item
                    break
        else:
            if self.rec_i < len(self.array_data):
                rec = self.array_data[self.rec_i]
                self.rec_i += 1
        return rec

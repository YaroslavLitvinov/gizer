#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import bson
from bson.json_util import loads
from logging import getLogger

class MongoReaderMock:
    """ Similiar interface to MongoReader. For test purposes. """
    def __init__(self, list_of_raw_bson_data, query=None):
        self.current_raw_bson_data_idx = None
        self.list_of_raw_bson_data = list_of_raw_bson_data
        self.query = query
        self.failed = False
        self.any_loaded = False
        self.load_next_test_dataset()

    def next_dataset_idx(self):
        if self.current_raw_bson_data_idx is None:
            return 0
        elif self.current_raw_bson_data_idx+1 < len(self.list_of_raw_bson_data):
            return self.current_raw_bson_data_idx + 1
        else:
            return None

    def load_next_test_dataset(self):
        if self.next_dataset_idx() is not None:
            self.current_raw_bson_data_idx = self.next_dataset_idx()
            self.rec_i = 0
            self.any_loaded = False
            getLogger(__name__).info("MockMongoReader load dataset idx=%d"
                                     % self.current_raw_bson_data_idx)
            data =  loads(self.list_of_raw_bson_data[self.current_raw_bson_data_idx])
            getLogger(__name__).info("MockMongoReader loaded dataset %d recs"  % len(data))
            self.array_data = data
            return data
        else:
            self.array_data = []
            getLogger(__name__).info('No more dataset available')
            return None

    def reset_dataset(self):
        self.current_raw_bson_data_idx = None
        self.load_next_test_dataset()

    def connauthreq(self):
        return None

    def make_new_request(self, query):
        self.query = query

    def next(self):
        rec = None
        getLogger(__name__).info("reader.next query=" + str(self.query))
        if self.query and len(self.query):
            for item_idx in xrange(len(self.array_data)):
                item = self.array_data[item_idx]
                if ('id' in item and item['id'] == self.query['id']) or \
                   ('_id' in item and item['_id'] == self.query['_id']):
                    rec = item
                    break
                elif 'ts' in self.query and '$gt' in self.query['ts'] and \
                        'ts' in item and item['ts'] > self.query['ts']['$gt']:
                    # emulate searching, hack query start iterating from next rec
                    getLogger(__name__).info("MockMongoReader match query")
                    getLogger(__name__).info("MockMongoReader req > %s rec=  %s" \
                                                 % (str(self.query['ts']['$gt']),
                                                    str(item['ts'])))
                    self.rec_i = item_idx
                    rec = self.array_data[self.rec_i]
                    self.query = None # reset query, just iterate results
                    self.rec_i += 1 # next item to iterate
                    break
        else:
            if self.rec_i < len(self.array_data):
                rec = self.array_data[self.rec_i]
                self.rec_i += 1
        getLogger(__name__).info("MockMongoReader next rec=%s" % str(rec))
        while not rec \
                and not self.any_loaded \
                and self.next_dataset_idx() is not None:
            self.load_next_test_dataset()
            rec = self.next()
        else:
            self.any_loaded = True
        return rec

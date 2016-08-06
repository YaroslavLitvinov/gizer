#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import bson
import pymongo
from bson.json_util import loads
from logging import getLogger
from collections import namedtuple

MockReaderDataset = namedtuple('MockReaderDataset', 
                               ['data',  # raw bson data
                                # inject_exception -- exception to raise
                                # when trying to read from dataset.
                                # None - no exceptions will be raised.
                                'inject_exception' ])

class MongoReaderMock:
    """ Similiar interface to MongoReader. For test purposes.
    params:
    -- datasets_list list of objects MockReaderDataset """
    def __init__(self, datasets_list, collection, query=None):
        self.current_dataset_idx = None
        self.datasets_list = datasets_list
        self.exception_to_inject = None
        self.query = query
        self.failed = False
        self.array_data = []
        self.collection = collection
        self.load_next_test_dataset()

    def real_transport(self):
        return False

    def next_dataset_idx(self):
        if self.current_dataset_idx is None:
            return 0
        elif self.current_dataset_idx+1 < len(self.datasets_list):
            return self.current_dataset_idx + 1
        else:
            return None

    def load_next_test_dataset(self):
        if self.next_dataset_idx() is not None:
            self.current_dataset_idx = self.next_dataset_idx()
            self.rec_i = 0
            getLogger(__name__).info("MockMongoReader load dataset idx=%d"
                                     % self.current_dataset_idx)
            dataset = self.datasets_list[self.current_dataset_idx]
            self.array_data = loads(dataset.data)
            self.exception_to_inject = dataset.inject_exception
            getLogger(__name__).info("MockMongoReader loaded dataset %d recs, \
inject_exception=%s"  % (len(self.array_data), str(self.exception_to_inject)))
            return self.array_data
        else:
            self.array_data = []
            self.exception_to_inject = None
            getLogger(__name__).info('No more dataset available')
            return None

    def reset_dataset(self):
        self.current_dataset_idx = None
        self.load_next_test_dataset()

    def connauthreq(self):
        return None

    def make_new_request(self, query):
        self.rec_i = 0
        self.query = query

    def count(self):
        return len(self.array_data)

    def next(self):
        if self.exception_to_inject:
            if self.exception_to_inject is pymongo.errors.OperationFailure or \
                    self.exception_to_inject is pymongo.errors.AutoReconnect:
                self.failed = True
                return None
            else:
                raise self.exception_to_inject
        rec = None
        getLogger(__name__).info("reader.next query=" + str(self.query))
        if self.query and len(self.query):
            for item_idx in xrange(len(self.array_data)):
                item = self.array_data[item_idx]
                if ('id' in item and 'id' in self.query \
                        and item['id'] == self.query['id']) or \
                   ('_id' in item  and '_id' in self.query \
                        and item['_id'] == self.query['_id']):
                    rec = item
                    break
                elif 'ts' in self.query and 'ts' in item and\
                        '$gt' in self.query['ts']:
                    if item['ts'] > self.query['ts']['$gt']:
                        # emulate search, hack query start iterating from next rec
                        getLogger(__name__).info("MockMongoReader match query")
                        getLogger(__name__).info("MockMongoReader req > %s \
rec= %s"
                                                 % (str(self.query['ts']['$gt']),
                                                    str(item['ts'])))
                        self.rec_i = item_idx
                        rec = self.array_data[self.rec_i]
                        self.query = None # reset query, just iterate results
                        self.rec_i += 1 # next item to iterate
                        break
                else:
                    # for this query just return all results
                    if self.rec_i < len(self.array_data):
                        rec = self.array_data[self.rec_i]
                    self.query = None # reset query, just iterate results
                    self.rec_i += 1 # next item to iterate
                    break
        else:
            self.query = None # reset query, just iterate results
            if self.rec_i < len(self.array_data):
                rec = self.array_data[self.rec_i]
                self.rec_i += 1
        # return rec and if no ore recs just switch to next dataset
        if rec:
            self.any_loaded_from_dataset = True
        elif self.next_dataset_idx() is not None:
            self.load_next_test_dataset()
        return rec

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
from exceptions import Exception

MockReaderDataset = namedtuple('MockReaderDataset', 
                               ['data',  # raw bson data
                                # inject_exception -- exception to raise
                                # when trying to read from dataset.
                                # None - no exceptions will be raised.
                                'inject_exception' ])

MOCK_EXCEPTION_KEY = 'mock_exception'
DATASET_KEY = 'dataset'
GAP_KEY = 'gap'

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
        self.collection = collection
        self.reset_dataset()

    def real_transport(self):
        return False

    def reset_dataset(self):
        self.array_data = []
        self.rec_i = 0
        for dataset in self.datasets_list:
            if dataset.inject_exception:
                self.array_data.append(
                    {MOCK_EXCEPTION_KEY: dataset.inject_exception})
            self.array_data.extend( loads(dataset.data) )
            self.array_data.append( None )

    def connauthreq(self):
        return None

    def make_new_request(self, query, projection=None):
        # projection is not supported by mock transport
        self.query = query
        self.reset_dataset()

    def count(self):
        if self.array_data:
            return self.array_data.index(None)
        else:
            return 0

    def next(self):
        item = len(self.array_data) # just  value
        rec = None
        while rec is None and item is not None:
            if self.array_data:
                item = self.array_data.pop(0)
            else:
                item = None
            if item is None:
                continue
            # if emulating search
            if item and type(item) is dict and MOCK_EXCEPTION_KEY in item:
                if item[MOCK_EXCEPTION_KEY] is pymongo.errors.OperationFailure or \
                        item[MOCK_EXCEPTION_KEY] is pymongo.errors.AutoReconnect:
                    self.failed = True
                    return None
                else:
                    raise item[MOCK_EXCEPTION_KEY]
            elif self.query and len(self.query):
                # if querying item having specific id
                if ('id' in item and 'id' in self.query ) or \
                   ('_id' in item  and '_id' in self.query):
                    if ('id' in item and item['id'] == self.query['id']) or \
                       ('_id' in item  and item['_id'] == self.query['_id']):
                        getLogger(__name__).info("MockMongoReader item by query=" 
                                                 + str(self.query))
                        rec = item
                # query ts > 
                elif 'ts' in self.query and 'ts' in item \
                        and '$gt' in self.query['ts']:
                    if item['ts'] > self.query['ts']['$gt']:
                        rec = item
                        getLogger(__name__).info("MockMongoReader match query")
                        getLogger(__name__).info("MockMongoReader req > %s rec= %s"
                                                 % (str(self.query['ts']['$gt']),
                                                    str(item['ts'])))
                # return all for unsupported query
                else:
                    rec = item
            # no query / empty query
            else:
                rec = item
            if rec:
                self.rec_i += 1
        return rec


def test_mock():
    data1 = '[{ "id": 10, "ts": 1 }, { "id": 11, "ts": 2 }, { "id": 12, "ts": 3 }]'
    data2 = '[{ "id": 20, "ts": 4 }, { "id": 21, "ts": 5 }]'
    test_datasets = []
    test_datasets.append(MockReaderDataset(data1, None))
    test_datasets.append(MockReaderDataset(data2, None))
    reader = MongoReaderMock(test_datasets, 'collection')
    # get all items
    reader.make_new_request({})
    assert(reader.next()['id'] == 10)
    assert(reader.next()['id'] == 11)
    assert(reader.next()['id'] == 12)
    assert(reader.next() == None)
    # get exact item
    reader.make_new_request({'id': 12})
    assert(reader.next()['id'] == 12)
    assert(reader.next() == None)
    # get itemswhose ts is greater than > 
    reader.make_new_request({"ts": {"$gt" : 1}})
    assert(reader.next()['id'] == 11)
    assert(reader.next()['id'] == 12)
    assert(reader.next() == None)
    # get all items as unsupported query
    reader.make_new_request({'$or': [ {'id': 11}, {'id': 12} ]})
    assert(reader.next()['id'] == 10)
    assert(reader.next()['id'] == 11)
    assert(reader.next()['id'] == 12)
    assert(reader.next() == None)
    # test fatal transpoort exception
    del reader
    reader = MongoReaderMock([MockReaderDataset(data1, 
                                                pymongo.errors.OperationFailure)], 
                             'collection')
    reader.make_new_request({})
    assert(reader.next() == None)
    assert(reader.failed == True)
    # test exception raise
    del reader
    reader = MongoReaderMock([MockReaderDataset(data1, 
                                                Exception('test'))], 
                             'collection')
    reader.make_new_request({})
    res= None
    try:
        res = reader.next()
        # must raise exception before assigning
        res = 1
    except:
        pass
    assert( res is None )


if __name__ == '__main__':
    test_mock()


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
MOCK_ITEM_KEY = 'mock_item'
MOCK_GAP_KEY = 'mock_gap'
DATASET_KEY = 'dataset'
GAP_KEY = 'gap'

class MongoReaderMock:
    """ Similiar interface to MongoReader. For test purposes.
    params:
    -- datasets_list list of objects MockReaderDataset """
    def __init__(self, datasets_list, collection, query=None):
        self.current_dataset_idx = None
        self.datasets_list = datasets_list
        self.reset_dataset()
        self.query = query
        self.failed = False
        self.collection = collection

    def real_transport(self):
        return False

    def reset_dataset(self):
        self.refill_data()

    def refill_data(self):
        self.array_data = []
        self.rec_i = 0
        for dataset_idx in xrange(len(self.datasets_list)):
            dataset = self.datasets_list[dataset_idx]
            if dataset.inject_exception:
                self.array_data.append(
                    {MOCK_EXCEPTION_KEY: dataset.inject_exception})
            else:
                items = [{MOCK_ITEM_KEY: i} for i in loads(dataset.data)]
                self.array_data.extend(items)
                self.array_data.append({MOCK_GAP_KEY: True})

    def connauthreq(self):
        return None

    def make_new_request(self, query, projection=None):
        # projection is not supported by mock transport
        self.query = query
        self.rec_i = 0

    def count(self):
        count = 0
        if self.array_data:
            for i in self.array_data:
                if MOCK_GAP_KEY in i and i[MOCK_GAP_KEY]:
                    break
                else:
                    count += 1
        return count

    def next(self):
        rec = None
        while rec is None and self.rec_i < len(self.array_data):
            item = self.array_data[self.rec_i]
            self.rec_i += 1
            # if emulating search
            if item and type(item) is dict and MOCK_EXCEPTION_KEY in item:
                getLogger(__name__).warning("Mocked exception: %s" % 
                                            item[MOCK_EXCEPTION_KEY])
                if item[MOCK_EXCEPTION_KEY] is pymongo.errors.OperationFailure or \
                        item[MOCK_EXCEPTION_KEY] is pymongo.errors.AutoReconnect or \
                        item[MOCK_EXCEPTION_KEY] is pymongo.errors.NetworkTimeout:
                    self.failed = True
                    return None
                else:
                    raise item[MOCK_EXCEPTION_KEY]
            elif item and type(item) is dict and MOCK_GAP_KEY in item:
                if item[MOCK_GAP_KEY]:
                    item[MOCK_GAP_KEY] = False
                    item = None
                else:
                    continue
            elif item and type(item) is dict and MOCK_ITEM_KEY in item:
                item = item[MOCK_ITEM_KEY]
            else:
                item = None
            
            if item and self.query and len(self.query):
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


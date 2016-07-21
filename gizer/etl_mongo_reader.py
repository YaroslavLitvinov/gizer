#!/usr/bin/env python

""" Execute mongo query and read data returned from mongo collection. 
Etl readed data in parallel"""

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from logging import getLogger
from gizer.opmultiprocessing import FastQueueProcessor

class EtlMongoReader:
    
    def __init__(self, pcount, qsize, async_worker_func, 
                 schema_engines, mongo_readers):
        self.pcount = pcount
        self.qsize = qsize
        self.schema_engines = schema_engines
        self.mongo_readers = mongo_readers
        self.current_mongo_reader = None
        self.all_recs_count = 0
        self.etl_recs_count = 0
        self.no_more_recs = False
        self.fast_queue = FastQueueProcessor(async_worker_func,
                                             schema_engines,
                                             pcount)

    def __del__(self):
        del self.fast_queue

    def execute_query(self, collection, query):
        self.all_recs_count = 0
        self.etl_recs_count = 0
        self.no_more_recs = False
        self.current_mongo_reader = self.mongo_readers[collection]
        self.current_mongo_reader.make_new_request(query)

    def next_processed(self):
        processed_list = None        
        if self.no_more_recs:
            return processed_list
        try:
            record = self.retrieve_mongo_record()
            if not self.fast_queue.error:
                processed_list = self.put_record_get_tables_async(
                    (record,
                     self.current_mongo_reader.collection))
    #            print "loop.b", len(processed_list), self.fast_queue.count()
                if not record:
                    self.no_more_recs = True
                else:
                    self.etl_recs_count += 1
        except:
            self.current_mongo_reader.failed = True
            raise
        if self.no_more_recs and not len(processed_list):
            return None
        else:
            return processed_list
    
    def retrieve_mongo_record(self):
        """ get next record from mongo collection """
        rec = self.current_mongo_reader.next()
        if not self.all_recs_count:
            self.all_recs_count = self.current_mongo_reader.count()
        if rec:
            if self.current_mongo_reader.rec_i % 1000 == 0:
                getLogger(__name__).info("%d of %d"
                                         % (self.current_mongo_reader.rec_i, 
                                            self.all_recs_count))
        return rec
    
    def put_record_get_tables_async(self, rec_and_collection):
        """ Put mongo record into pipeline to do parallel work in multiple
        processes. Get results asynchronously if available.
        Pipeline queue size can never exceed specified limit.
        rec -- mongo record to put into pipeline"""
        finish = False
        res = []
        if rec_and_collection[0]:
            self.fast_queue.put(rec_and_collection)
        get_all = self.fast_queue.count() \
                  or self.fast_queue.poll() or self.fast_queue.is_any_working()
        while self.fast_queue.count() >= self.qsize \
                or self.fast_queue.poll() \
                or (not rec_and_collection[0] and get_all and not finish):
            async_res = self.fast_queue.get()
            if async_res:
                res.append(async_res)
            else:
                finish = True
        return res

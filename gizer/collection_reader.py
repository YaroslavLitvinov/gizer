#!/usr/bin/env python

""" Mongo Collection records reader that is using parallel processing 
    for handling records that read. """

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from gizer.etl_mongo_reader import EtlMongoReader
from mongo_reader.prepare_mongo_request import prepare_mongo_request_for_list
from mongo_schema.schema_engine import create_tables_load_bson_data

# collection reader is just yet another reader using by syncronizer
COLLECTION_READER_WORKERS_COUNT = 8
COLLECTION_READER_QUEUE_SIZE = COLLECTION_READER_WORKERS_COUNT*2

def async_worker_handle_mongo_rec(schema_engines,
                                  rec_data_and_collection):
    """ function intended to call by FastQueueProcessor.
    process mongo record / bson data in separate process.
    schema_engines -- dict {'collection name': SchemaEngine}. Here is
    many schema engines to use every queue to handle items from any collection;
    rec_data_and_collection - tuple('collection name', bson record)"""
    rec = rec_data_and_collection[0]
    collection = rec_data_and_collection[1]
    return create_tables_load_bson_data(schema_engines[collection],
                                        [rec])

class CollectionReader:
    def __init__(self, collection_name, schema_engine, mongo_reader):
        self.collection_name = collection_name
        self.schema_engine = schema_engine
        self.mongo_reader = mongo_reader
        self.etl_mongo_reader = EtlMongoReader(COLLECTION_READER_WORKERS_COUNT,
                                               COLLECTION_READER_QUEUE_SIZE,
                                               async_worker_handle_mongo_rec,
                                               #1st worker param
                                               {collection_name: schema_engine}, 
                                               {collection_name: mongo_reader})

    def __del__(self):
        del self.etl_mongo_reader

    def get_mongo_table_objs_by_ids(self, rec_ids):
        res = {}
        # prepare query
        mongo_query = prepare_mongo_request_for_list(self.schema_engine, rec_ids)
        self.etl_mongo_reader.execute_query(self.collection_name, mongo_query)
        # get and process records
        processed_recs = self.etl_mongo_reader.next_processed()
        while processed_recs is not None:
            for mongo_tables_obj in processed_recs:
                rec_id = mongo_tables_obj.rec_id()
                res[str(rec_id)] = mongo_tables_obj
            processed_recs = self.etl_mongo_reader.next_processed()
        return res

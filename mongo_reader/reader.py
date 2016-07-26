#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import time
import pymongo
from logging import getLogger
#import urllib
from pymongo.mongo_client import MongoClient
from pymongo.cursor import CursorType


def mongo_reader_from_settings(settings, collection_name, request):
    """ Create transport to work with only one replica """
    return MongoReader([settings], collection_name, request)

def mongo_reader_from_settings_list(settings_list, collection_name, request):
    """ Create transport to work with many replicas """
    return MongoReader(settings_list, collection_name, request)

class MongoReader:

    def __init__(self, settings_list, collection, query):
        self.settings_list = settings_list
        self.collection = collection
        self.query = query
        self.rec_i = 0
        self.cursor = None
        self.client = None
        self.failed = False
        self.attempts = 0

    def reset_dataset(self):
        """ For compatibility with mock interface """
        pass

    def connauthreq(self):
        if len(self.settings_list) == 1:
            settings = self.settings_list[0]
            uri_fmt = "mongodb://{user}:{password}@{host}:{port}/{dbname}{params}"
            params = ""
            if len(settings.params):
                params = "?" + settings.params
            
            uri = uri_fmt.format(user=settings.user, 
                                 #password=urllib.quote_plus(self.settings.passw),
                                 password=settings.passw,
                                 host=settings.host, 
                                 port=settings.port,
                                 dbname=settings.dbname,
                                 params=params)
            self.client = MongoClient(uri)
        getLogger(__name__).info("Authenticated")

    def make_new_request(self, query):
        # there are only one dbname for all replicas
        dbname = self.settings_list[0].dbname
        if not self.client:
            self.connauthreq()
        mongo_collection = self.client[dbname][self.collection]
        cursor = mongo_collection.find(query)
        cursor.batch_size(1000)
        self.rec_i = 0
        self.cursor = cursor
        return cursor

    def count(self):
        return self.cursor.count()

    def next(self):
        if not self.cursor:
            self.cursor = self.make_new_request(self.query)

        self.attempts = 0
        rec = None
        while self.cursor.alive and not self.failed:
        #while not self.failed:
            try:
                rec = self.cursor.next()
                self.rec_i += 1
            except (pymongo.errors.AutoReconnect,
                    StopIteration):
                self.attempts += 1
                if self.attempts <= 4:
                    time.sleep(pow(2, self.attempts))
                    getLogger(__name__).warning("Connect attempt #%d, %s" %
                            (self.attempts, str(time.time())))
                    continue
                else:
                    self.failed = True
            except pymongo.errors.OperationFailure:
                self.failed = True
                getLogger(__name__).error("Exception: pymongo.errors.OperationFailure")
            break
        return rec

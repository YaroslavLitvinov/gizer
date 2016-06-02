#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
import time
import pymongo
import urllib
from pymongo.mongo_client import MongoClient
from pymongo.cursor import CursorType

def message(mes, cr='\n'):
    sys.stderr.write(mes + cr)

def mongo_reader_from_settings(settings, collection_name, request):
    is_oplog = False
    if collection_name == 'oplog.rs':
        is_oplog = True
    return MongoReader(settings, collection_name, request, is_oplog)

class MongoReader:

    def __init__(self, settings, collection, query, oplog = False):
        self.settings = settings
        self.collection = collection
        self.query = query
        self.rec_i = 0
        self.cursor = None
        self.client = None
        self.failed = False
        self.attempts = 0
        self.oplog = oplog

    def connauthreq(self):
        uri_fmt = "mongodb://{user}:{password}@{host}:{port}/{dbname}"
        params = ""
        if len(self.settings.params):
            params = "?" + self.settings.params
        
        uri = uri_fmt.format(user=self.settings.user, 
                             password=urllib.quote_plus(self.settings.passw),
                             host=self.settings.host, 
                             port=self.settings.port,
                             dbname=self.settings.dbname,
                             params=params)
        self.client = MongoClient(uri)
        message("Authenticated")
        return self.make_new_request(self.query)

    def make_new_request(self, query):
        if self.oplog:
            cursor_type = CursorType.TAILABLE
            oplog_replay = True
        else:
            cursor_type = CursorType.NON_TAILABLE
            oplog_replay = False

        mongo_collection = self.client[self.settings.dbname][self.collection]
        self.cursor = mongo_collection.find(query,
                                            cursor_type = cursor_type,
                                            oplog_replay = oplog_replay)
        self.cursor.batch_size(1000)
        return self.cursor

    def next(self):
        if not self.cursor:
            self.connauthreq()

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
                    message("Connect attempt #%d, %s" %
                            (self.attempts, str(time.time())))
                    continue
                else:
                    self.failed = True
            except pymongo.errors.OperationFailure:
                self.failed = True
                message("Exception: pymongo.errors.OperationFailure")
            break
        return rec

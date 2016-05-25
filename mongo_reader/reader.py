#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
from pymongo.mongo_client import MongoClient
from pymongo.cursor import CursorType

def message(mes, cr='\n'):
    sys.stderr.write(mes + cr)

def mongo_reader_from_settings(settings, collection_name, request):
    return MongoReader(settings.ssl,
                       settings.host,
                       settings.port,
                       settings.dbname,
                       collection_name,
                       settings.user,
                       settings.passw,
                       request)

class MongoReader:

    def __init__(self, ssl, host, port, dbname, collection,
                 user, passw, query, oplog = False):
        self.ssl = ssl
        self.host = host
        self.port = int(port)
        self.dbname = dbname
        self.collection = collection
        self.user = user
        self.passw = passw
        self.query = query
        self.rec_i = 0
        self.cursor = None
        self.client = None
        self.failed = False
        self.attempts = 0
        self.oplog = oplog

    def connauthreq(self):
        self.client = MongoClient(self.host, self.port, ssl=self.ssl)
        if self.user and self.passw:
            self.client[self.dbname].authenticate(self.user, self.passw)
            message("Authenticated")
        return self.make_new_request(self.query)

    def make_new_request(self, query):
        if self.oplog:
            cursor_type = CursorType.TAILABLE
            oplog_replay = True
        else:
            cursor_type = CursorType.NON_TAILABLE
            oplog_replay = False

        mongo_collection = self.client[self.dbname][self.collection]
        self.cursor = mongo_collection.find(self.query,
                                            cursor_type = cursor_type,
                                            oplog_replay = oplog_replay)
        self.cursor.batch_size(1000)
        return self.cursor

    def next(self):
        if not self.cursor:
            self.connauthreq()

        self.attempts = 0
        rec = None
        while self.cursor.alive and self.failed is False:
            try:
                rec = self.cursor.next()
                self.rec_i += 1
            except pymongo.errors.AutoReconnect:
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

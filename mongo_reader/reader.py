#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import sys
from pymongo.mongo_client import MongoClient

def message(mes, cr='\n'):
    sys.stderr.write(mes + cr)

class MongoReader:

    def __init__(self, ssl, host, port, dbname, collection,
                 user, passw, request):
        self.ssl = ssl
        self.host = host
        self.port = int(port)
        self.dbname = dbname
        self.collection = collection
        self.user = user
        self.passw = passw
        self.request = request
        self.rec_i = 0
        self.cursor = None
        self.client = None
        self.failed = False
        self.attempts = 0

    def connauthreq(self):
        self.client = MongoClient(self.host, self.port, ssl=self.ssl)
        if self.user and self.passw:
            self.client[self.dbname].authenticate(self.user, self.passw)
            message("Authenticated")
        mongo_collection = self.client[self.dbname][self.collection]
        self.cursor = mongo_collection.find(self.request)
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


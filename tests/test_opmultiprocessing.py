#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from gizer.opmultiprocessing import FastQueueProcessor

def worker(arg1, arg2):
    return arg1 + arg2

def worker_with_error(foo, foo2):
    raise Exception('test')

def test_fatsqueue():
    fast_queue = FastQueueProcessor(worker, 100, 7)
    for x in xrange(9):
        fast_queue.put(x)
    results = [fast_queue.get() for i in xrange(10)]
    print "get ok"
    results.sort()
    print results
    assert([None, 100, 101, 102, 103, 104, 105, 106, 107, 108] == results)
    print "ok"

def test_fatsqueue_error():
    fast_queue = FastQueueProcessor(worker_with_error, None, 1)
    fast_queue.put(1)
    fast_queue.get()
    assert(fast_queue.error == True)

def test_fatsqueue_eof():
    fast_queue = FastQueueProcessor(worker, 100, 1)
    fast_queue.put(1)
    del fast_queue

if __name__ == '__main__':
    test_fatsqueue()

#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from gizer.opmultiprocessing import FastQueueProcessor

def worker(arg1, arg2):
    return arg1 + arg2

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

if __name__ == '__main__':
    test_fatsqueue()

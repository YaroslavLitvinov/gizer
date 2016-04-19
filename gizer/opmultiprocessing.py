#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from multiprocessing import Process
from multiprocessing import Pipe
from collections import namedtuple
from collections import deque

_EOF = 'EOF'

def _worker(pipes):
    pipe_work, pipe_main = pipes
    worker_func = None
    worker_1st_arg = None
    while True:
        try:
            if not worker_func:
                worker_func = pipe_work.recv()
                worker_1st_arg = pipe_work.recv()
            else:
                arg = pipe_work.recv()    # Read from the output pipe and do nothing
                # close pipe if 'EOF' received
                if arg == _EOF:
                    pipe_work.close()
                    pipe_main.close()
                    break
                else:
                    res = worker_func(worker_1st_arg, arg)
                    pipe_work.send(res)
        except EOFError:
            break


class FastQueueProcessor:

    FastProc = namedtuple('FastProc', ['pipe_work', 'pipe_main', 'proc'])

    def __init__(self, worker, worker_1st_arg, procn):
        self.queue_data = deque()
        self.procs = self._create_procs(worker, worker_1st_arg, procn)
        self.proc_statuses = [False for i in xrange(procn)]

    def _create_procs(self, worker, worker_1st_arg, procn):
        procs = []
        for i in xrange(procn):
            pipe_work, pipe_main = Pipe()
            proc = Process(target = _worker, args = ((pipe_work, pipe_main), ))
            proc.start()
            pipe_main.send(worker)
            pipe_main.send(worker_1st_arg)
            procs.append(FastQueueProcessor.FastProc(pipe_work = pipe_work, 
                                                     pipe_main = pipe_main, 
                                                     proc = proc))
        return procs
       
    def _consume_from_queue(self):
        for i in xrange(len(self.proc_statuses)):
            if not self.count():
                break
            status = self.proc_statuses[i]
            if not status:
                data = self.queue_data.popleft()
                self.procs[i].pipe_main.send(data)
                self.proc_statuses[i] = True

    def is_any_working(self):
        for status in self.proc_statuses:
            if status:
                return True
        return False

    def __del__(self):
        for proc in self.procs:
            proc.pipe_main.send(_EOF)
            proc.pipe_work.close()
            proc.pipe_main.close()
            proc.proc.join()

    def count(self):
        return len(self.queue_data)

    def put(self, data):
        self.queue_data.append(data)
        self._consume_from_queue()

    def poll(self):
        for proc in self.procs:
            if proc.pipe_main.poll():
                return True
        return False


    def get(self):
        """ @return result of calculation on data"""
        res = None
        while True:
            if not len(self.queue_data) and not self.is_any_working():
                break
            for i in xrange(len(self.procs)):
                proc = self.procs[i]
                if proc.pipe_main.poll():
                    res = proc.pipe_main.recv()
                    self.proc_statuses[i] = False
                    break
            if res is not None:
                break
            self._consume_from_queue()
        return res

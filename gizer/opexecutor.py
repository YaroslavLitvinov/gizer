#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import collections
from subprocess import Popen

class Executor:
    def __init__(self):
        self.subprocesses = []

    def handle_exit(self, proc):
        retcode = proc.popen.poll()
        if retcode is None:
            return None
        if retcode != 0:
            raise Exception('Opexecutor', 'Exit code=%d,  %s' % (retcode, str(proc.cmd)))
        return retcode
       
    def free_completed(self):
        for i in reversed(range(len(self.subprocesses))):
            res = self.handle_exit(self.subprocesses[i])
            if res is not None:
                del(self.subprocesses[i])

    def wait_for_complete(self):
        for p in self.subprocesses:
            retcode = p.popen.wait()
            self.handle_exit(p)

    def execute(self, cmd):
        tclass = collections.namedtuple('Proc', ['popen', 'cmd'])
        self.free_completed()
        self.subprocesses.append(tclass(popen=Popen(cmd), cmd=cmd))

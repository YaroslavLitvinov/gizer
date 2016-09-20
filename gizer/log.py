#!/usr/bin/env python

""" """

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import logging

LOG_LEVEL = logging.INFO

def save_loglevel():
    LOG_LEVEL = logging.getLogger().getEffectiveLevel()

def logless(loglevel=logging.ERROR):
    logging.getLogger().setLevel(loglevel)

def logmore():
    logging.getLogger().setLevel(LOG_LEVEL)

#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import csv
import sys
from subprocess import call

NULLVAL = '\N'
ESCAPECHAR = '\\'
DELIMITER = '\t'
LINETERMINATOR = '\r\n'
DOUBLEQUOTE = False
QUOTING = csv.QUOTE_NONE

def ensure_dir_empty(dirpath):
    if not os.path.exists(dirpath):
        os.mkdir(dirpath)
    for fname in os.listdir(dirpath):
        fpath = os.path.join(dirpath, fname)
        if os.path.isfile(fpath):
            os.remove(fpath)

def worker_csv_writer(csvwriter, data):
    wrt = self.writers[name]
    written_reccount = wrt.writer.write_csv(sqltable)
    if wrt.writer.file.tell() >= self.chunk_size:
        wrt.writer.close()
    self.writers[name] = wrt
    return written_reccount

class CsvInfo:
    def __init__(self, writer, filepath, name, filec):
        self.writer = writer
        self.filepath = filepath 
        self.name = name
        self.file_counter = filec

class CsvManager:
    def __init__(self, names, csvs_path, chunk_size):
        self.writers = {}
        self.csvs_path = csvs_path
        self.chunk_size = chunk_size
        self.cleandirs(names)

    def cleandirs(self, names):
        for name in names:
            dirpath = os.path.join(self.csvs_path, name)
            ensure_dir_empty(dirpath)

    def create_writer(self, name, fnumber):
        dirpath = os.path.join(self.csvs_path, name)
        filepath = os.path.join(dirpath, str(fnumber).zfill(5))
        f = open(filepath, 'wb')
        wrt = CsvInfo(CsvWriter(f, True),  filepath, name, fnumber)
        return wrt

    def write_csv(self, name, rows):
        """
        Write table records to csv file
        @param sqltable data to write
        @return records count written
        """
        if name not in self.writers.keys():
            self.writers[name] = self.create_writer(name, 0)
        elif not self.writers[name].writer.file:
            newfile_count = self.writers[name].file_counter + 1
            self.writers[name] = self.create_writer(name, newfile_count)

        wrt = self.writers[name]
        written_reccount = wrt.writer.write_csv(rows)
        if wrt.writer.file.tell() >= self.chunk_size:
            wrt.writer.close()
        self.writers[name] = wrt
        return written_reccount
    
    def finalize(self):
        for name, wrt in self.writers.iteritems():
            wrt.writer.close()

class CsvWriter:
    def __init__(self, output_file, psql_copy, null_val = NULLVAL):
        self.null_val = null_val
        self.psql_copy = psql_copy
        self.file = output_file
        self.csvwriter = csv.writer(output_file, 
                                    escapechar = ESCAPECHAR,
                                    delimiter = DELIMITER,
                                    lineterminator = LINETERMINATOR,
                                    doublequote = DOUBLEQUOTE,
                                    quoting = QUOTING)

    def close(self):
        self.file.close()
        self.file = None

    def write_csv(self, rows):
        """ 
        @return records count was written
        """
        for row in rows:
            self.csvwriter.writerow(row)
        return len(rows)


################

class CsvReader:
    def __init__(self, input_file, null_val = NULLVAL):
        self.null_val = null_val
        self.file = input_file
        self.csvreader = csv.reader(input_file, 
                                    escapechar = ESCAPECHAR,
                                    delimiter = DELIMITER,
                                    lineterminator = LINETERMINATOR,
                                    doublequote = DOUBLEQUOTE,
                                    quoting = QUOTING)

    def close(self):
        self.file.close()
        self.file = None

    def read_record(self):
        """ 
        @param table object schema_engine.SqlTable
        @return records read records
        """
        def decode_val(val):
            if type(val) is str or type(val) is unicode:
                if val == self.null_val:
                    return None
                else:
                    return val.decode('utf-8').decode('unicode-escape')
            else:
                return val

        def prepare_csv_data(row):
            csvvals = []
            for val in row:
                csvvals.append(decode_val(val))
            return csvvals

        try:
            return prepare_csv_data(self.csvreader.next())
        except StopIteration:
            return None

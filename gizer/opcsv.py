#!/usr/bin/env python

import os
import collections
import csv

class CsvInfo:
    def __init__(self, writer, filepath, name, datac, filec):
        self.writer = writer
        self.filepath = filepath 
        self.name = name
        self.data_counter = datac
        self.file_counter = filec

class CsvManager:
    def __init__(self, tmp_path, hdfs_path, chunk_size):
        self.writers = {}
        self.tmp_path = tmp_path
        self.hdfs_path = hdfs_path
        self.chunk_size = chunk_size

    def put_to_hdfs(self, wrt):
        hdfsdir = os.path.join(self.hdfs_path, wrt.name)
        print "hdfs dfs -mkdir -p ", hdfsdir
        print "hdfs dfs -put", wrt.filepath, \
            os.path.join(hdfsdir, str(wrt.file_counter).zfill(4))
        print "rm -f", wrt.filepath

    def create_writer(self, name, number):
        filepath = os.path.join(self.tmp_path, name+'.'+str(number).zfill(4))
        f = open(filepath, 'wb')
        wrt = CsvInfo(CsvWriter(f, '\\N'),  filepath, name, 0, number)
        return wrt

    def write_csv(self, sqltable):
        name = sqltable.table_name
        filec = None
        if name not in self.writers.keys():
            filec = 0
        elif self.writers[name].data_counter >= self.chunk_size:
            filec = self.writers[name].file_counter + 1
        if filec is not None:
            self.writers[name] = self.create_writer(name, filec)
        wrt = self.writers[name]
        wrt.data_counter += wrt.writer.write_csv(sqltable)
        if wrt.data_counter >= self.chunk_size:
            wrt.writer.close()
            self.put_to_hdfs(wrt)
    
    def finalize(self):
        for name, wrt in self.writers.iteritems():
            wrt.writer.close()
            self.put_to_hdfs(wrt)

class CsvWriter:
    def __init__(self, output_file, null_val_as):
        self.null_val = null_val_as
        self.file = output_file
        self.csvwriter = csv.writer(output_file, 
                                    quotechar='"',
                                    escapechar='\\',
                                    delimiter='\t',
                                    lineterminator='\n',
                                    doublequote=False,
                                    quoting=csv.QUOTE_ALL)

    def close(self):
        print "close file"
        self.file.close()

    def write_csv(self, sqltable):
        """ @param table object schema_engine.SqlTable
        """
        def escape_val(val):
            if type(val) is str or type(val) is unicode:
                return val.replace('\n', '\\n').replace('\t', '\\t').encode('utf-8')
            else:
                return val

        def prepare_csv_data(current_idx, colnames, columns):
            csvvals = []
            for i in colnames:
                val = columns[i].values[current_idx]
                if val is not None:
                    csvvals.append(escape_val(val))
                else:
                    csvvals.append(self.null_val)
            return csvvals

        counter = 0
        firstcolname = sqltable.sql_column_names[0]
        reccount = len(sqltable.sql_columns[firstcolname].values)
        for val_i in xrange(reccount):
            csvdata = prepare_csv_data(val_i, 
                                       sqltable.sql_column_names, 
                                       sqltable.sql_columns)
            self.csvwriter.writerow(csvdata)
            counter += len(csvdata)
        return counter

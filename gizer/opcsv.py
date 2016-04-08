import os
import csv
import sys
from subprocess import call
from gizer.opexecutor import Executor

class CsvInfo:
    def __init__(self, writer, filepath, name, filec):
        self.writer = writer
        self.filepath = filepath 
        self.name = name
        self.file_counter = filec

class CsvManager:
    def __init__(self, tmp_path, hdfs_path, chunk_size):
        self.writers = {}
        self.tmp_path = tmp_path
        self.hdfs_path = hdfs_path
        self.chunk_size = chunk_size
        self.hdfs_dirs = {}
        self.executor = Executor()

    def put_to_hdfs(self, wrt):
#ensure hdfs dirs exists
        hdfsdir = os.path.join(self.hdfs_path, wrt.name)
        if hdfsdir not in self.hdfs_dirs:
            rm_cmd = ['hdfs', 'dfs', '-rm', '-R', '-f', hdfsdir]
            call(rm_cmd)
            mkdir_cmd = ['hdfs', 'dfs', '-mkdir', '-p', hdfsdir]
            if call(mkdir_cmd) is 0:
                self.hdfs_dirs[hdfsdir] = True

        cmd = ['hdfs', 'dfs', '-moveFromLocal', wrt.filepath, \
                   os.path.join(hdfsdir, str(wrt.file_counter).zfill(5))]
        self.executor.execute(cmd)

    def create_writer(self, name, fnumber):
        filepath = os.path.join(self.tmp_path, name+'.'+str(fnumber).zfill(5))
        f = open(filepath, 'wb')
        wrt = CsvInfo(CsvWriter(f, '\\\\N'),  filepath, name, fnumber)
        return wrt

    def write_csv(self, sqltable):
        """
        Write table records to csv file and place asynchronously to hdfs
        @param sqltable data to write
        @return records count written
        """
        name = sqltable.table_name
        if name not in self.writers.keys():
            self.writers[name] = self.create_writer(name, 0)
        elif not self.writers[name].writer.file:
            newfile_count = self.writers[name].file_counter + 1
            self.writers[name] = self.create_writer(name, newfile_count)

        wrt = self.writers[name]
        written_reccount = wrt.writer.write_csv(sqltable)
        if wrt.writer.file.tell() >= self.chunk_size:
            wrt.writer.close()
            self.put_to_hdfs(wrt)
        self.writers[name] = wrt
        return written_reccount
    
    def finalize(self):
        for name, wrt in self.writers.iteritems():
            wrt.writer.close()
            self.put_to_hdfs(wrt)
        self.executor.wait_for_complete()

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
        self.file.close()
        self.file = None

    def write_csv(self, sqltable):
        """ 
        @param table object schema_engine.SqlTable
        @return records count was written
        """
        def escape_val(val):
            if type(val) is str or type(val) is unicode:
                return val.replace('\n', ' ').replace('\r', ' ').encode('utf-8')
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

        firstcolname = sqltable.sql_column_names[0]
        reccount = len(sqltable.sql_columns[firstcolname].values)
        for val_i in xrange(reccount):
            csvdata = prepare_csv_data(val_i, 
                                       sqltable.sql_column_names, 
                                       sqltable.sql_columns)
            self.csvwriter.writerow(csvdata)
        return reccount

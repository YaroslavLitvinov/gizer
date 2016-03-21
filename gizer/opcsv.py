#!/usr/bin/env python

import csv

class CsvWriter:
    def __init__(self, output_file, null_val_as):
        self.null_val = null_val_as
        self.csvwriter = csv.writer(output_file, 
                                    delimiter='\t',
                                    lineterminator='\n',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)

    def write_csv(self, table):
        """ @param table object schema_engine.SqlTable
        """
        def prepare_csv_data(current_idx, colnames, columns):
            csvvals = []
            for i in colnames:
                val = columns[i].values[current_idx]
                if val is not None:
                    csvvals.append(val)
                else:
                    csvvals.append(self.null_val)
            return csvvals

        firstcolname = table.sql_column_names[0]
        reccount = len(table.sql_columns[firstcolname].values)
        for val_i in xrange(reccount):
            csvdata = prepare_csv_data(val_i, table.sql_column_names, table.sql_columns)
            self.csvwriter.writerow(csvdata)

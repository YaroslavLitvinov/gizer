#!/usr/bin/env python

import csv

class CsvWriter:
    def __init__(self, output_file, null_val_as):
        self.null_val = null_val_as
        self.csvwriter = csv.writer(output_file, 
                                    quotechar='"',
                                    escapechar='\\',
                                    delimiter='\t',
                                    lineterminator='\n',
                                    doublequote=False,
                                    quoting=csv.QUOTE_ALL)

    def write_csv(self, table):
        """ @param table object schema_engine.SqlTable
        """
        def escape_val(val):
            if type(val) is str or type(val) is unicode:
                return val.replace('\n', '\\n').replace('\t', '\\t')
            else:
                return val

        def prepare_csv_data(current_idx, colnames, columns):
            csvvals = []
            for i in colnames:
                val = columns[i].values[current_idx]
                if val is not None:
                    csvvals.append(escape_val(val))
                else:
                    csvvals.append(escape_val(self.null_val))
            return csvvals

        firstcolname = table.sql_column_names[0]
        reccount = len(table.sql_columns[firstcolname].values)
        for val_i in xrange(reccount):
            csvdata = prepare_csv_data(val_i, table.sql_column_names, table.sql_columns)
            self.csvwriter.writerow(csvdata)

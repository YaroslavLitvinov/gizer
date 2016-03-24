import psycopg2

class PsqlRequests:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor()

    def get_max_index(self, tablename, indexcolumn):
        self.cursor.execute("SELECT max(%s) FROM %s;" % (indexcolumn, tablename))
        idx = self.cursor.fetchone()[0]
        if idx is None:
            idx = 0
        return idx

    def get_table_max_indexes(self, table):
        indexes = {}
        for column_name, column in table.sql_columns.iteritems():
            index_key = column.index_key()
            if index_key:
                indexes[index_key] = self.get_max_index(\
                        table.table_name, column_name)
        return indexes



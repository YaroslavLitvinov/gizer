from gizer.oplog_parser import OplogParser
from gizer.opinsert import generate_insert_queries
from mongo_to_hive_mapping.schema_engine import create_tables_load_bson_data


def test_oplog_parser():

    def test_cb_insert(ns, schema_engine, objdata):
        tables = create_tables_load_bson_data(schema_engine, objdata)
        posts_table = tables.tables['posts']
        assert(posts_table)
        assert(posts_table.sql_column_names == [u'body', u'created_at', u'id_bsontype', u'id_oid', u'title', u'updated_at', u'user_id', 'idx'])
        return generate_insert_queries(posts_table, "")

    def test_cb_update(ns, schema, objdata, parent_id):
        return "update"

    def test_cb_delete(ns, schema, objdata):
        return "delete"

    p = OplogParser("./test_data/schemas/rails4_mongoid_development",
                    test_cb_insert, test_cb_update, test_cb_delete)
    p.load_file('test_data/test_oplog.js')
    while p.next() != None:
        pass

    assert(p.insert_count==1)
    assert(p.delete_count==1)
    assert(p.update_count==8)

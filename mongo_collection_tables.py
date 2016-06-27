#!/usr/bin/env python

""" Get list of psql tables corresponding to specified collection. """

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import argparse
import json
from mongo_schema import schema_engine

def main():
    """ main """
    parser = argparse.ArgumentParser()
    parser.add_argument("-cn", "--collection-name",
                        help="Mongo collection name",
                        type=str, required=True)
    parser.add_argument("-ifs", "--input-file-schema", action="store",
                        help="Input file with json schema",
                        type=file, required=True)
    parser.add_argument("-delim",
                        help="Use delimiter between table name components",
                        type=str, required=True)

    args = parser.parse_args()

    collection_name = args.collection_name

    js_schema = [json.load(args.input_file_schema)]
    schema = schema_engine.SchemaEngine(collection_name, js_schema)
    for i in schema.get_tables_list(args.delim):
        print i

if __name__ == "__main__":
    main()

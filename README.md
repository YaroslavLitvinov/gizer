[![Coverage Status](https://coveralls.io/repos/github/YaroslavLitvinov/gizer/badge.svg?branch=master)](https://coveralls.io/github/YaroslavLitvinov/gizer?branch=master)

1. Environment
1.1. Add directory containing mongo_schema module to python path. 
     If module resided in git directory: export PYTHONPATH=~/git/:.
1.2. Specify TEST_PSQLCONN env variable before running tests, for example:
     export TEST_PSQLCONN="dbname=zvm user=zvm"
1.3. Test it
     ./run_cov_tests.sh or py.test

2. Config file. Connection settings, etc. See config-sample.ini for example.

3. Tools.
3.1. mongo_reader.py - 1st part of initial load.
     It's creates relational model of data for specific collection and then saves all collection's data into csv files.
3.2. psql_copy.py - 2nd part of initial load.
     Export scv files previously created by mongo_reader.py into postgres tables.
3.3. etlstatus.py - application managing etl status by external application;
3.4. config_value.py - external config reader, one value per execution;
3.5. mongo_to_psql.py - application for syncing, handling oplog.

4. Examples
python mongo_schema/get_mongo_schema_as_json.py --host localhost:27017  -cn test.get_sql_query_tests > get_sql_query_tests.js
python mongo_reader.py --config-file ../gizer-config.ini -cn submit_feedbacks --ddl-statements-file submit_feedbacks.sql --csv-path tmp -psql-table-prefix 2016_04_11_ -stats-file submit_feedbacks.stat
python psql_copy.py --config-file ../gizer-config.ini -cn submit_feedbacks --psql-table-name submit_feedbacks --input-csv-dir tmp/submit_feedbacks/ -psql-table-prefix 2016_04_11_

5. Know issues.
5.1. Schema items' types should be strictly defined. Incorrectly defined types may lead to errors.
5.2. Fields which are not in schema or whose have different types will not be loaded to relational model

6. Test Coverage: 96%

[![Coverage Status](https://coveralls.io/repos/github/YaroslavLitvinov/gizer/badge.svg?branch=master)](https://coveralls.io/github/YaroslavLitvinov/gizer?branch=master)

## Intro<br>
Application requires two connections to PostgreSQL instances: one is
using for caching purposes and another is real target instance.  At
least PostgreSQL 9.3 required. This requirement is coming from initial
database synchronization (Init load). 

Solution is divided into 3 phases:<br>
* Schema acquisition.<br>
  See get_mongo_schema_as_json.py tool from
  https://github.com/YaroslavLitvinov/mongo_schema. That tool
  creates schema of a data to be used during init & oplog load
  stages.
* Init load.<br>
  It's performing by bulk psql copy. During this phase data
  is reading directly from mongo connector, so no oplog connector is
  using on this stage. It's can be useful to run only this stage if
  you just need to update your postgres data once a day.<br>
  mongo_reader.py and psql_copy.py are the right tools for this stage.
  mongo_reader.py reads data from mongodb into csv files.<br>
  psql_copy.py exports colections of csv files into Postgres.
* Oplog load.<br>
  Sharded cluster supported. Every mongodb replica should be specified
  in config file as separate section. As solution is kind of batch
  it's must be invoked every time in order to process freshly added
  mongodb's changes. Simple scheduler like crontab works for this and
  helps to deliver fresh pattches to PostgreSQL. So frequently as you
  need, for example every minute of five.<br>
  Tool mongo_to_psql.py serves for this stage.<br>

## Environment<br>
* Python 2.7.x
* Add directory containing mongo_schema module to python path.  If
     module resided in git directory: export PYTHONPATH=~/git/:.
* Specify TEST_PSQLCONN env variable before running tests, for
     example: export TEST_PSQLCONN="dbname=zvm user=zvm"
* Test it
     ./run_cov_tests.sh or py.test

## Config file.<br>
  Connection settings, etc. See sample-config.ini for inspiration.

## Tools.
* mongo_reader.py - 1st part of initial load.
     It's creates relational model of data for specific collection and then saves all collection's data into csv files.
* psql_copy.py - 2nd part of initial load.
     Export scv files previously created by mongo_reader.py into postgres tables.
* etlstatus.py - application managing etl status by external application;
* config_value.py - external config reader, one value per execution;
* mongo_to_psql.py - application for syncing, handling oplog.

## Examples from cmd line to perform init load<br>
```
python mongo_schema/get_mongo_schema_as_json.py --host localhost:27017  -cn test.get_sql_query_tests > get_sql_query_tests.js
python mongo_reader.py --config-file ../gizer-config.ini -cn submit_feedbacks --ddl-statements-file submit_feedbacks.sql --csv-path tmp -psql-table-prefix 2016_04_11_ -stats-file submit_feedbacks.stat
python psql_copy.py --config-file ../gizer-config.ini -cn submit_feedbacks --psql-table-name submit_feedbacks --input-csv-dir tmp/submit_feedbacks/ -psql-table-prefix 2016_04_11_
```

## Know issues.<br>
* Schema items' types should be strictly defined. Incorrectly defined types may lead to errors.
* Fields which are not in schema or whose have different types will not be loaded to relational model

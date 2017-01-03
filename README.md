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
  in config file as separate section. In this mode oplog reader data is reading from syncronization point and transforming oplog data to appropriate sqls. Additionally, just before commiting changes into Postgres it makes reverse load of postgres data and comparing it to actual mongodb record's data. It creates additional overhead, but ensures us from having non consistent data in Postgres database. In case if inconsistency detected, bad data will be fixed by reloading specific record or in worst case it can lead to complete reload of whole database. So, in short this is a startegy that keeps data consistent. As solution is kind of batch `mongo_to_psql` must be invoked every time in order to process freshly added
  mongodb's changes. Simple crontab scheduler can be used for this. So every app invoke will deliver fresh pattches to PostgreSQL. For example, you can run this 'synchronization' tool every minute or five.<br>
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

## Command line examples<br>
Acquire schema<br>
```python mongo_schema/get_mongo_schema_as_json.py --host localhost:27017  -cn test.get_sql_query_tests > get_sql_query_tests.js```

Save latest oplog timestamp before running init load<br>
```python etlstatus.py -init-load-start-save-ts  --config-file ../gizer-config.ini```

Run init load part 1 of 2
```python mongo_reader.py --config-file ../gizer-config.ini -cn submit_feedbacks --ddl-statements-file submit_feedbacks.sql --csv-path tmp -psql-table-prefix 2016_04_11_ -stats-file submit_feedbacks.stat```

Run init load part 2 of 2
```python psql_copy.py --config-file ../gizer-config.ini -cn submit_feedbacks --psql-table-name submit_feedbacks --input-csv-dir tmp/submit_feedbacks/ -psql-table-prefix 2016_04_11_```

When init load finishes save completion status ok/error<br>
```python etlstatus.py -init-load-finish ok  --config-file ../gizer-config.ini```

Verify etl status, if exit code is 1 then run init load<br>
```python etlstatus.py -init-load-status  --config-file ../gizer-config.ini```

Run following command every time to update postgres database by MongoDB data<br>
```python mongo_to_psql.py --config-file ../gizer-config.ini```

## Know issues.<br>
* Schema items' types should be strictly defined. Incorrectly defined types may lead to errors.
* Fields which are not in schema or whose have different types will not be loaded to relational model

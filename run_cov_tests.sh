export PYTHONPATH=$PYTHONPATH:$PWD
py.test --cov-report=term-missing --cov=gizer tests/
py.test --cov-report=term-missing --cov=mongo_to_hive_mapping mongo_to_hive_mapping

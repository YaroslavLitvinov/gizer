export PYTHONPATH=$PYTHONPATH:$PWD
py.test --cov-report=term-missing --cov=gizer tests/
py.test --cov-report=term-missing --cov=mongo_schema ../mongo_schema

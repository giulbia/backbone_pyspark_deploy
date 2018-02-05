#!/usr/bin/env bash

source /analytics/anaconda3/bin/activate /analytics/anaconda/envs/deployment

kinit -kt ...

export SPARK_MAJOR_VERSION=2
export SPARK_HOME=/usr/hdp/current/spark2-client/
export PYSPARK_DRIVER_PYTHON=/analytics/anaconda3/bin/python
export PYSPARK_PYTHON=/analytics/anaconda3/bin/python
export SPARK_YARN_USER_ENV="PYSPARK_PYTHON=/analytics/anaconda3/bin/python"
export PYTHONPATH=$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH

python -m unittest discover

pex --no-wheel --disable-cache -i conf/pip.conf -e project.main:main -r requirement.txt -o project.pex .

source deactivate

# Training
/analytics/anaconda3/bin/python project.pex --properties conf/run_config.ini --params conf/config.json --deploy-mode client

# Predict
/analytics/anaconda3/bin/python project.pex --properties conf/run_config.ini --params conf/config.json --deploy-mode client --predict

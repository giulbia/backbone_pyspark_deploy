#!/usr/bin/env bash

source /analytics/anaconda3/bin/activate /analytics/anaconda/envs/deployment

python setup.py bdist_egg

kinit -kt ...

hdfs dfs -put -f dist/*.egg /user/toto
hdfs dfs -chmod 755 /usr/toto/*.egg

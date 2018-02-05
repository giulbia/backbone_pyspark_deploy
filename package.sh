#!/usr/bin/env bash

python -V

source /analytics/anaconda3/bin/activate /analytics/anaconda/envs/deployment

python -V

python -m unittest discover

pex --no-wheel --disable-cache -i conf/pip.conf -e module.main:main -r requirement.txt -o module.pex .

echo "Build done"

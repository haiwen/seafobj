#!/bin/bash

set -e

apt-get update  -q=2
apt-get install python2.7 python-pip python-dev build-essential python-ceph -q=2
apt install python-rados -q=2
pip install --upgrade pip 

pip install -r ./requirements.txt

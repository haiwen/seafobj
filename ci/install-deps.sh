#!/bin/bash

set -e -x

SCRIPT=${BASH_SOURCE[0]}
TESTS_DIR=$(dirname "${SCRIPT}")/..
SETUP_DIR=${TESTS_DIR}/ci

cd $SETUP_DIR

pip install -r requirements.txt

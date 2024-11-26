#!/bin/bash

set -e

SCRIPT=${BASH_SOURCE[0]}
PROJECT_DIR=$(dirname "${SCRIPT}")

cd $PROJECT_DIR

export PYTHONPATH=$PROJECT_DIR:$PYTHONPATH
export SEAFILE_CONF_DIR=$PROJECT_DIR/tests/conf

ci/run.py --test-only

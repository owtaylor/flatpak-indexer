#!/bin/bash

set +e -x

pytest --cov=flatpak_indexer --cov-report=term-missing --cov-fail-under=100 tests
[ $? == 0 ] || failed="$failed pytest"
flake8 flatpak_status tests
[ $? == 0 ] || failed="$failed flake8"

set -e +x

if [ "$failed" != "" ] ; then
    echo "FAILED:$failed"
    exit 1
fi

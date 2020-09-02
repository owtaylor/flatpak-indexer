#!/bin/bash

set +e -x

pytest \
    --cov=flatpak_indexer \
    --cov-report=term-missing \
    --cov-fail-under=100 \
    --disable-socket \
    tests

[ $? == 0 ] || failed="$failed pytest"
flake8 flatpak_indexer setup.py tests tools
[ $? == 0 ] || failed="$failed flake8"

set -e +x

if [ "$failed" != "" ] ; then
    if [[ -t 1 ]] ; then
        echo -e "\e[31m\e[1mFAILED:\e[0m$failed"
    else
        echo -e "FAILED:$failed"
    fi
    exit 1
else
    if [[ -t 1 ]] ; then
        echo -e "\e[32m\e[1mSUCCESS\e[0m"
    else
        echo -e "SUCCESS"
    fi
fi

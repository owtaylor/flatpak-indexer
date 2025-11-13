#!/bin/bash

set +e -x

# These are set to build-time specific location when building the Red Hat-internal
# image; unset them to avoid breaking things at runtime.
unset REQUESTS_CA_BUNDLE
unset GIT_SSL_CAINFO

pytest --cov-fail-under=100 "$@"

[ $? == 0 ] || failed="$failed pytest"
ruff format --check flatpak_indexer tests tools
[ $? == 0 ] || failed="$failed ruff-format"
ruff check flatpak_indexer tests tools
[ $? == 0 ] || failed="$failed ruff-check"

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

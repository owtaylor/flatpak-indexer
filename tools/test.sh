#!/bin/bash

# These are set to build-time specific location when building the Red Hat-internal
# image; unset them to avoid breaking things at runtime.
unset REQUESTS_CA_BUNDLE
unset GIT_SSL_CAINFO

modules=()
pytest_args=()
all=true
failed=""

for arg in "$@" ; do
    case $arg in
        --pytest|--ruff-check|--ruff-format|--pyright)
            modules+=("${arg#--}")
            all=false
            ;;
        *)
            modules+=(pytest)
            pytest_args+=("$arg")
            all=false
            ;;
    esac
done

if $all ; then
    modules=(pytest pyright ruff-check ruff-format)
fi

if [[ "${#pytest_args[@]}" = 0 ]] ; then
    pytest_args=(--cov-fail-under=100)
fi

run() {
    module=$1
    shift
    if [[ " ${modules[*]} " =~ " $module " ]] ; then
        if [[ -t 1 ]] ; then
            echo -e "\e[33m\e[1mRUNNING\e[0m: $*"
        else
            echo "RUNNING: $*"
        fi
        "$@"
        [[ $? -eq 0 ]] || failed="$failed $module"
    fi
}

run pytest pytest "${pytest_args[@]}"
run ruff-format ruff format --check flatpak_indexer tests tools
run ruff-check ruff check flatpak_indexer tests tools
run pyright pyright flatpak_indexer tests tools

if [[ "$failed" != "" ]] ; then
    if [[ -t 1 ]] ; then
        echo -e "\e[31m\e[1mFAILED\e[0m:$failed"
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

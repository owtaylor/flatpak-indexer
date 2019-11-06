#!/bin/bash

set -ex

origin=$(cd $(dirname $0)/.. && pwd)

work=$(mktemp -d)
cleanup() {
    :
#    rm -rf $work
}
trap cleanup EXIT

set -x

s2i build --copy --as-dockerfile=$work/Dockerfile $origin registry.redhat.io/rhscl/python-36-rhel7 flatpak-indexer
podman build $work -t flatpak-indexer

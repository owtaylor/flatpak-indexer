#!/bin/bash

set -ex

origin=$(cd $(dirname $0)/.. && pwd)

work=$(mktemp -d)
cleanup() {
    rm -rf $work
}
trap cleanup EXIT

set -x

s2i build --copy --as-dockerfile=$work/Dockerfile --context-dir=frontend $origin registry.redhat.io/rhscl/httpd-24-rhel7 flatpak-indexer-frontend
podman build $work -t flatpak-indexer-frontend

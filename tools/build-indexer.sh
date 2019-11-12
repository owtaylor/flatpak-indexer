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
tmp_tag="flatpak-indexer:$(date +%Y%m%d-%H%M%S)"
podman build $work -t $tmp_tag
if podman run --rm $tmp_tag tools/test.sh ; then
    podman tag $tmp_tag flatpak-indexer:latest
    podman rmi $tmp_tag
else
    podman rmi $tmp_tag
    exit 1
fi

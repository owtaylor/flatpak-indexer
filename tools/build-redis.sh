#!/bin/bash

set -ex

context=$(cd $(dirname $0)/.. && pwd)/redis
exec podman build $context -t flatpak-indexer-redis

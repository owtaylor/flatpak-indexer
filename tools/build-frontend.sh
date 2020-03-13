#!/bin/bash

set -ex

context=$(cd $(dirname $0)/.. && pwd)/frontend
exec podman build $context -t flatpak-indexer-frontend

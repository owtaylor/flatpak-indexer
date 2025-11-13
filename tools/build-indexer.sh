#!/bin/bash

set -ex

origin=$(cd $(dirname $0)/.. && pwd)

podman build -t flatpak-indexer:latest $origin

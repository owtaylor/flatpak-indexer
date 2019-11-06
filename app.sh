#!/bin/sh

exec flatpak-indexer -v -c /etc/flatpak-indexer/config.yaml daemon

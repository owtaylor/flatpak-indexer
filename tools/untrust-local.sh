#!/bin/bash

set -ex

sudo sh -c 'rm /etc/pki/ca-trust/source/anchors/flatpak_indexer_ca.crt && update-ca-trust'
sudo sh -c 'sed -i /flatpaks.local.fishsoup.net/d /etc/hosts'

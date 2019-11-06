#!/bin/bash

set -ex

tls_secrets_dir=$(cd $(dirname $0)/.. && pwd)/tls-secrets

sudo sh -c "cp $tls_secrets_dir/ca.crt /etc/pki/ca-trust/source/anchors/flatpak_indexer_ca.crt && update-ca-trust"
sudo sh -c 'grep -l flatpaks.local.fishsoup.net /etc/hosts > /dev/null || echo "127.0.0.1       flatpaks.local.fishsoup.net" >> /etc/hosts'
rm -f flatpak_indexer.crt

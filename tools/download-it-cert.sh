#!/bin/bash

topdir=$(cd $(dirname $0)/.. && pwd)

curl -o $topdir/flatpak_indexer/certs/RH-IT-Root-CA.crt https://certs.corp.redhat.com/certs/2022-IT-Root-CA.pem


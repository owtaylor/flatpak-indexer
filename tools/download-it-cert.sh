#!/bin/bash

topdir=$(cd $(dirname $0)/.. && pwd)

curl -o $topdir/flatpak_indexer/certs/RH-IT-Root-CA.crt https://password.corp.redhat.com/RH-IT-Root-CA.crt


#!/bin/bash
set -e

work=$(mktemp -d)
cleanup() {
    rm -rf $work
}
trap cleanup EXIT

tls_secrets_dir=$(cd $(dirname $0)/.. && pwd)/tls-secrets
[ -e $tls_secrets_dir ] || mkdir $tls_secrets_dir

cd $work

# Generate private keys
openssl genrsa -out ca.key 2048
openssl genrsa -out tls.key 2048

# Generate CSRs
cat > ca.config <<EOF
[req]
prompt=no
distinguished_name=cadn
req_extensions=v3_req

[v3_req]
basicConstraints=critical,CA:TRUE,pathlen:0

[cadn]
CN=Flatpak Indexer CA
OU=Flatpak Indexer
emailAddress=nomail@example.com
EOF

cat > cert.config <<EOF
[req]
prompt=no
distinguished_name=certdn
req_extensions=v3_req

[v3_req]
subjectAltName=DNS:flatpaks.local.fishsoup.net
basicConstraints=critical,CA:FALSE

[certdn]
CN=flatpaks.local.fishsoup.net
OU=Flatpak Indexer
emailAddress=nomail@example.com
EOF

openssl req -new -config ca.config -key ca.key -out ca.csr
openssl req -new -config cert.config -key tls.key -out tls_cert.csr

# Generate Root Certificate
openssl x509 -req -in ca.csr -days 365 -extfile ca.config -extensions v3_req -signkey ca.key -out ca.crt

# Generate Server Certificate
openssl x509 -req -in tls_cert.csr -days 365 -extfile cert.config -extensions v3_req -CA ca.crt -CAkey ca.key -CAcreateserial -out tls.crt

# Copy the files to the output directory
cp tls.crt ca.crt tls.key $tls_secrets_dir
# Needs to be world-readable to be read by the apache process
sudo chmod 0644 $tls_secrets_dir/tls.key

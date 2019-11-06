flatpak-indexer
===============

This is a service that reads container data out of
[Pyxis](https://gitlab.cee.redhat.com/rad/pyxis) and writes out an
index in the format that the Flatpak client expects, so that Flatpaks
can be installed from the command line and browsed through GNOME
Software.

Its deployed with two applications:

 * The indexer - runs periodically (eventually perhaps triggered from the universal
   message bus), queries Pyxis to find the currently published Flatpak images
   in the container catalog, and builds indexes from that.

 * The frontend - a web server redirecting requests to the static files that the
   indexer generated.

Configuration
-------------

The indexer service is configured via a YAML file (typically provided as a kubernetes
config map). Example:

``` yaml
pyxis_url: https://pyxis.dev.engineering.redhat.com/v1
# if missing, system trust root is used; if relative, looked up in included certs
pyxis_cert: RH-IT-Root-CA.crt
# When extract_icons is set for an index, icons are saved to icons_dir and labels are
# rewritten in the index from a data: URL to an URL formed from icons_uri
icons_dir: icons/
icons_uri: https://flatpaks.redhat.com/icons/
daemon:
        # How often to query Pyxis
        update_interval: 1800
registries:
        # key must match registry ID in Pyxis
        registry.access.redhat.com:
                # Written into the index
                public_url: 'https://registry.access.redhat.com/v2/'
                # Manual list of Flatpak repositories - will eventually extract from Pyxis
                repositories:
                - ubi8
indexes:
        amd64:
                registry: registry.access.redhat.com
                architecture: amd64
                tag: latest
                # path to the output location - environment variable substitions
                # are possible for all strings
                output: ${OUTPUT_DIR:out}/index/flatpak-amd64.json
                extract_icons: True
```

Development setup
-----------------

``` sh
# DO ONCE
# Download the Red Hat IT root certificate
./tools/download-cert.sh
pipenv --three
pipenv install --dev
# To enter development environmnet
pipenv shell

# DO EVERY TIME

# To try a test run
flatpak-indexer -v -c config-local.yaml
# To check style
flake8
```

Testing frontend image locally
------------------------------

You'll need to have [s2i](https://github.com/openshift/source-to-image) installed on
your system and have run the indexer with `config-local.yaml` as above.

``` sh
# DO ONCE:

# Generate a certificate for flatpaks.local.fishsoup.net
./tools/generate-certs.sh
# And install a hostname and CA cert for that on your system
./tools/trust-local.sh

# DO EVERY TIME

# Build the container
./tools/build-frontend.sh
# Run the container
./tools/run-container.sh
```

You can then go to
[https://flatpaks.local.fishsoup.net:8443/index/static?label:org.flatpak.ref:exists=1&architecture=amd64]
in your browser and should see the JSON output of the indexer.

License
-------
flagstate is distributed is distributed under the [MIT license](LICENSE) .

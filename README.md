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
#pyxis_url: https://catalog.redhat.com/api/containers/v1
pyxis_url: https://pyxis.engineering.redhat.com/v1
# if missing, system trust root is used; if relative, looked up in included certs
pyxis_cert: RH-IT-Root-CA.crt
# these define a client certificate/key pair to authenticate to pyxis; this
# shows environment variable substitution with a fallback
pyxis_client_cert: ${PYXIS_CERT_DIR:${HOME}/.config/flatpak-indexer}/client.crt
pyxis_client_key: ${PYXIS_CERT_DIR:${HOME}/.config/flatpak-indexer}/client.key
# When extract_icons is set for an index, icons are saved to icons_dir and labels are
# rewritten in the index from a data: URL to an URL formed from icons_uri
icons_dir: ${OUTPUT_DIR:out}/icons/
icons_uri: https://flatpaks.local.fishsoup.net.com/app-icons/
daemon:
        # How often to query Pyxis
        update_interval: 1800
registries:
        # key must match registry ID in Pyxis
        registry.access.redhat.com:
                # Written into the index
                public_url: https://registry.access.redhat.com/
                # Manually set list (will replace with Pyxis query)
                repositories:
                - ubi8
        # You can also define a pseudo-registry based on koji/brew builds
        brew:
                # internal registry where builds are pushed
                public_url: https://registry-proxy.engineering.redhat.com/
                # name of a Koji config section
                koji_config: brew
indexes:
        all:
                # path to the output location - environment variable substitions
                # are possible for all strings
                output: ${OUTPUT_DIR:out}/test/flatpak-latest.json
                registry: registry.access.redhat.com
                tag: latest
                extract_icons: True
        amd64:
                output: ${OUTPUT_DIR:out}/test/flatpak-latest-amd64.json
                registry: registry.access.redhat.com
                architecture: amd64
                tag: latest
                extract_icons: True
        # These indexes index all Flatpaks found in the rhel-8.2.0-gate tag
        brew:
                output: ${OUTPUT_DIR:out}/brew/flatpak-rhel-8.2.0-gate.json
                registry: brew
                koji_tag: rhel-8.2.0-gate
                extract_icons: True
        brew-amd64:
                output: ${OUTPUT_DIR:out}/brew/flatpak-rhel-8.2.0-gate-amd64.json
                registry: brew
                architecture: amd64
                koji_tag: rhel-8.2.0-gate
                extract_icons: True
```

Development setup
-----------------

To develop against the internal Pyxis instance, you'll need a client certificate that
authenticates you as a Red Hat user. See https://mojo.redhat.com/docs/DOC-1210484 -
once you have the container working, you'll need the specific instructions under the
"caDirUserCert" section.

``` sh
# DO ONCE

# Put your client certificate/key into place
mkdir -p ~/.config/flatpak-indexer
cp myuser.crt ~/.config/flatpak-indexer/client.crt
cp myuser.key ~/.config/flatpak-indexer/client.key

# Download the Red Hat IT root certificate
./tools/download-cert.sh
pipenv --three
pipenv install --dev
# To enter development environmnet
pipenv shell

# DO EVERY TIME

# To run tests and check style
./tools/test.sh
# To run a specific test
pytest tests -k test_config_basic
# To try a test run against dev Pyxis
flatpak-indexer -v -c config-local.yaml index
```

Development standards
---------------------
All commits must:
 * Test cleanly with flake8 as configured for this project in [.flake8]
 * Contain changes to the test suite so that coverage stays at 100%

Requiring 100% coverage can at times seem onerous, but keeps the criteria
simple, and most cases in Python, untested code is broken code.

Some hints:
 * For wrong user input in the config file, try to check it upfront in
   [flatpak_indexer/config.py], rather than at point of use.
 * For: "this code can never be hit unless something is wrong elsewhere",
   try to just delete the code - especially if there will be a reasonably
   clear Python backtrace.


Testing indexer image locally
-----------------------------

It's also possible to build and run the indexer as an image generated with
s2i, to test it closer to the production setup. You'll need to have
[s2i](https://github.com/openshift/source-to-image) installed on your system.

See the instructions in the "Development setup" above section for obtaining a
client certificate.

``` sh
# DO ONCE

# Put your client certificate/key into place
mkdir -p ~/.config/flatpak-indexer
cp myuser.crt ~/.config/flatpak-indexer/client.crt
cp myuser.key ~/.config/flatpak-indexer/client.key

# Download the Red Hat IT root certificate
./tools/download-cert.sh

# DO EVERY TIME

# Build the container and run tests
./tools/build-indexer.sh
# Run the container
./tools/run-indexer.sh
```

Testing frontend image locally
------------------------------

The frontend configuration can only be tested as a s2i image.
You'll need to have [s2i](https://github.com/openshift/source-to-image) installed on
your system and have run the indexer either with the development setup or as an image.

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
./tools/run-frontend.sh
```

You can then go to
[https://flatpaks.local.fishsoup.net:8443/test/index/static?label:org.flatpak.ref:exists=1&architecture=amd64]
in your browser and should see the JSON output of the indexer.

License
-------
flagstate is distributed is distributed under the [MIT license](LICENSE) .

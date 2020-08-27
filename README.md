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
config map). You can find examples in [config-fedora.yaml](config-local.yaml) and
[config-pyxis.yaml](config-pyxis.yaml).


Development setup (Red Hat)
---------------------------

To develop against the internal Pyxis instance, you'll need a client certificate that
authenticates you as a Red Hat user. See https://mojo.redhat.com/docs/DOC-1210484 -
once you have the container working, you'll need the specific instructions under the
"caDirUserCert" section.

``` sh
# Put your client certificate/key into place
mkdir -p ~/.config/flatpak-indexer
cp myuser.crt ~/.config/flatpak-indexer/client.crt
cp myuser.key ~/.config/flatpak-indexer/client.key

# Download the Red Hat IT root certificate
./tools/download-it-cert.sh
```

Development setup (general)
---------------------------

``` sh
# DO ONCE

pipenv --three
pipenv install --dev

# ENTER YOUR DEVELOPMENT SETUP

pipenv shell

# TEST-DRIVEN WORKFLOW

# To run tests and check style
./tools/test.sh

# To run a specific test
pytest tests -k test_config_basic

# TESTING WITH REAL DATA

# Start redis in one terminal
./tools/run-redis.sh

# Run a differ in another terminal
flatpak-indexer -v -c config-[pyxis|fedora].yaml differ

# And try indexing in a third terminal
flatpak-indexer -v -c config-[pyxis|fedora].yaml index
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

See the instructions in the "Development setup (Red Hat)" section as well
for testing against the internal Red Hat instance.

``` sh
# Build the container and run tests
./tools/build-indexer.sh
# Run the indexer
./tools/run-indexer.sh --fedora --indexer
# In a different terminal, run a differ daemon
./tools/run-indexer.sh --fedora --differ
```

To test against the internal Red Hat Pyxis instance, you would pass --pyxis
instead of --fedora above.

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

You can then load the [Fedora](https://flatpaks.local.fishsoup.net:8443/fedora/index/static?label:org.flatpak.ref:exists=1&architecture=amd64&tag=latest)
or [Pyxis](https://flatpaks.local.fishsoup.net:8443/pyxis/index/static?label:org.flatpak.ref:exists=1&architecture=amd64&tag=latest)
index in your browser, and you should see the correct JSON.

You can also add a Flatpak remote that points to the generated index:

``` sh
flatpak remote-add --user fedora-local oci+https://flatpaks.local.fishsoup.net:8443/fedora/
```

Test Data
---------
The tests for the Fedora datasource use a subset of Fedora package data.

`tools/create-test-data.py` can either download the test data from scratch,
or more usually, update it based on an existing download. This script
is not used directly, instead you run:

`tools/update-test-data.sh` - this updates the test data based either
on the test-data/ directory, if it exists, or from the test-data-cache
git branch.

`tools/update-test-data-cache.sh` - this updates the cache of test data
that is stored in a separate branch of the repository.

Caching a recent version of the test-data in a git branch allows for efficient
continous integration tests.

License
-------
flagstate is distributed is distributed under the [MIT license](LICENSE) .

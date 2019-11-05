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
                # Full path to the output location
                output: flatpak-amd64.json
                extract_icons: True
```

Development setup
-----------------

``` sh
# Once
pipenv --three
pipenv install --dev
# To enter development environmnet
pipenv shell
# To try a test run
flatpak-indexer -v -c config-local.yaml
# To check style
flake8
```

License
-------
flagstate is distributed is distributed under the [MIT license](LICENSE) .

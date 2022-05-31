import os

from setuptools import setup

data_files = []
if os.environ.get('FLATPAK_INDEXER_INSTALL_BINARIES'):
    data_files.append(('bin', ['bin/tar-diff', 'bin/time']))

setup(name='flatpak-indexer',
      version='0.1',
      description='Service to index Flatpak containers',
      author='Owen Taylor',
      author_email='otaylor@redhat.com',
      license='MIT',
      packages=['flatpak_indexer',
                'flatpak_indexer.datasource',
                'flatpak_indexer.datasource.fedora',
                'flatpak_indexer.datasource.pyxis',
                'flatpak_indexer.test'],
      package_data={
          'flatpak_indexer': [
              'certs/*.cert',
              'certs/*.crt',
              'certs/.dummy',
              'messaging-certs/*.pem',
          ],
      },
      data_files=data_files,
      install_requires=[
          'click',
          'koji',
          'redis >= 4.0.0',
          'pika',
          'requests',
          'PyYAML',
          'version_utils >= 0.3.2',
          # Dev requirements - listed in main requirements to make the image
          # image self-contained for testing.
          'fakeredis',
          'flake8',
          'flake8-import-order',
          'iso8601',
          'pytest',
          'pytest-cov',
          'pytest-socket',
          'responses',
          'www-authenticate',
      ],
      entry_points={
          'console_scripts': [
              'flatpak-indexer=flatpak_indexer.cli:cli',
          ],
      })

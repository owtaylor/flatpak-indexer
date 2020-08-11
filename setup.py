from setuptools import setup

setup(name='flatpak-indexer',
      version='0.1',
      description='Service to index Flatpak containers',
      author='Owen Taylor',
      author_email='otaylor@redhat.com',
      license='MIT',
      packages=['flatpak_indexer',
                'flatpak_indexer.datasource',
                'flatpak_indexer.datasource.fedora',
                'flatpak_indexer.datasource.pyxis'],
      package_data={
          'flatpak_indexer': [
              'certs/*.cert',
              'certs/*.crt',
              'certs/.dummy'
          ],
          'flatpak_indexer.datasource.fedora': [
              'messaging-certs/*.pem'
          ]
      },
      install_requires=[
          'click',
          'koji',
          'redis',
          'pika',
          'requests',
          'PyYAML',
          # Dev requirements - listed in main requirements to make the image
          # image self-contained for testing.
          'fakeredis',
          'flake8',
          'iso8601',
          'pytest',
          'pytest-cov',
          'responses',
      ],
      entry_points={
          'console_scripts': [
              'flatpak-indexer=flatpak_indexer.cli:cli',
          ],
      })

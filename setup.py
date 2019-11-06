from setuptools import setup

setup(name='flatpak-indexer',
      version='0.1',
      description='Service to index Flatpak containers',
      author='Owen Taylor',
      author_email='otaylor@redhat.com',
      license='MIT',
      packages=['flatpak_indexer'],
      package_data={
          'flatpak_indexer': [
              'certs/*.cert',
              'certs/*.crt',
              'certs/.dummy'
          ],
      },
      install_requires=[
          'click',
          'requests',
          'PyYAML',
      ],
      entry_points={
          'console_scripts': [
              'flatpak-indexer=flatpak_indexer.cli:cli',
          ],
      })

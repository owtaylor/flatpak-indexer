from setuptools import setup

setup(name='flatpak-indexer',
      version='0.1',
      description='Service to index Flatpak containers',
      author='Owen Taylor',
      author_email='otaylor@redhat.com',
      license='MIT',
      packages=['flatpak_indexer'],
      include_package_data=True,
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

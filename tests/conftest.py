import flatpak_indexer.test


def pytest_configure(config):
    flatpak_indexer.test.rootpath = config.rootpath

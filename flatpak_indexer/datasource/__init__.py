def load_updaters(config):
    datasources = set()

    for index in config.indexes:
        registry = config.registries[index.registry]
        datasources.add(registry.datasource)

    updaters = []

    if 'fedora' in datasources:
        from .fedora import FedoraUpdater
        updaters.append(FedoraUpdater(config))

    if 'pyxis' in datasources:
        from .pyxis import PyxisUpdater
        updaters.append(PyxisUpdater(config))

    return updaters

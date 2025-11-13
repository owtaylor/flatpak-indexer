from typing import Dict, List

from ..config import Config
from ..models import RegistryModel


class Updater:
    def __init__(self, config: Config):
        raise NotImplementedError()

    def start(self):
        raise NotImplementedError()

    def update(self, registry_data: Dict[str, RegistryModel]):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()


def load_updaters(config) -> List[Updater]:
    datasources = set()

    for index in config.indexes:
        registry = config.registries[index.registry]
        datasources.add(registry.datasource)

    updaters: List[Updater] = []

    if "fedora" in datasources:
        from .fedora import FedoraUpdater

        updaters.append(FedoraUpdater(config))

    if "koji" in datasources:
        from .koji import KojiUpdater

        updaters.append(KojiUpdater(config))

    if "pyxis" in datasources:
        from .pyxis import PyxisUpdater

        updaters.append(PyxisUpdater(config))

    return updaters

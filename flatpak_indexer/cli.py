import logging
import time
from typing import Dict

import click

from .cleaner import Cleaner
from .config import Config
from .datasource import load_updaters
from .differ import Differ
from .indexer import Indexer
from .models import RegistryModel


logger = logging.getLogger(__name__)


@click.group()
@click.pass_context
@click.option('--config-file', '-c', required=True,
              help='Config file')
@click.option('-v', '--verbose', is_flag=True,
              help='Show verbose debugging output')
def cli(ctx, config_file, verbose):
    cfg = Config.from_path(config_file)

    ctx.obj = {
        'config': cfg,
    }

    FORMAT = "%(asctime)s:%(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(level=logging.WARNING, format=FORMAT)
    if verbose:
        logging.getLogger('flatpak_indexer').setLevel(logging.INFO)


@cli.command(name="daemon")
@click.pass_context
def daemon(ctx):
    cfg = ctx.obj['config']

    cleaner = Cleaner(cfg)
    updaters = load_updaters(cfg)
    indexer = Indexer(cfg, cleaner=cleaner)

    for updater in updaters:
        updater.start()

    try:
        last_update_time = None
        while True:
            if last_update_time is not None:
                time.sleep(max(0,
                               cfg.daemon.update_interval.total_seconds()
                               - (time.time() - last_update_time)))
            last_update_time = time.time()

            registry_data: Dict[str, RegistryModel] = {}
            for updater in updaters:
                try:
                    updater.update(registry_data)
                except Exception:
                    logger.exception("Failed to update data sources")

            cleaner.reset()

            try:
                indexer.index(registry_data)
            except Exception:
                logger.exception("Failed to create index")
                continue

            try:
                cleaner.clean()
            except Exception:
                logger.exception("Failed to clean unused files")
                continue
    finally:
        # Stopping the updaters (and their worker threads) is needed for the process to exit
        for updater in updaters:
            updater.stop()


@cli.command(name="differ")
@click.pass_context
def differ(ctx):
    cfg = ctx.obj['config']

    differ = Differ(cfg)
    differ.run()


@cli.command(name="index")
@click.pass_context
def index(ctx):
    cfg = ctx.obj['config']

    updaters = load_updaters(cfg)
    indexer = Indexer(cfg)

    registry_data: Dict[str, RegistryModel] = {}
    for updater in updaters:
        updater.start()
        try:
            updater.update(registry_data)
        finally:
            updater.stop()
    indexer.index(registry_data)

import click
import logging
import time

from .config import Config
from .indexer import Indexer


logger = logging.getLogger(__name__)


@click.group()
@click.pass_context
@click.option('--config-file', '-c', required=True,
              help='Config file')
@click.option('-v', '--verbose', is_flag=True,
              help='Show verbose debugging output')
def cli(ctx, config_file, verbose):
    cfg = Config(config_file)

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

    indexer = Indexer(cfg)

    last_update_time = None
    while True:
        if last_update_time is not None:
            time.sleep(max(0, cfg.daemon.update_interval - (time.time() - last_update_time)))
        last_update_time = time.time()

        try:
            indexer.index()
        except Exception:
            logger.exception("Failed to create index")


@cli.command(name="index")
@click.pass_context
def index(ctx):
    cfg = ctx.obj['config']

    indexer = Indexer(cfg)
    indexer.index()

import yaml

from flatpak_indexer.session import Session
from flatpak_indexer.test.koji import mock_koji
from flatpak_indexer.test.redis import mock_redis

from .utils import get_config

CONFIG = yaml.safe_load("""
koji_config: brew
redis_url: redis://localhost
""")


@mock_koji
@mock_redis
def test_build_cache(tmp_path):
    config = get_config(tmp_path, CONFIG)
    build_cache = Session(config).build_cache

    image_a = build_cache.get_image_build("baobab-stable-3520211002221204.1")
    image_b = build_cache.get_image_build("baobab-stable-3520211002221204.1")

    assert image_b is image_a

    module_a = build_cache.get_module_build("baobab-stable-3620220517102805")
    module_b = build_cache.get_module_build("baobab-stable-3620220517102805")

    assert module_b is module_a

    package_b = build_cache.get_package_build("baobab-42.0-1.module_f36+14451+219d93a5")
    package_a = build_cache.get_package_build("baobab-42.0-1.module_f36+14451+219d93a5")

    assert package_b is package_a

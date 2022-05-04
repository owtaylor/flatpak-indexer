import yaml

from flatpak_indexer.build_cache import BuildCache
from .koji import mock_koji
from .redis import mock_redis
from .utils import get_config


CONFIG = yaml.safe_load("""
koji_config: brew
redis_url: redis://localhost
""")


@mock_koji
@mock_redis
def test_build_cache(tmp_path):
    config = get_config(tmp_path, CONFIG)
    build_cache = BuildCache(config)

    image_a = build_cache.get_image_build("baobab-master-3220200331145937.2")
    image_b = build_cache.get_image_build("baobab-master-3220200331145937.2")

    assert image_b is image_a

    module_a = build_cache.get_module_build('baobab-master-3220200331145937')
    module_b = build_cache.get_module_build('baobab-master-3220200331145937')

    assert module_b is module_a

    package_b = build_cache.get_package_build('baobab-3.34.0-2.module_f32+8432+1f88bc5a')
    package_a = build_cache.get_package_build('baobab-3.34.0-2.module_f32+8432+1f88bc5a')

    assert package_b is package_a

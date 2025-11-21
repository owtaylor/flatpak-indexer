from typing import Dict
import json

import pytest
import yaml

from flatpak_indexer.datasource import load_updaters
from flatpak_indexer.datasource.direct import DirectUpdater
from flatpak_indexer.models import RegistryModel
from flatpak_indexer.test.redis import mock_redis

from .registry import MockRegistry, mock_registry
from .utils import get_config


def run_update(updater):
    registry_data: Dict[str, RegistryModel] = {}

    updater.start()
    try:
        updater.update(registry_data)
    finally:
        updater.stop()

    return registry_data


CONFIG = yaml.safe_load("""
redis_url: redis://localhost
koji_config: brew
registries:
    staging:
        public_url: https://registry.example.com/
        datasource: direct
        repositories: ["rhel10/firefox-flatpak", "rhel10/flatpak-runtime"]
indexes:
    rhel10-amd64:
        architecture: amd64
        registry: staging
        output: out/test/flatpak-rhel10-amd64.json
        tag: latest
    rhel10-all:
        registry: staging
        output: out/test/flatpak-rhel10.json
        tag: latest
""")


MULTI_TAG_CONFIG = yaml.safe_load("""
redis_url: redis://localhost
koji_config: brew
registries:
    staging:
        public_url: https://registry.example.com/
        datasource: direct
        repositories: ["rhel10/firefox-flatpak"]
indexes:
    rhel10-latest:
        registry: staging
        output: out/test/flatpak-rhel10-latest.json
        tag: latest
    rhel10-stable:
        registry: staging
        output: out/test/flatpak-rhel10-stable.json
        tag: stable
""")


_FIREFOX_LABELS = {
    "org.flatpak.ref": "app/org.mozilla.Firefox/x86_64/stable",
    "org.freedesktop.appstream.icon-128": "https://www.example.com/icons/firefox.png",
}

_RUNTIME_LABELS = {
    "org.flatpak.ref": "runtime/org.fedoraproject.Platform/x86_64/f40",
}


def make_manifest(config_data, architecture="amd64"):
    """Helper to create OCI manifest with config"""
    return {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": len(json.dumps(config_data)),
            "digest": "sha256:configdigest123",
        },
        "layers": [],
    }, config_data


def make_manifest_list(manifests_by_arch):
    """Helper to create manifest list/index"""
    manifest_list = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [],
    }

    for arch, (manifest, config) in manifests_by_arch.items():
        manifest_list["manifests"].append(
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": f"sha256:manifest{arch}",
                "size": len(json.dumps(manifest)),
                "platform": {
                    "architecture": arch,
                    "os": "linux",
                },
            }
        )

    return manifest_list


@mock_redis
@mock_registry
def test_direct_updater_basic(tmp_path, registry_mock: MockRegistry):
    """Test basic direct updater with single tag, multiple repos, multi-arch"""
    config = get_config(tmp_path, CONFIG)

    # Add Firefox flatpak with multi-arch support
    firefox_amd64_config = {
        "architecture": "amd64",
        "os": "linux",
        "created": "2024-01-15T10:00:00Z",
        "config": {"Labels": _FIREFOX_LABELS},
        "rootfs": {"diff_ids": ["sha256:layer1"]},
    }
    firefox_arm64_config = {
        "architecture": "arm64",
        "os": "linux",
        "created": "2024-01-15T10:00:00Z",
        "config": {"Labels": _FIREFOX_LABELS},
        "rootfs": {"diff_ids": ["sha256:layer2"]},
    }

    firefox_amd64_manifest, _ = make_manifest(firefox_amd64_config, "amd64")
    firefox_arm64_manifest, _ = make_manifest(firefox_arm64_config, "arm64")

    # Add configs as blobs
    firefox_amd64_config_digest, _ = registry_mock.add_blob(
        "rhel10/firefox-flatpak", json.dumps(firefox_amd64_config)
    )
    firefox_arm64_config_digest, _ = registry_mock.add_blob(
        "rhel10/firefox-flatpak", json.dumps(firefox_arm64_config)
    )

    # Update manifests with correct config digests
    firefox_amd64_manifest["config"]["digest"] = firefox_amd64_config_digest
    firefox_arm64_manifest["config"]["digest"] = firefox_arm64_config_digest

    # Create manifest list
    manifest_list_data = make_manifest_list(
        {
            "amd64": (firefox_amd64_manifest, firefox_amd64_config),
            "arm64": (firefox_arm64_manifest, firefox_arm64_config),
        }
    )

    # Add manifests for each architecture
    registry_mock.add_manifest(
        "rhel10/firefox-flatpak",
        None,
        json.dumps(firefox_amd64_manifest),
        fake_digest="sha256:manifestamd64",
    )
    registry_mock.add_manifest(
        "rhel10/firefox-flatpak",
        None,
        json.dumps(firefox_arm64_manifest),
        fake_digest="sha256:manifestarm64",
    )

    # Add manifest list
    registry_mock.add_manifest(
        "rhel10/firefox-flatpak",
        "latest",
        json.dumps(manifest_list_data),
    )

    # Add runtime flatpak (single arch)
    runtime_config = {
        "architecture": "amd64",
        "os": "linux",
        "created": "2024-01-10T08:00:00Z",
        "config": {"Labels": _RUNTIME_LABELS},
        "rootfs": {"diff_ids": ["sha256:layer3"]},
    }

    runtime_config_digest, _ = registry_mock.add_blob(
        "rhel10/flatpak-runtime", json.dumps(runtime_config)
    )

    runtime_manifest, _ = make_manifest(runtime_config)
    runtime_manifest["config"]["digest"] = runtime_config_digest

    registry_mock.add_manifest(
        "rhel10/flatpak-runtime",
        "latest",
        json.dumps(runtime_manifest),
    )

    # Run the updater
    updater = DirectUpdater(config)
    registry_data = run_update(updater)

    # Verify results
    data = registry_data["staging"]
    assert len(data.repositories) == 2

    # Check firefox repository
    firefox_repo = data.repositories["rhel10/firefox-flatpak"]
    # Should have 2 images (amd64 and arm64, but only amd64 is requested)
    # Actually, one index requests amd64, one requests all architectures
    # So we should get both amd64 and arm64
    assert len(firefox_repo.images) == 2

    # Find the amd64 image
    firefox_amd64_image = None
    for image in firefox_repo.images.values():
        if image.labels.get("org.flatpak.ref") == "app/org.mozilla.Firefox/x86_64/stable":
            if "amd64" in image.digest or image.architecture == "amd64":
                firefox_amd64_image = image
                break

    assert firefox_amd64_image is not None
    assert "latest" in firefox_amd64_image.tags

    # Check runtime repository
    runtime_repo = data.repositories["rhel10/flatpak-runtime"]
    assert len(runtime_repo.images) == 1
    runtime_image = next(iter(runtime_repo.images.values()))
    assert (
        runtime_image.labels["org.flatpak.ref"] == "runtime/org.fedoraproject.Platform/x86_64/f40"
    )
    assert "latest" in runtime_image.tags

    # Check tag histories
    assert "latest" in firefox_repo.tag_histories
    firefox_tag_history = firefox_repo.tag_histories["latest"]
    assert firefox_tag_history.name == "latest"
    assert len(firefox_tag_history.items) == 2  # amd64 and arm64

    # Verify tag history has entries for both architectures
    tag_arches = {item.architecture for item in firefox_tag_history.items}
    assert tag_arches == {"amd64", "arm64"}

    assert "latest" in runtime_repo.tag_histories
    runtime_tag_history = runtime_repo.tag_histories["latest"]
    assert runtime_tag_history.name == "latest"
    assert len(runtime_tag_history.items) == 1  # amd64 only
    assert runtime_tag_history.items[0].architecture == "amd64"


@mock_redis
@mock_registry
def test_direct_updater_multiple_tags(tmp_path, registry_mock: MockRegistry):
    """Test direct updater with multiple tags for same repository"""
    config = get_config(tmp_path, MULTI_TAG_CONFIG)

    # Add Firefox flatpak with two different tags
    firefox_latest_config = {
        "architecture": "amd64",
        "os": "linux",
        "created": "2024-01-15T10:00:00Z",
        "config": {"Labels": _FIREFOX_LABELS},
        "rootfs": {"diff_ids": ["sha256:layer1"]},
    }

    firefox_stable_config = {
        "architecture": "amd64",
        "os": "linux",
        "created": "2024-01-10T10:00:00Z",
        "config": {"Labels": _FIREFOX_LABELS},
        "rootfs": {"diff_ids": ["sha256:layer2"]},
    }

    # Add configs as blobs
    latest_config_digest, _ = registry_mock.add_blob(
        "rhel10/firefox-flatpak", json.dumps(firefox_latest_config)
    )
    stable_config_digest, _ = registry_mock.add_blob(
        "rhel10/firefox-flatpak", json.dumps(firefox_stable_config)
    )

    # Create manifests
    latest_manifest, _ = make_manifest(firefox_latest_config)
    latest_manifest["config"]["digest"] = latest_config_digest

    stable_manifest, _ = make_manifest(firefox_stable_config)
    stable_manifest["config"]["digest"] = stable_config_digest

    # Add both tags
    registry_mock.add_manifest(
        "rhel10/firefox-flatpak",
        "latest",
        json.dumps(latest_manifest),
    )
    registry_mock.add_manifest(
        "rhel10/firefox-flatpak",
        "stable",
        json.dumps(stable_manifest),
    )

    # Run the updater
    updater = DirectUpdater(config)
    registry_data = run_update(updater)

    # Verify results
    data = registry_data["staging"]
    assert len(data.repositories) == 1

    firefox_repo = data.repositories["rhel10/firefox-flatpak"]
    # Should have 2 images (one for latest, one for stable)
    assert len(firefox_repo.images) == 2

    # Check that we have both tags
    all_tags = set()
    for image in firefox_repo.images.values():
        all_tags.update(image.tags)

    assert "latest" in all_tags
    assert "stable" in all_tags

    # Check tag histories for both tags
    assert "latest" in firefox_repo.tag_histories
    assert "stable" in firefox_repo.tag_histories

    latest_history = firefox_repo.tag_histories["latest"]
    assert latest_history.name == "latest"
    assert len(latest_history.items) == 1
    assert latest_history.items[0].architecture == "amd64"

    stable_history = firefox_repo.tag_histories["stable"]
    assert stable_history.name == "stable"
    assert len(stable_history.items) == 1
    assert stable_history.items[0].architecture == "amd64"


VALIDATION_ERROR_CONFIG_EMPTY = yaml.safe_load("""
redis_url: redis://localhost
koji_config: brew
registries:
    staging:
        public_url: https://registry.example.com/
        datasource: direct
        repositories: []
indexes:
    test:
        registry: staging
        output: out/test.json
        tag: latest
""")


VALIDATION_ERROR_CONFIG_COLON = yaml.safe_load("""
redis_url: redis://localhost
koji_config: brew
registries:
    staging:
        public_url: https://registry.example.com/
        datasource: direct
        repositories: ["rhel10/firefox-flatpak:latest"]
indexes:
    test:
        registry: staging
        output: out/test.json
        tag: latest
""")


def test_direct_updater_validation_empty_repositories(tmp_path):
    """Test that empty repositories list raises validation error"""
    from flatpak_indexer.base_config import ConfigError

    with pytest.raises(ConfigError, match="repositories.*must.*non-empty|at least one"):
        get_config(tmp_path, VALIDATION_ERROR_CONFIG_EMPTY)


def test_direct_updater_validation_colon_in_repository(tmp_path):
    """Test that colon in repository name raises validation error"""
    from flatpak_indexer.base_config import ConfigError

    with pytest.raises(ConfigError, match="colon|':'"):
        get_config(tmp_path, VALIDATION_ERROR_CONFIG_COLON)


@mock_redis
@mock_registry
def test_direct_updater_single_arch_manifest(tmp_path, registry_mock: MockRegistry):
    """Test direct updater with single architecture (no manifest list)"""
    simple_config = yaml.safe_load("""
    redis_url: redis://localhost
    koji_config: brew
    registries:
        staging:
            public_url: https://registry.example.com/
            datasource: direct
            repositories: ["test/simple"]
    indexes:
        test:
            architecture: amd64
            registry: staging
            output: out/test.json
            tag: v1.0
    """)

    config = get_config(tmp_path, simple_config)

    # Add simple single-arch manifest
    simple_config_data = {
        "architecture": "amd64",
        "os": "linux",
        "created": "2024-01-01T00:00:00Z",
        "config": {"Labels": {"test": "label"}},
        "rootfs": {"diff_ids": ["sha256:layer1"]},
    }

    config_digest, _ = registry_mock.add_blob("test/simple", json.dumps(simple_config_data))

    simple_manifest, _ = make_manifest(simple_config_data)
    simple_manifest["config"]["digest"] = config_digest

    # Add manifest directly (no manifest list)
    registry_mock.add_manifest(
        "test/simple",
        "v1.0",
        json.dumps(simple_manifest),
    )

    # Run the updater
    updater = DirectUpdater(config)
    registry_data = run_update(updater)

    # Verify results
    data = registry_data["staging"]
    assert len(data.repositories) == 1

    simple_repo = data.repositories["test/simple"]
    assert len(simple_repo.images) == 1

    simple_image = next(iter(simple_repo.images.values()))
    assert simple_image.labels["test"] == "label"
    assert "v1.0" in simple_image.tags


def test_load_direct_updater(tmp_path):
    """Test that load_updaters correctly loads DirectUpdater"""
    config = get_config(tmp_path, CONFIG)
    updaters = load_updaters(config)

    # Should have one updater for the direct datasource
    assert len(updaters) == 1
    assert isinstance(updaters[0], DirectUpdater)


@mock_redis
@mock_registry
def test_direct_updater_same_image_multiple_tags(tmp_path, registry_mock: MockRegistry):
    """Test direct updater when the same image is tagged with multiple tags"""
    same_image_config = yaml.safe_load("""
    redis_url: redis://localhost
    koji_config: brew
    registries:
        staging:
            public_url: https://registry.example.com/
            datasource: direct
            repositories: ["test/app"]
    indexes:
        test-latest:
            registry: staging
            output: out/test-latest.json
            tag: latest
        test-stable:
            registry: staging
            output: out/test-stable.json
            tag: stable
    """)

    config = get_config(tmp_path, same_image_config)

    # Create a single image config
    image_config = {
        "architecture": "amd64",
        "os": "linux",
        "created": "2024-01-15T10:00:00Z",
        "config": {"Labels": {"org.flatpak.ref": "app/test.App/x86_64/stable"}},
        "rootfs": {"diff_ids": ["sha256:layer1"]},
    }

    config_digest, _ = registry_mock.add_blob("test/app", json.dumps(image_config))

    manifest_data, _ = make_manifest(image_config)
    manifest_data["config"]["digest"] = config_digest

    # Tag the same manifest with both "latest" and "stable"
    manifest_digest_latest = registry_mock.add_manifest(
        "test/app", "latest", json.dumps(manifest_data)
    )
    manifest_digest_stable = registry_mock.add_manifest(
        "test/app", "stable", json.dumps(manifest_data)
    )

    # The same manifest should get the same digest
    assert manifest_digest_latest == manifest_digest_stable

    # Run the updater
    updater = DirectUpdater(config)
    registry_data = run_update(updater)

    # Verify results
    data = registry_data["staging"]
    assert len(data.repositories) == 1

    app_repo = data.repositories["test/app"]
    # Should have only 1 image (same digest), but tagged with both tags
    assert len(app_repo.images) == 1

    image = next(iter(app_repo.images.values()))
    assert sorted(image.tags) == ["latest", "stable"]
    assert image.labels["org.flatpak.ref"] == "app/test.App/x86_64/stable"

    # Check tag histories for both tags
    assert "latest" in app_repo.tag_histories
    assert "stable" in app_repo.tag_histories

    latest_history = app_repo.tag_histories["latest"]
    assert latest_history.name == "latest"
    assert len(latest_history.items) == 1
    assert latest_history.items[0].architecture == "amd64"
    assert latest_history.items[0].digest == image.digest

    stable_history = app_repo.tag_histories["stable"]
    assert stable_history.name == "stable"
    assert len(stable_history.items) == 1
    assert stable_history.items[0].architecture == "amd64"
    assert stable_history.items[0].digest == image.digest

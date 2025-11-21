from typing import Any
import logging

from .models import ImageModel
from .registry_client import RegistryClient
from .session import Session

# registry-image:<digest> - json representation of the Image
KEY_PREFIX_REGISTRY_IMAGE = "registry-image:"

logger = logging.getLogger(__name__)


def query_registry_image(
    session: Session,
    registry_client: RegistryClient,
    repository_name: str,
    digest: str,
    log_as: str,
) -> ImageModel:
    """
    Query an image from a registry with caching in redis


    Args:
       session:
       registry_client:
       repository_name:
       digest:
       log_as: identifying string, if actually fetching from the registry
    Returns:
       ImageModel: the retrieved image. Note that the tags and pull_spec fields
          are unset since they can depend on our config and shouldn't be cached.
    """

    raw = session.redis_client.get(KEY_PREFIX_REGISTRY_IMAGE + digest)
    if raw:
        image = ImageModel.from_json_text(raw, check_current=True)
        if image:
            return image

    logger.info("Fetching manifest and config for %s", log_as)
    manifest = registry_client.get_manifest(repository_name, digest)
    config = registry_client.get_config(repository_name, manifest)
    image = make_registry_image(manifest, config, digest)

    session.redis_client.set(KEY_PREFIX_REGISTRY_IMAGE + digest, image.to_json_text())

    return image


def make_registry_image(
    manifest: dict[str, Any],
    config: dict[str, Any],
    digest: str,
) -> ImageModel:
    """
    Create an image from manifest and config

    Args:
       manifest: the decoded JSON manifest for the image
       config: the decoded JSON config for the image
       digest: the image digest (hash of the JSON manifest)
    Returns:
       ImageModel: the retrieved image. Note that the tags and pull_spec fields
          are unset since they can depend on our config and shouldn't be cached.
    """

    return ImageModel(
        digest=digest,
        media_type=manifest["mediaType"],
        os=config["os"],
        architecture=config["architecture"],
        labels=config["config"]["Labels"],
        annotations={},
        tags=[],
        diff_ids=config["rootfs"]["diff_ids"],
    )

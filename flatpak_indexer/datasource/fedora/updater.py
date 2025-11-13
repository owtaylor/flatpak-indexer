from collections import defaultdict
from typing import DefaultDict, Dict, List, NamedTuple, Optional, Set

import redis

from ...bodhi_query import (
    list_updates,
    refresh_all_updates,
    refresh_update_status,
    reset_update_cache,
)
from ...config import Config
from ...fedora_monitor import FedoraMonitor
from ...models import (
    BodhiUpdateModel,
    ImageBuildModel,
    ImageModel,
    RegistryModel,
    TagHistoryItemModel,
    TagHistoryModel,
)
from ...session import Session
from ...utils import unparse_pull_spec
from .. import Updater


class UpdateBuild(NamedTuple):
    update: BodhiUpdateModel
    build: ImageBuildModel


class StableUpdateBuild(UpdateBuild):
    @property
    def date_stable(self):
        assert self.update.date_stable
        return self.update.date_stable


class TestingUpdateBuild(UpdateBuild):
    @property
    def date_testing(self):
        assert self.update.date_testing
        return self.update.date_testing


class RepoInfo:
    def __init__(self):
        self.testing_updates: List[TestingUpdateBuild] = []
        self.stable_updates: List[StableUpdateBuild] = []


def _set_build_image_tags(image_build: ImageBuildModel, tags: List[str]):
    for image in image_build.images:
        image.tags = tags


def _fix_pull_spec(image: ImageModel, registry_url: str, repo_name: str):
    # Replace the image pull spec which points to the candidate registry
    # to the final location of the image - this will be more robust if builds
    # are deleted from the candidate registry
    image.pull_spec = unparse_pull_spec(registry_url, repo_name, image.digest)


class FedoraUpdater(Updater):
    redis_client: "redis.Redis[bytes]"
    change_monitor: Optional[FedoraMonitor]

    def __init__(self, config: Config):
        self.conf = config

        self.change_monitor = None
        self.queue_name = None

    def start(self):
        self.change_monitor = FedoraMonitor(self.conf, watch_bodhi_updates=True)
        self.change_monitor.start()

    def update(self, registry_data):
        session = Session(self.conf)

        assert self.change_monitor, "start() must be called before update()"

        changed, serial = self.change_monitor.get_bodhi_changed()
        if changed is None:
            # If we reconnected to a different queue, we don't have any
            # information about the status of cached updates, and need to start over
            reset_update_cache(session)
        else:
            for bodhi_update_id in changed:
                refresh_update_status(session, bodhi_update_id)

        # Now we've updated, we can remove old entries from the log
        self.change_monitor.clear_bodhi_changed(serial)

        refresh_all_updates(session, content_type="flatpak")
        updates = list_updates(session, content_type="flatpak")
        builds = {
            nvr: session.build_cache.get_image_build(nvr)
            for update in updates
            for nvr in update.builds
        }

        repos: Dict[str, RepoInfo] = {}

        for update in updates:
            for build_nvr in update.builds:
                build = builds[build_nvr]

                if build.repository not in repos:
                    repos[build.repository] = RepoInfo()

                repo = repos[build.repository]

                if update.date_testing:
                    repo.testing_updates.append(TestingUpdateBuild(update, build))
                if update.date_stable:
                    repo.stable_updates.append(StableUpdateBuild(update, build))

        for repo_name, repo in repos.items():
            # Find the current testing update - the status might be 'stable' if it's been
            # moved to stable afterwards
            current_testing = max(
                (
                    update_build
                    for update_build in repo.testing_updates
                    if update_build.update.status in ("testing", "stable")
                ),
                key=lambda ub: ub.date_testing,
                default=None,
            )

            # Discard any updates that have date_testing after the current update - they
            # must have been unpushed from testing
            repo.testing_updates = [
                update_build
                for update_build in repo.testing_updates
                if (current_testing and current_testing.date_testing >= update_build.date_testing)
            ]

            # Sort the newest first
            repo.testing_updates.sort(key=lambda ub: ub.date_testing, reverse=True)

            # Now the same for stable
            current_stable = max(
                (
                    update_build
                    for update_build in repo.stable_updates
                    if update_build.update.status == "stable"
                ),
                key=lambda ub: ub.date_stable,
                default=None,
            )
            repo.stable_updates = [
                update_build
                for update_build in repo.stable_updates
                if (current_stable and current_stable.date_stable >= update_build.date_stable)
            ]
            repo.stable_updates.sort(key=lambda ub: ub.date_stable, reverse=True)

        registry_statuses: DefaultDict[str, Set[str]] = defaultdict(set)
        for index_config in self.conf.get_indexes_for_datasource("fedora"):
            registry_name = index_config.registry

            assert index_config.bodhi_status  # config.py enforces this for fedora datasource

            registry_statuses[registry_name].add(index_config.bodhi_status)

        for registry_name, statuses in registry_statuses.items():
            registry = RegistryModel()
            registry_url = self.conf.registries[registry_name].public_url

            need_testing = "testing" in registry_statuses[registry_name]
            need_stable = "stable" in registry_statuses[registry_name]

            for repo_name, repo in repos.items():
                if repo.testing_updates:
                    latest_testing_build = repo.testing_updates[0].build
                else:
                    latest_testing_build = None
                if repo.stable_updates:
                    latest_stable_build = repo.stable_updates[0].build
                else:
                    latest_stable_build = None

                # Set the tags on images based on what is current
                if (
                    latest_testing_build
                    and latest_stable_build
                    and latest_testing_build is latest_stable_build
                ):
                    _set_build_image_tags(latest_testing_build, ["latest", "testing"])
                else:
                    if latest_testing_build:
                        _set_build_image_tags(latest_testing_build, ["testing"])
                    if latest_stable_build:
                        _set_build_image_tags(latest_stable_build, ["latest"])

                # Now build the image list and tag history
                if need_testing and repo.testing_updates:
                    tag_history = TagHistoryModel(name="testing")

                    for update, build in repo.testing_updates:
                        for image in build.images:
                            _fix_pull_spec(image, registry_url, repo_name)
                            registry.add_image(repo_name, image)
                            item = TagHistoryItemModel(
                                architecture=image.architecture,
                                date=update.date_testing,
                                digest=image.digest,
                            )
                            tag_history.items.append(item)

                    registry.repositories[repo_name].tag_histories["testing"] = tag_history

                if need_stable and repo.stable_updates:
                    tag_history = TagHistoryModel(name="latest")

                    for update, build in repo.stable_updates:
                        for image in build.images:
                            _fix_pull_spec(image, registry_url, repo_name)
                            registry.add_image(repo_name, image)
                            item = TagHistoryItemModel(
                                architecture=image.architecture,
                                date=update.date_stable,
                                digest=image.digest,
                            )
                            tag_history.items.append(item)

                    registry.repositories[repo_name].tag_histories["latest"] = tag_history

            registry_data[registry_name] = registry

    def stop(self):
        assert self.change_monitor, "start() must be called before stop()"
        self.change_monitor.stop()

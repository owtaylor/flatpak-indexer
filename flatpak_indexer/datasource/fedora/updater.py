import json
import os

import koji
import redis

from ...utils import atomic_writer
from ...models import RegistryModel, TagHistoryModel, TagHistoryItemModel

from .bodhi_change_monitor import BodhiChangeMonitor
from .koji_query import query_flatpak_build
from .bodhi_query import (list_updates, refresh_all_updates,
                          refresh_update_status, reset_update_cache)


class RepoInfo:
    def __init__(self):
        self.testing_updates = []
        self.latest_stable = None
        self.stable_updates = []
        self.latest_testing = None


def _set_build_image_tags(update, tags):
    for image in update.images:
        image.tags = tags


class FedoraUpdater(object):
    def __init__(self, config):
        self.conf = config

        self.redis_client = redis.Redis.from_url(self.conf.redis_url)
        self.change_monitor = None

        options = koji.read_config(profile_name=self.conf.koji_config)
        koji_session_opts = koji.grab_session_options(options)
        self.koji_session = koji.ClientSession(options['server'], koji_session_opts)

    def start(self):
        queue_name_raw = self.redis_client.get('fedora-messaging-queue')
        queue_name = queue_name_raw.decode('utf-8') if queue_name_raw else None
        self.change_monitor = BodhiChangeMonitor(queue_name)
        new_queue_name = self.change_monitor.start()
        if new_queue_name != queue_name:
            # If we couldn't connect to an existing update queue, we don't have any
            # information about the status of cached updates, and need to start over
            reset_update_cache(self.redis_client)

            self.redis_client.set('fedora-messaging-queue', new_queue_name)

    def update(self):
        for bodhi_update_id in self.change_monitor.get_changed():
            refresh_update_status(self.koji_session, self.redis_client, bodhi_update_id)

        refresh_all_updates(self.koji_session, self.redis_client, content_type='flatpak')
        updates = list_updates(self.redis_client, content_type='flatpak')
        builds = {update.update_id:
                  query_flatpak_build(self.koji_session, self.redis_client, update.builds[0])
                  for update in updates}

        repos = {}

        for update in updates:
            build = builds[update.update_id]

            if build.repository not in repos:
                repos[build.repository] = RepoInfo()

            repo = repos[build.repository]

            if update.date_testing:
                repo.testing_updates.append(update)
            if update.date_stable:
                repo.stable_updates.append(update)

        for repo in repos.values():
            # Find the current testing update - the status might be 'stable' if it's been
            # moved to stable afterwards
            current_testing = max((r for r in repo.testing_updates
                                  if r.status in ('testing', 'stable')),
                                  key=lambda r: r.date_testing, default=None)

            # Discard any updates that have date_testing after the current update - they
            # must have been unpushed from testing
            repo.testing_updates = [r for r in repo.testing_updates
                                    if (current_testing and
                                        current_testing.date_testing >= r.date_testing)]

            # Sort the newest first
            repo.testing_updates.sort(key=lambda r: r.date_testing, reverse=True)

            # Now the same for stable
            current_stable = max((r for r in repo.testing_updates
                                  if r.status == 'stable'),
                                 key=lambda r: r.date_stable, default=None)
            repo.stable_updates = [r for r in repo.stable_updates if
                                   current_stable and current_stable.date_stable >= r.date_stable]
            repo.stable_updates.sort(key=lambda r: r.date_stable, reverse=True)

        registry_statuses = {}
        for index_config in self.conf.indexes:
            registry_name = index_config.registry
            if self.conf.registries[registry_name].datasource != 'fedora':
                continue

            if registry_name not in registry_statuses:
                registry_statuses[registry_name] = set()

            registry_statuses[registry_name].add(index_config.bodhi_status)

        for registry_name, statuses in registry_statuses.items():
            registry = RegistryModel()

            need_testing = 'testing' in registry_statuses[registry_name]
            need_stable = 'stable' in registry_statuses[registry_name]

            for repo_name, repo in repos.items():
                testing_builds = [builds[u.update_id] for u in repo.testing_updates]
                stable_builds = [builds[u.update_id] for u in repo.stable_updates]

                # Set the tags on images based on what is current
                if (testing_builds and stable_builds and
                        testing_builds[0] is stable_builds[0]):
                    _set_build_image_tags(testing_builds[0], ["latest", "testing"])
                else:
                    if testing_builds:
                        _set_build_image_tags(testing_builds[0], ["testing"])
                    if stable_builds:
                        _set_build_image_tags(stable_builds[0], ["latest"])

                # Now build the image list and tag history
                if need_testing and testing_builds:
                    tag_history = TagHistoryModel(name="testing")

                    for update, build in zip(repo.testing_updates, testing_builds):
                        for image in build.images:
                            registry.add_image(repo_name, image)
                            item = TagHistoryItemModel(architecture=image.architecture,
                                                       date=update.date_testing,
                                                       digest=image.digest)
                            tag_history.items.append(item)

                    registry.repositories[repo_name].tag_histories["testing"] = tag_history
                    print(tag_history.to_json())

                if need_stable and stable_builds:
                    tag_history = TagHistoryModel(name="latest")

                    for update, build in zip(repo.stable_updates, stable_builds):
                        for image in build.images:
                            registry.add_image(repo_name, image)
                            item = TagHistoryItemModel(architecture=image.architecture,
                                                       date=update.date_stable,
                                                       digest=image.digest)
                            tag_history.items.append(item)

                    registry.repositories[repo_name].tag_histories["latest"] = tag_history

            filename = os.path.join(self.conf.work_dir, registry_name + ".json")
            with atomic_writer(filename) as writer:
                json.dump(registry.to_json(),
                          writer, sort_keys=True, indent=4, ensure_ascii=False)

    def stop(self):
        self.change_monitor.stop()

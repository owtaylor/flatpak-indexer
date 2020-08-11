import json
import os

import koji
import redis

from ...utils import atomic_writer
from ...models import RegistryModel

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
            repo.latest_testing = max((r for r in repo.testing_updates
                                       if r.status in ('testing', 'stable')),
                                      key=lambda r: r.date_testing, default=None)
            repo.latest_stable = max((r for r in repo.stable_updates
                                      if r.status == 'stable'),
                                     key=lambda r: r.date_stable, default=None)

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
                testing_build = (builds[repo.latest_testing.update_id]
                                 if repo.latest_testing else None)
                stable_build = (builds[repo.latest_stable.update_id]
                                if repo.latest_stable else None)

                if (testing_build and
                        stable_build and
                        testing_build is stable_build):
                    _set_build_image_tags(testing_build, ["latest", "testing"])
                else:
                    if testing_build:
                        _set_build_image_tags(testing_build, ["testing"])
                    if stable_build:
                        _set_build_image_tags(stable_build, ["latest"])

                if need_testing and testing_build:
                    for image in testing_build.images:
                        registry.add_image(repo_name, image)

                if need_stable and stable_build:
                    for image in stable_build.images:
                        registry.add_image(repo_name, image)

            filename = os.path.join(self.conf.work_dir, registry_name + ".json")
            with atomic_writer(filename) as writer:
                json.dump(registry.to_json(),
                          writer, sort_keys=True, indent=4, ensure_ascii=False)

    def stop(self):
        self.change_monitor.stop()

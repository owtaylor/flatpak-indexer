import codecs
import base64
from datetime import datetime, timezone
import hashlib
import logging
import json
import os
from tempfile import NamedTemporaryFile
from urllib.parse import urljoin

from .utils import get_retrying_requests_session


logger = logging.getLogger(__name__)

MEDIA_TYPE_MANIFEST_V2 = 'application/vnd.docker.distribution.manifest.v2+json'

DATA_URI_PREFIX = 'data:image/png;base64,'


class IconStore(object):
    def __init__(self, icons_dir, icons_uri):
        self.icons_dir = icons_dir
        self.icons_uri = icons_uri

        self.old_icons = {}
        for f in os.listdir(icons_dir):
            fullpath = os.path.join(icons_dir, f)
            if os.path.isdir(fullpath):
                for filename in os.listdir(fullpath):
                    self.old_icons[(f, filename)] = True

        self.icons = {}

    def store(self, uri):
        if not uri.startswith(DATA_URI_PREFIX):
            return None

        decoded = base64.b64decode(uri[len(DATA_URI_PREFIX):])

        h = hashlib.sha256()
        h.update(decoded)
        digest = h.hexdigest()
        subdir = digest[:2]
        filename = digest[2:] + '.png'

        key = (subdir, filename)
        if key in self.icons:
            pass
        elif key in self.old_icons:
            self.icons[key] = True
        else:
            if not os.path.exists(os.path.join(self.icons_dir, subdir)):
                os.mkdir(os.path.join(self.icons_dir, subdir))
            fullpath = os.path.join(self.icons_dir, subdir, filename)
            logger.info("Storing icon: %s", fullpath)
            with open(os.path.join(self.icons_dir, subdir, filename), 'wb') as f:
                f.write(decoded)
            self.icons[key] = True

        return urljoin(self.icons_uri, subdir + '/' + filename)

    def clean(self):
        for key in self.old_icons:
            if key not in self.icons:
                subdir, filename = key
                fullpath = os.path.join(self.icons_dir, subdir, filename)
                os.unlink(fullpath)
                logger.info("Removing icon: %s", fullpath)


class Index:
    def __init__(self, conf, registry_public_url, icon_store=None):
        self.registry_public_url = registry_public_url
        self.config = conf
        self.icon_store = icon_store
        self.repos = {}

    def extract_icon(self, labels, key):
        if not self.config.extract_icons:
            return

        value = labels.get(key)
        if value is None:
            return

        uri = self.icon_store.store(value)
        if uri is not None:
            labels[key] = uri

    def make_image(self, name, image_info, all_tags):
        arch = image_info['architecture']
        os = image_info['parsed_data']['os']

        labels = {label['name']: label['value']
                  for label in image_info['parsed_data'].get('labels', [])}
#        if not 'org.flatpak.ref' in labels:
#            return None

        self.extract_icon(labels, 'org.freedesktop.appstream.icon-64')
        self.extract_icon(labels, 'org.freedesktop.appstream.icon-128')

        image = {
            'Digest': image_info['docker_image_id'],
            'MediaType': MEDIA_TYPE_MANIFEST_V2,
            'OS': os,
            'Architecture':  arch,
            'Labels': labels,
        }

        image['Tags'] = all_tags

        return image

    def add_image(self, name, image_info, all_tags):
        if name not in self.repos:
            self.repos[name] = {
                "Name": name,
                "Images": [],
                "Lists": [],
            }

        repo = self.repos[name]

        image = self.make_image(name, image_info, all_tags)
        if image:
            repo["Images"].append(image)

    def write(self):
        # We auto-create only one level and don't use os.makedirs,
        # to better catch configuration mistakes
        output_dir = os.path.dirname(self.config.output)
        if not os.path.isdir(output_dir):
            os.mkdir(output_dir)

        tmpfile = NamedTemporaryFile(delete=False,
                                     dir=output_dir,
                                     prefix=os.path.basename(self.config.output))
        success = False
        try:
            filtered_repos = (v for v in self.repos.values() if v['Images'] or v['Lists'])
            sorted_repos = sorted(filtered_repos, key=lambda r: r['Name'])
            for repo in sorted_repos:
                repo["Images"].sort(key=lambda x: x["Tags"])
                repo["Lists"].sort(key=lambda x: x["Tags"])

            writer = codecs.getwriter("utf-8")(tmpfile)
            json.dump({
                'Registry': self.registry_public_url,
                'Results': sorted_repos,
            }, writer, sort_keys=True, indent=4, ensure_ascii=False)
            writer.close()
            tmpfile.close()

            # We don't overwrite unchanged files, so that the modtime and
            # httpd-computed ETag stay the same.

            changed = True
            if os.path.exists(self.config.output):
                h1 = hashlib.sha256()
                with open(self.config.output, "rb") as f:
                    h1.update(f.read())
                h2 = hashlib.sha256()
                with open(tmpfile.name, "rb") as f:
                    h2.update(f.read())

                if h1.digest() == h2.digest():
                    changed = False

            if changed:
                # Atomically write over result
                os.chmod(tmpfile.name, 0o644)
                os.rename(tmpfile.name, self.config.output)
                logger.info("Wrote %s", self.config.output)
            else:
                logger.info("%s is unchanged", self.config.output)
                os.unlink(tmpfile.name)

            success = True
        finally:
            if not success:
                tmpfile.close()
                os.unlink(tmpfile.name)


class Registry:
    def __init__(self, name, global_config, page_size):
        self.name = name
        self.global_config = global_config
        self.config = global_config.registries[name]
        self.page_size = page_size

    def _iterate_all_images(self, repository):
        session = get_retrying_requests_session()

        logger.info("Getting all images for {}/{}".format(self.name, repository))
        page_size = self.page_size
        page = 0
        while True:
            url = ('{api_url}repositories/registry/{registry}/repository/{repository}/images' +
                   '?page_size={page_size}&page={page}').format(
                       api_url=self.global_config.pyxis_url,
                       registry=self.name,
                       repository=repository,
                       page_size=page_size,
                       page=page)
            logger.info("Requesting {}".format(url))

            kwargs = {
            }

            if self.global_config.pyxis_cert is None:
                kwargs['verify'] = True
            else:
                kwargs['verify'] = self.global_config.pyxis_cert

            if self.global_config.pyxis_client_cert:
                kwargs['cert'] = (self.global_config.pyxis_client_cert,
                                  self.global_config.pyxis_client_key)

            response = session.get(url, headers={'Accept': 'application/json'}, **kwargs)
            response.raise_for_status()

            response_json = response.json()

            for image in response_json['data']:
                yield image

            if response_json['total'] <= page_size * page + len(response_json['data']):
                break

            page += 1

    def _iterate_repository_images(self, repository, desired_tags):
        image_by_tag_arch = {}

        for image_info in self._iterate_all_images(repository):
            arch = image_info['architecture']

            repository_info = None
            for ri in image_info['repositories']:
                if ri['repository'] == repository and \
                   ri['registry'] == self.name:
                    repository_info = ri
                    break

            if repository_info:
                for tag in repository_info['tags']:
                    tag_name = tag['name']
                    tag_date = parse_date(tag['added_date'])
                    if tag_name in desired_tags:
                        key = tag_name, arch
                        info = image_by_tag_arch.get(key)
                        if info is None or tag_date > info[0]:
                            image_by_tag_arch[key] = (tag_date,
                                                      image_info,
                                                      repository_info)

        for (tag_name, arch), (_, image_info, repository_info) in image_by_tag_arch.items():
            all_tags = sorted({tag["name"] for tag in repository_info['tags']})

            yield tag_name, arch, image_info, all_tags

    def iterate_images(self, desired_tags):
        for repository in self.config.repositories:
            for tag_name, arch, image_info, all_tags in \
                self._iterate_repository_images(repository, desired_tags):

                yield repository, tag_name, arch, image_info, all_tags


def parse_date(date_str):
    dt = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f+00:00')
    dt.replace(tzinfo=timezone.utc)

    return dt


class Indexer(object):
    def __init__(self, config, page_size=50):
        self.conf = config
        self.page_size = page_size

    def index(self):
        icon_store = None
        if self.conf.icons_dir is not None:
            icon_store = IconStore(self.conf.icons_dir, self.conf.icons_uri)

        registries = {}
        indexes_by_registry = {}
        for index_config in self.conf.indexes:
            registry_name = index_config.registry
            if registry_name not in registries:
                registries[registry_name] = Registry(registry_name,
                                                     self.conf,
                                                     self.page_size)
            registry = registries[registry_name]

            indexes = indexes_by_registry.setdefault(registry_name, [])
            indexes.append(Index(index_config,
                                 self.conf.registries[index_config.registry].public_url,
                                 icon_store=icon_store))

        for registry_name, indexes in indexes_by_registry.items():
            registry = registries[registry_name]
            desired_tags = {index.config.tag for index in indexes}

            for repository, tag_name, arch, image_info, all_tags in \
                registry.iterate_images(desired_tags):

                for index in indexes:
                    if (tag_name == index.config.tag and
                        (index.config.architecture is None or
                         arch == index.config.architecture)):

                        index.add_image(repository, image_info, all_tags)

            for index in indexes:
                index.write()

        if icon_store is not None:
            icon_store.clean()

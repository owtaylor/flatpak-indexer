import codecs
import base64
from datetime import datetime, timezone
import hashlib
import logging
import json
import requests
import os
import re
from urllib.parse import urljoin


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

    def make_image(self, name, image_info, repository_info):
        arch = image_info['architecture']
        os = image_info['parsed_data']['os']

        labels = {label['name']: label['value']
                  for label in image_info['parsed_data'].get('labels', {})}
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

        image['Tags'] = sorted({tag["name"] for tag in repository_info['tags']})

        return image

    def add_image(self, name, image_info, repository_info):
        if name not in self.repos:
            self.repos[name] = {
                "Name": name,
                "Images": [],
                "Lists": [],
            }

        repo = self.repos[name]

        image = self.make_image(name, image_info, repository_info)
        if image:
            repo["Images"].append(image)

    def write(self):
        with open(self.config.output, 'wb') as f:
            filtered_repos = (v for v in self.repos.values() if v['Images'] or v['Lists'])
            sorted_repos = sorted(filtered_repos, key=lambda r: r['Name'])
            for repo in sorted_repos:
                repo["Images"].sort(key=lambda x: x["Tags"])
                repo["Lists"].sort(key=lambda x: x["Tags"])

            writer = codecs.getwriter("utf-8")(f)
            json.dump({
                'Registry': self.registry_public_url,
                'Results': sorted_repos,
            }, writer, sort_keys=True, indent=4, ensure_ascii=False)
            writer.close()
        logger.info("Wrote %s", self.config.output)


def parse_date(date_str):
    if re.match(r'\.\d+Z', date_str):
        dt = datetime.strptime("2014-01-01T00:00:00Z", '%Y-%m-%dT%H:%M:%S.%fZ')
    else:
        dt = datetime.strptime("2014-01-01T00:00:00Z", '%Y-%m-%dT%H:%M:%SZ')
    dt.replace(tzinfo=timezone.utc)

    return dt


class Indexer(object):
    def __init__(self, config):
        self.conf = config

    def iterate_images(self, registry, repository):
        logger.info("Getting all images for {}/{}".format(registry, repository))
        page_size = 50
        page = 0
        while True:
            url = ('{api_url}repositories/registry/{registry}/repository/{repository}/images' +
                   '?page_size={page_size}&page={page}').format(
                       api_url=self.conf.pyxis_url,
                       registry=registry,
                       repository=repository,
                       page_size=page_size,
                       page=page)
            logger.info("Requesting {}".format(url))

            if self.conf.pyxis_cert is None:
                verify = True
            else:
                verify = self.conf.pyxis_cert


            response = requests.get(url, headers={'Accept': 'application/json'}, verify=verify)
            response.raise_for_status()

            response_json = response.json()

            for image in response_json['data']:
                yield image

            if response_json['total'] <= page_size * page + len(response_json['data']):
                break

            page += 1

    def index(self):
        icon_store = None
        if self.conf.icons_dir is not None:
            icon_store = IconStore(self.conf.icons_dir, self.conf.icons_uri)

        indexes_by_registry = {}
        for index_config in self.conf.indexes:
            indexes = indexes_by_registry.setdefault(index_config.registry, [])
            indexes.append(Index(index_config,
                                 self.conf.registries[index_config.registry].public_url,
                                 icon_store=icon_store))

        for registry, indexes in indexes_by_registry.items():
            registry_config = self.conf.registries.get(registry)
            for repository in registry_config.repositories:
                desired_tags = {index.config.tag for index in indexes}
                image_by_tag = {}

                for image_info in self.iterate_images(registry, repository):
                    arch = image_info['architecture']
                    image_by_tag_arch = image_by_tag.setdefault(arch, {})

                    repository_info = None
                    for repository_info in image_info['repositories']:
                        if repository_info['repository'] != repository or \
                           repository_info['registry'] != registry:
                            continue

                    if repository_info:
                        for tag in repository_info['tags']:
                            tag_name = tag['name']
                            tag_date = parse_date(tag['added_date'])
                            if tag_name in desired_tags:
                                if tag_name not in image_by_tag_arch or \
                                   tag_date > image_by_tag_arch[tag_name][0]:
                                    image_by_tag_arch[tag_name] = (tag_date,
                                                                   image_info,
                                                                   repository_info)

                for index in indexes:
                    for arch in image_by_tag:
                        if index.config.architecture is None or arch == index.config.architecture:
                            info = image_by_tag[arch].get(index.config.tag)
                            if info:
                                index.add_image(repository, info[1], info[2])

            for index in indexes:
                index.write()

        if icon_store is not None:
            icon_store.clean()

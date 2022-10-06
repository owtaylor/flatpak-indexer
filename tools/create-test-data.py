#!/usr/bin/env python3

import gzip
import json
import os
import shutil
import subprocess
import sys
import tempfile

import click
import koji
import requests


# The data we use is meant to be a point-in-time snapshot of Fedora at this
# date.
DATE = "2022-10-01 00:00:00"
TAGS = ""


def show(msg, indent):
    print(" " * indent + msg, file=sys.stderr)


def get_btype(build):
    extra = build.get('extra')
    if extra:
        typeinfo = extra.get('typeinfo')
    else:
        typeinfo = None

    if extra and extra.get('image'):
        return 'image'
    elif typeinfo and typeinfo.get('module'):
        return 'module'
    else:
        return 'rpm'


class Downloader:
    def __init__(self, output, base):
        self.output = output
        self.base = base
        self.base_update_info = {}
        self.update_info = {}
        self.build_id_to_nvr = {}
        self.module_nvr_short_to_nvr = {}
        self.image_packages = set()

        if self.base:
            self._load_build_ids(self.base)
            self._load_update_info(self.base)

        koji_config_file = os.path.join(os.path.dirname(__file__), '../koji.conf')
        options = koji.read_config(profile_name='fedora', user_config=koji_config_file)
        session_opts = koji.grab_session_options(options)
        self.koji_session = koji.ClientSession(options['server'], session_opts)

    def _check_existing(self, relative, *, indent=0):
        dest = os.path.join(self.output, relative)
        if os.path.exists(dest):
            return True, dest
        elif self.base and os.path.exists(os.path.join(self.base, relative)):
            d = os.path.dirname(dest)
            if not os.path.exists(d):
                os.makedirs(d)
            shutil.copy(os.path.join(self.base, relative), dest)
            return True, dest
        else:
            return False, dest

    def _load_build_ids(self, load_from):
        for ent in os.scandir(os.path.join(load_from, 'builds')):
            if ent.name.endswith('.json.gz'):
                with gzip.open(ent.path, 'rt') as f:
                    build = json.load(f)
                self.build_id_to_nvr[build['build_id']] = build['nvr']
                if get_btype(build) == 'module':
                    n, v, r = build['nvr'].rsplit('-', 2)
                    nvr_short = n + '-' + v + '-' + r.split('.')[0]
                    self.module_nvr_short_to_nvr[nvr_short] = build['nvr']

    def _load_update_info(self, load_from):
        fname = os.path.join(load_from, 'updates.index.gz')
        if os.path.exists(fname):
            with gzip.open(fname, 'rt') as f:
                self.base_update_info = json.load(f)

    def save_update_info(self):
        fname = os.path.join(self.output, 'updates.index.gz')
        with gzip.open(fname, 'wt') as f:
            json.dump(self.update_info, f, indent=4)

    def create_directories(self):
        if os.path.exists(self.output):
            print(f"{self.output} already exists", file=sys.stderr)
        os.mkdir(self.output)
        os.mkdir(os.path.join(self.output, 'updates'))
        os.mkdir(os.path.join(self.output, 'builds'))
        os.mkdir(os.path.join(self.output, 'git'))

    def download_build(self, *, nvr=None, build_id=None, indent=0):
        if nvr is None and build_id is None:
            raise RuntimeError("nvr or build_id must be specified")

        if nvr is None:
            nvr = self.build_id_to_nvr.get(build_id)

        build = None
        if nvr:
            exists, output_file = self._check_existing(f'builds/{nvr}.json.gz')
            if exists:
                show(f"{nvr}: already downloaded", indent)
                with gzip.open(output_file, 'rt') as f:
                    build = json.load(f)
        else:
            exists = False

        if build is None:
            if nvr:
                build = self.koji_session.getBuild(nvr)
            else:
                build = self.koji_session.getBuild(build_id)

            nvr = build['nvr']
            show(f"{nvr}: downloaded", indent)

        output_file = os.path.join(self.output, f'builds/{nvr}.json.gz')
        self.build_id_to_nvr[build['build_id']] = build['nvr']

        indent += 4
        btype = get_btype(build)

        if not exists:
            if btype == 'image' or btype == 'module':
                archives = self.koji_session.listArchives(build['id'])
                build['archives'] = []
                show("Listing archives", indent)
                seen = set()
                for archive in archives:
                    if btype == 'module':
                        if archive['filename'] not in ('modulemd.txt', 'modulemd.x86_64.txt'):
                            continue
                    build['archives'].append(archive)
                    show(f"Listing rpms for archive {archive['id']}", indent)
                    components = self.koji_session.listRPMs(imageID=archive['id'])
                    archive['components'] = []
                    for c in components:
                        if c['arch'] not in ('x86_64', 'noarch'):
                            continue
                        archive['components'].append(c)

            with gzip.open(output_file, 'wt') as f:
                json.dump(build, f, indent=4)

        # Now find extra builds to download

        if btype == 'image' or btype == 'module':
            seen = set()
            for archive in build['archives']:
                for c in archive['components']:
                    if not c['build_id'] in seen:
                        seen.add(c['build_id'])
                        nvr = self.download_build(build_id=c['build_id'], indent=indent)
                        if btype == 'image':
                            self.image_packages.add(nvr.rsplit('-', 2)[0])

        if btype == 'image':
            for m in build['extra']['image']['modules']:
                if m in self.module_nvr_short_to_nvr:
                    self.download_build(nvr=self.module_nvr_short_to_nvr[m], indent=indent)
                else:
                    module_name = m.rsplit('-', 2)[0]
                    package_id = self.koji_session.getPackageID(module_name)
                    for module_build in self.koji_session.listBuilds(type='module',
                                                                     packageID=package_id):
                        if module_build['nvr'].startswith(m):
                            self.download_build(nvr=module_build['nvr'], indent=indent + 4)

        return nvr

    def download_tag_data(self, indent=0):
        self.tagged_packages = {}
        for tag in ['f36', 'f35']:
            exists, output_file = self._check_existing(f'tags/{tag}.json.gz')
            if not exists:
                show(f'Downloading tag history for {tag}', indent=indent)
                result = self.koji_session.queryHistory(tables=['tag_listing'], tag=tag)
                filtered_result = [r
                                   for r in result['tag_listing']
                                   if r['name'] in self.image_packages]
                d = os.path.dirname(output_file)
                if not os.path.exists(d):
                    os.makedirs(d)
                with gzip.open(output_file, 'wt') as f:
                    json.dump(filtered_result, f, indent=4)
            else:
                show(f'Using existing tag history for {tag}', indent=indent)
                with gzip.open(output_file, 'rt') as f:
                    filtered_result = json.load(f)

            indent += 4
            for r in filtered_result:
                self.download_build(nvr=f"{r['name']}-{r['version']}-{r['release']}",
                                    indent=indent + 4)

    def download_package_details(self, *, indent=0):
        for package in sorted(self.image_packages):
            self.download_updates('rpm', package, indent=indent)
            self.dump_git(os.path.join('rpms', package), indent=indent + 4)

    def download_updates(self, content_type, package, *, releases=None, date=DATE, indent=0):
        key = f"{content_type}/{package}"

        if (key in self.update_info or
            (key in self.base_update_info and
             self.base_update_info[key]['date'] == date)):

            show(f"{key}: already downloaded updates", indent)
            if key not in self.update_info:
                self.update_info[key] = self.base_update_info[key]
                for update in self.update_info[key]['updates']:
                    src = os.path.join(self.base, 'updates', update + '.json.gz')
                    dest = os.path.join(self.output, 'updates', update + '.json.gz')

                    shutil.copy(src, dest)

                    with gzip.open(src, 'rt') as f:
                        r = json.load(f)
                        for b in r['builds']:
                            build_name = b['nvr'].rsplit('-', 2)[0]
                            if build_name == package:
                                self.download_build(nvr=b['nvr'], indent=indent + 4)

            return

        show(f"{key}: downloading updates", indent)

        if releases is None:
            if content_type == 'flatpak':
                releases = ['F36F', 'F35F']
            elif content_type == 'rpm':
                releases = ['F36', 'F35']

        url = "https://bodhi.fedoraproject.org/updates/"
        params = {
            'page': 1,
            'rows_per_page': 100,
            'content_type': content_type,
            'packages': package,
            'releases': releases,
            'submitted_before': date,
        }

        response = requests.get(url,
                                headers={'Accept': 'application/json'},
                                params=params)
        response.raise_for_status()
        response_json = response.json()

        self.update_info[key] = {
            'date': date,
            'updates': []
        }

        for r in response_json['updates']:
            if ' ' in r['title']:
                update_name = r['updateid']
            else:
                update_name = r['title']
            output_file = os.path.join(self.output, 'updates', update_name + '.json.gz')
            with gzip.open(output_file, 'wt') as f:
                json.dump(r, f, indent=4)

            self.update_info[key]['updates'].append(update_name)

            if r['status'] in ('pending', 'testing'):
                print("Error: {update_name} status is {r['status']}", file=sys.stderr)
                sys.exit(1)

            for b in r['builds']:
                build_name = b['nvr'].rsplit('-', 2)[0]
                if build_name == package:
                    self.download_build(nvr=b['nvr'], indent=indent + 4)

    def do_dump_git(self, tempdir, output_file, pkg):
        subprocess.check_call(['git', 'clone', '--mirror',
                               'https://src.fedoraproject.org/' + pkg + '.git'],
                              cwd=tempdir)
        repodir = os.path.join(tempdir, os.path.basename(pkg) + '.git')
        result = {}
        branches = subprocess.check_output(['git', 'branch', '-a', '--format=%(refname:lstrip=2)'],
                                           cwd=repodir, encoding='UTF-8').strip().split('\n')
        for branch in branches:
            commits = subprocess.check_output(['git', 'log', '--format=%H', branch],
                                              cwd=repodir, encoding='UTF-8').strip().split('\n')
            result[branch] = commits
        d = os.path.dirname(output_file)
        if not os.path.exists(d):
            os.makedirs(d)
        with gzip.open(output_file, 'wt') as f:
            json.dump(result, f, indent=4)

    def dump_git(self, pkg, *, indent=0):
        exists, output_file = self._check_existing(f'git/{pkg}.json.gz')
        if exists:
            show(f"{pkg}.git: already downloaded", indent)
            return

        show(f"{pkg}.git: downloading", indent)

        tempdir = tempfile.mkdtemp()
        try:
            self.do_dump_git(tempdir, output_file, pkg)
        finally:
            shutil.rmtree(tempdir)


@click.command()
@click.option('-o', '--output', required=True,
              help='Output directory')
@click.option('-b', '--base',
              help='Reference directory')
def main(output, base):
    """Download test data"""
    downloader = Downloader(output, base)

    downloader.create_directories()

    downloader.download_updates('flatpak', 'baobab')
    downloader.download_updates('flatpak', 'eog')
    downloader.download_updates('flatpak', 'feedreader')
    downloader.download_updates('flatpak', 'quadrapassel')

    # These rpm builds are used when testing modification of Bodhi updates
    for b in (["gnome-weather-41.0-1.fc35",
               "gnome-weather-42~beta-1.fc36",
               "gnome-weather-42~rc-1.fc36",
               "gnome-weather-42.0-1.fc36",
               "sushi-41.0-1.fc35"]):
        downloader.download_build(nvr=b)

    # Module with multiple contexts
    downloader.download_build(nvr='django-1.6-20180828135711.9c690d0e')
    downloader.download_build(nvr='django-1.6-20180828135711.a5b0195c')

    downloader.download_tag_data()

    downloader.download_package_details()

    downloader.save_update_info()


if __name__ == '__main__':
    main()

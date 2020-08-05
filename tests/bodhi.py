import gzip
import json
import os
import re
from urllib.parse import parse_qs, urlparse

from iso8601 import iso8601
import responses

from flatpak_indexer.datasource.fedora.bodhi_query import parse_date_value

_updates = []


def load_updates():
    if len(_updates) == 0:
        data_dir = os.path.join(os.path.dirname(__file__), '../test-data/updates')
        for child in os.listdir(data_dir):
            if not child.endswith('.json.gz'):
                continue
            with gzip.open(os.path.join(data_dir, child), 'rt') as f:
                _updates.append(json.load(f))

    return _updates


def _parse_date_param(params, name):
    values = params.get(name)
    if not values:
        return None
    return iso8601.parse_date(values[0])


def _check_date(update, name, since):
    if since is None:
        return True

    value = update.get(name)
    if value is None:
        return False
    if parse_date_value(value) < since:
        return False

    return True


def get_updates_callback(request, modify=None):
    params = parse_qs(urlparse(request.url).query)

    page = int(params['page'][0])
    rows_per_page = int(params['rows_per_page'][0])
    content_type = params.get('content_type', (None,))[0]
    packages = params.get('packages')

    submitted_since = _parse_date_param(params, 'submitted_since')
    modified_since = _parse_date_param(params, 'modified_since')
    pushed_since = _parse_date_param(params, 'pushed_since')

    updates = load_updates()

    matched_updates = []
    for update in updates:
        if modify:
            update = modify(update)

        if content_type is not None and update['content_type'] != content_type:
            continue
        if packages is not None:
            found = False
            for b in update['builds']:
                n, v, r = b['nvr'].rsplit('-', 2)
                if n in packages:
                    found = True
            if not found:
                continue

        if not _check_date(update, 'date_submitted', submitted_since):
            continue
        if not _check_date(update, 'date_modified', modified_since):
            continue
        if not _check_date(update, 'date_pushed', pushed_since):
            continue

        matched_updates.append(update)

    # Sort in descending order by date_submitted
    matched_updates.sort(key=lambda x: parse_date_value(update['date_submitted']),
                         reverse=True)

    pages = (len(matched_updates) + rows_per_page - 1) // rows_per_page
    paged_updates = matched_updates[(page - 1) * rows_per_page:page * rows_per_page]

    return (200, {}, json.dumps({
        'page': page,
        'pages': pages,
        'rows_per_page': rows_per_page,
        'total': len(matched_updates),
        'updates': paged_updates
    }))


def get_update_callback(request, modify=None):
    path = urlparse(request.url).path
    update_id = path.split('/')[-1]

    updates = load_updates()
    for update in updates:
        if update['updateid'] == update_id:
            return (200, {}, json.dumps({
                'update': update,
                'can_edit': False,
            }))

    return (404, {}, json.dumps({
        "status": "error",
        "errors": [
            {
                "location": "url",
                "name": "id",
                "description": "Invalid update id"
            }
        ]}))


def mock_bodhi(modify=None):
    def get_updates_callback_(request):
        return get_updates_callback(request, modify=modify)

    def get_update_callback_(request):
        return get_update_callback(request, modify=modify)

    responses.add_callback(method=responses.GET,
                           url='https://bodhi.fedoraproject.org/updates/',
                           callback=get_updates_callback_,
                           content_type='application/json',
                           match_querystring=False)
    responses.add_callback(method=responses.GET,
                           url=re.compile(
                               'https://bodhi.fedoraproject.org/updates/([a-zA-Z0-9-]+)'),
                           callback=get_update_callback_,
                           content_type='application/json',
                           match_querystring=False)

from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any, Dict, List, Union
import logging
import re

from .http_utils import HttpConfig
from .models import BodhiUpdateModel
from .nvr import NVR
from .release_info import Release, ReleaseStatus
from .session import Session
from .utils import format_date, parse_date

logger = logging.getLogger(__name__)


# This is the maximum amount of time we'll ask Bodhi for all new updates
# of a given type; if we haven't updated our image information for longer
# than this, then we request package by package
#
# This is not typically hit - we'll refresh everything if our fedora-messaging
# queue has been garbage collected.
ALL_UPDATES_MAX_INTERVAL = timedelta(days=1)


# When querying Koji for events that happened since we last queried, we
# allow a timestamp offset of this much
TIMESTAMP_FUZZ = timedelta(minutes=1)


def parse_date_value(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _update_update_from_response(pipe, update_json, old_update: BodhiUpdateModel):
    content_type = update_json["content_type"]

    if old_update:
        old_entities = {nvr.name for nvr in old_update.builds}
    else:
        old_entities = set()

    def parse_date(date):
        if date:
            return parse_date_value(date)

    update = BodhiUpdateModel(
        update_id=update_json["updateid"],
        release_name=update_json["release"]["name"],
        release_branch=update_json["release"]["branch"],
        date_submitted=parse_date(update_json["date_submitted"]),
        date_testing=parse_date(update_json["date_testing"]),
        date_stable=parse_date(update_json["date_stable"]),
        user_name=update_json["user"]["name"],
        status=update_json["status"],
        type=update_json["type"],
    )

    for build_json in update_json["builds"]:
        update.builds.append(NVR(build_json["nvr"]))

    new_entities = {nvr.name for nvr in update.builds}

    def _entity_key(entity_name):
        return entity_name + ":" + update.release_branch + ":" + update.update_id

    pipe.set("update:" + update.update_id, update.to_json_text())
    to_remove = [_entity_key(e) for e in old_entities - new_entities]
    if to_remove:
        pipe.zrem("updates-by-entity:" + content_type, *to_remove)
    to_add = {_entity_key(e): 0 for e in new_entities - old_entities}
    if to_add:
        pipe.zadd("updates-by-entity:" + content_type, to_add)


def _load_old_updates(pipe, results):
    old_updates = {}
    if results:
        old_updates_raw = pipe.mget(*("update:" + r["updateid"] for r in results))
        for result, old_update_raw in zip(results, old_updates_raw):
            if old_update_raw:
                old_updates[result["updateid"]] = BodhiUpdateModel.from_json_text(old_update_raw)

    return old_updates


def _update_updates_from_response(pipe, results, old_updates):
    for result in results:
        _update_update_from_response(pipe, result, old_updates.get(result["updateid"]))


def _run_query(requests_session, content_type, url, params, save_entities, results):
    # Depending on our query parameters, we might get duplicates in the response, and might
    # get less than rows_per_page rows in the response
    # (https://github.com/fedora-infra/bodhi/issues/4130),
    # so we need to track what updates we actually get to compare to 'total' in the
    # response, which is de-duplicated
    seen_updates = set()
    page = 1
    while True:
        params["page"] = page
        logger.info("Querying Bodhi with params: %s", params)

        response = requests_session.get(url, headers={"Accept": "application/json"}, params=params)
        response.raise_for_status()
        response_json = response.json()

        for update_json in response_json["updates"]:
            update_id = update_json["updateid"]
            if update_id in seen_updates:
                continue

            seen_updates.add(update_id)

            found_build = False
            for build_json in update_json["builds"]:
                package_name = build_json["nvr"].rsplit("-", 2)[0]
                if build_json["type"] == content_type and (
                    save_entities is None or package_name in save_entities
                ):
                    found_build = True
            if not found_build:
                continue

            results.append(update_json)

        # The first check avoids an extra round trip in the normal case, the second check
        # avoids paging forever if something goes wrong
        if len(seen_updates) >= response_json["total"] or len(response_json["updates"]) == 0:
            break
        else:
            page += 1


def _query_updates(
    session: Session,
    requests_session,
    content_type,
    results,
    query_entities=None,
    save_entities=None,
    after=None,
    rows_per_page=100,
):
    url = "https://bodhi.fedoraproject.org/updates/"
    params: Dict[str, Union[int, str, List[str]]] = {
        "rows_per_page": rows_per_page,
    }

    bodhi_releases: List[str] = []
    for release in session.fedora_releases:
        if release.status == ReleaseStatus.EOL or release.status == ReleaseStatus.RAWHIDE:
            continue

        bodhi_release = release.name
        if content_type == "flatpak":
            bodhi_release += "F"
        bodhi_releases.append(bodhi_release)
    params["releases"] = bodhi_releases

    # Setting the content type in the query:
    #
    # a) messes up the pagination in the query
    # (https://github.com/fedora-infra/bodhi/issues/4130)
    #
    # b) can make things much slower
    # (https://github.com/fedora-infra/bodhi/issues/3064)
    #
    # params['content_type'] = content_type
    #
    # For Fedora, because each content type has a separate release, we effectively
    # filter by content type anyways.

    if query_entities is not None:
        if len(query_entities) > 5:
            for i in range(0, len(query_entities), 5):
                _query_updates(
                    session,
                    requests_session,
                    content_type,
                    results,
                    query_entities=query_entities[i : i + 5],
                    save_entities=save_entities,
                    after=after,
                    rows_per_page=rows_per_page,
                )
            return
        else:
            params["packages"] = query_entities

    if after is not None:
        for key in ["submitted_since", "modified_since"]:
            params_copy = dict(params)
            params_copy[key] = (after - TIMESTAMP_FUZZ).isoformat()

            _run_query(requests_session, content_type, url, params_copy, save_entities, results)
    else:
        _run_query(requests_session, content_type, url, params, save_entities, results)


def _refresh_updates(session: Session, content_type, entities, pipe, rows_per_page=None):
    pipe.watch("updates-by-entity:" + content_type)

    assert isinstance(session.config, HttpConfig)
    requests_session = session.config.get_requests_session()

    to_query = set(entities)
    to_refresh = set()

    current_ts = datetime.utcnow().replace(tzinfo=timezone.utc)

    queried_ts = pipe.hmget("update-cache:" + content_type, *entities)
    parsed_ts = [parse_date(ts.decode("utf-8")) if ts else None for ts in queried_ts]

    results: List[Dict[str, Any]] = []

    refresh_ts = max((ts for ts in parsed_ts if ts is not None), default=None)
    if refresh_ts is not None:
        if current_ts - refresh_ts < ALL_UPDATES_MAX_INTERVAL:
            for entity_name, ts in zip(entities, parsed_ts):
                if entity_name in to_query:
                    if ts == refresh_ts:
                        to_refresh.add(entity_name)
                        to_query.remove(entity_name)

        if len(to_refresh) > 0:
            _query_updates(
                session,
                requests_session,
                content_type,
                results,
                save_entities=to_refresh,
                after=refresh_ts - TIMESTAMP_FUZZ,
                rows_per_page=rows_per_page,
            )

    if len(to_query) > 0:
        _query_updates(
            session,
            requests_session,
            content_type,
            results,
            query_entities=sorted(to_query),
            save_entities=to_query,
            rows_per_page=rows_per_page,
        )

    old_updates = _load_old_updates(pipe, results)

    pipe.multi()

    _update_updates_from_response(pipe, results, old_updates)

    formatted_current_ts = format_date(current_ts)
    pipe.hset("update-cache:" + content_type, mapping={e: formatted_current_ts for e in entities})

    pipe.execute()


def refresh_updates(session: Session, content_type, entities, rows_per_page=10):
    session.redis_client.transaction(
        partial(_refresh_updates, session, content_type, entities, rows_per_page=rows_per_page)
    )


def _refresh_all_updates(session: Session, content_type, pipe, rows_per_page=10):
    pipe.watch("updates-by-entity:" + content_type)

    assert isinstance(session.config, HttpConfig)
    requests_session = session.config.get_requests_session()

    current_ts = datetime.utcnow()

    cache_ts = pipe.hget("update-cache:" + content_type, "@ALL@")
    if cache_ts:
        after = parse_date(cache_ts.decode("utf-8")) - TIMESTAMP_FUZZ
    else:
        after = None

    results: List[Dict[str, Any]] = []
    _query_updates(
        session, requests_session, content_type, results, after=after, rows_per_page=rows_per_page
    )

    old_updates = _load_old_updates(pipe, results)

    pipe.multi()
    _update_updates_from_response(pipe, results, old_updates)
    pipe.hset("update-cache:" + content_type, "@ALL@", format_date(current_ts))
    pipe.execute()


def refresh_all_updates(session, content_type, rows_per_page=10):
    session.redis_client.transaction(
        partial(_refresh_all_updates, session, content_type, rows_per_page=rows_per_page)
    )


def _refresh_update(update_json, pipe):
    pipe.watch("updates-by-entity:" + update_json["content_type"])

    results = [update_json]
    old_updates = _load_old_updates(pipe, results)

    pipe.multi()
    _update_updates_from_response(pipe, results, old_updates)
    pipe.execute()


def refresh_update_status(session, update_id):
    """Refreshes the status of a single update"""
    url = f"https://bodhi.fedoraproject.org/updates/{update_id}"

    assert isinstance(session.config, HttpConfig)
    requests_session = session.config.get_requests_session()

    if session.redis_client.get("update:" + update_id) is None:
        logger.info("Update %s not found, no need to update status", update_id)
        return

    logger.info("Querying bodhi for the new status of: %s", update_id)
    response = requests_session.get(url, headers={"Accept": "application/json"})
    response.raise_for_status()

    # Could optimize to avoid updating the updates-by-entity index
    session.redis_client.transaction(partial(_refresh_update, response.json()["update"]))


def reset_update_cache(session):
    session.redis_client.delete("update-cache:flatpak")
    session.redis_client.delete("update-cache:rpm")


def list_updates(session, content_type, entity_name=None, release_branch=None):
    """Returns a list of (PackageUpdateBuild, PackageBuild)"""

    if release_branch is not None:
        branches = [release_branch]
    else:
        branches = [
            r.branch
            for r in session.fedora_releases
            if r.status != ReleaseStatus.EOL and r.status != ReleaseStatus.RAWHIDE
        ]

    if entity_name is not None:
        key = entity_name

        first_key = key + ":" + branches[0]
        last_key = key + ":" + branches[-1]

        updates_by_entity = session.redis_client.zrangebylex(
            "updates-by-entity:" + content_type, "[" + first_key + ":", "(" + last_key + ";"
        )
    else:
        updates_by_entity = session.redis_client.zrange("updates-by-entity:" + content_type, 0, -1)

    def filter_results(keys):
        for key in keys:
            entity_name, rb, update_id = key.decode("utf-8").split(":")
            if release_branch is None or rb in branches:
                yield "update:" + update_id

    # Remove duplicates
    to_fetch = sorted(set(filter_results(updates_by_entity)))

    return [BodhiUpdateModel.from_json_text(x) for x in session.redis_client.mget(to_fetch)]


def query_releases(session: Session):
    """
    Query current Fedora releases from bodhi.

    Use sesson.fedora_releases instead to get caching and consistency
    for the duration of the session.
    """
    requests_session = session.config.get_requests_session()

    url = "https://bodhi.fedoraproject.org/releases/"
    params: Dict[str, Union[int, str, List[str]]] = {
        "rows_per_page": 100,
        "exclude_archived": 1,
    }

    result: List[Release] = []

    page = 1
    while True:
        params["page"] = page
        logger.info("Querying Bodhi with params: %s", params)
        response = requests_session.get(url, headers={"Accept": "application/json"}, params=params)
        response.raise_for_status()
        response_json = response.json()

        for release_json in response_json["releases"]:
            name = release_json["name"]
            if not re.match(r"F\d+$", name):
                continue

            branch = release_json["branch"]
            state = release_json["state"]

            if state == "disabled":
                # Just treat this the same as absent
                continue
            elif state == "pending":
                if branch == "rawhide":
                    status = ReleaseStatus.RAWHIDE
                else:
                    status = ReleaseStatus.BRANCHED
            elif state == "frozen":
                status = ReleaseStatus.BRANCHED
            elif state == "current":
                status = ReleaseStatus.GA
            elif state == "archived":
                status = ReleaseStatus.EOL
            else:
                logger.warn("Unknown state for release %s: %s", name, state)
                continue

            result.append(Release(name=name, branch=branch, tag=name.lower(), status=status))

        if page == int(response_json["pages"]):
            break

    result.sort(key=lambda release: release.name)

    return result

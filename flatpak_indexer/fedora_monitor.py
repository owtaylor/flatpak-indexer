from functools import partial
import json
import logging
import os
import ssl
import threading
import time
from typing import cast, Optional, Set, Tuple
import uuid

import pika
import pika.credentials
import pika.exceptions
import redis


from .redis_utils import get_redis_client, RedisConfig


logger = logging.getLogger(__name__)


class ChannelCancelled(Exception):
    pass


KEY_SERIAL = "changelog:serial"
KEY_UPDATE_CHANGELOG = "changelog:updates"
KEY_DISTGIT_CHANGELOG = "changelog:distgit"


class FedoraMonitor:
    """
    Monitor for changes to Fedora infrastructure objects

    The FedoraMonitor class is used to track when Bodhi updates or distgit repositories
    change by listing to Fedora Messaging messages. The basic way it works is
    that there's a worker thread that receives the messages, and enters them
    into a "changelog" stores in Redis. The get_*_changed() and clear_*_changed()
    methods are then used to retrieve and (after processing) retire the
    changelog entries.
    """
    INITIAL_RECONNECT_TIMEOUT = 1
    MAX_RECONNECT_TIMEOUT = 10 * 60
    RECONNECT_TIMEOUT_MULTIPLIER = 5

    def __init__(self, config: RedisConfig, watch_bodhi_updates=False, watch_distgit_changes=False):
        self.config = config
        self.watch_bodhi_updates = watch_bodhi_updates
        self.watch_distgit_changes = watch_distgit_changes

        # This is the redis client for the main thread - the worker thread
        # needs to use self.thread_redis_client
        self.redis_client = get_redis_client(config)

        self.thread = threading.Thread(name="FedoraMonitor", target=self._run)
        self.started = threading.Event()

        self.lock = threading.Lock()
        self.failure = None
        self.stopping = False
        self.connection = None

        self.reconnect_timeout = self.INITIAL_RECONNECT_TIMEOUT

    def start(self):
        """
        Start the worker thread and consume queued messages

        This starts the worker thread and waits for it to connect to Fedora Messaging
        and consume immediately available change messages before returning. The
        idea of this is to avoid indexing once with stale data, then only indexing
        with correct data on the second run.
        """
        self.thread.start()
        self.started.wait()

        self._maybe_reraise_failure("Failed to start connection to fedora-messaging")

    def stop(self):
        """
        Stop the worker thread

        Signals the worker thread to exit and waits for it. There's a lot of complexity
        to make this bulletproof, though it's basically just useful for tests.
        """
        # Check first if thread is already in a failed state
        self._maybe_reraise_failure("Error communicating with fedora-messaging")

        with self.lock:
            self.stopping = True
            connection = self.connection

        if connection:
            def do_stop():
                connection.close()

            connection.add_callback_threadsafe(do_stop)

        self.thread.join()

        self._maybe_reraise_failure("Failed to stop connection to fedora-messaging")

    def _get_changed(self, changelog_key) -> Tuple[Optional[Set[str]], int]:
        self._maybe_reraise_failure("Error communicating with fedora-messaging")

        result: Optional[Set[str]] = set()
        entries = self.redis_client.zrangebyscore(
            changelog_key, 0, float("inf"), withscores=True, score_cast_func=int
        )
        for key, _ in entries:
            if key == b"":
                result = None
            elif result is not None:
                result.add(key.decode("utf-8"))

        if len(entries) > 0:
            serial = entries[-1][1]
        else:
            serial = 0

        return result, serial

    def _clear_changed(self, changelog_key, serial):
        self.redis_client.zremrangebyscore(changelog_key, 0, serial)

    def get_bodhi_changed(self) -> Tuple[Optional[Set[str]], int]:
        """
        Return Bodhi updates that have changed.

        This returns a set of Bodhi updates known to have changed since the last time
        that get_changed() was called (in this process or in a previous run
        of flatpak-indexer). Once cached information has been updated, clear_changed()
        should be called to remove these entries from the log we store.

        Return value:
        A tuple: the first value either either a set of changed Bodhi IDs,
        or None. None means that we don't have reliable change information and
        all updates must be refetched. The second value is a serial to pass to
        clear_changed().
        """
        assert self.watch_bodhi_updates

        return self._get_changed(KEY_UPDATE_CHANGELOG)

    def clear_bodhi_changed(self, serial):
        """Remove old changelog entries after they have been processed"""
        assert self.watch_bodhi_updates

        self._clear_changed(KEY_UPDATE_CHANGELOG, serial)

    def get_distgit_changed(self) -> Tuple[Optional[Set[str]], int]:
        """
        Return distgit repositories that have changed.

        This returns a set of distgit repositories known to have changed since the last time
        that get_changed() was called (in this process or in a previous run
        of flatpak-indexer). Once cached information has been updated, clear_changed()
        should be called to remove these entries from the log we store.

        Return value:
        A tuple: the first value either either a set of changed repository paths,
        or None. None means that we don't have reliable change information and
        all repositories must be pulled. The second value is a serial to pass to
        clear_changed().
        """
        assert self.watch_distgit_changes

        return self._get_changed(KEY_DISTGIT_CHANGELOG)

    def clear_distgit_changed(self, serial):
        """Remove old changelog entries after they have been processed"""
        assert self.watch_distgit_changes

        self._clear_changed(KEY_DISTGIT_CHANGELOG, serial)

    def _maybe_reraise_failure(self, msg):
        with self.lock:
            if self.failure:
                raise RuntimeError(msg) from self.failure

    def _do_add_to_log(
        self, new_queue_name, update_id, distgit_path, pipe: "redis.client.Pipeline[bytes]"
    ):
        pipe.watch(KEY_SERIAL)
        if self.watch_bodhi_updates and (new_queue_name or update_id):
            pipe.watch(KEY_UPDATE_CHANGELOG)
        if self.watch_distgit_changes and (new_queue_name or distgit_path):
            pipe.watch(KEY_DISTGIT_CHANGELOG)

        pre = cast("redis.Redis[bytes]", pipe)
        serial = 1 + int(pre.get("updatequeue:serial") or 0)

        pipe.multi()
        pipe.set(KEY_SERIAL, serial)
        if new_queue_name:
            pipe.set("fedora-messaging-queue", new_queue_name)
            if self.watch_bodhi_updates:
                pipe.delete(KEY_UPDATE_CHANGELOG)
                pipe.zadd(KEY_UPDATE_CHANGELOG, {b'': serial})
            if self.watch_distgit_changes:
                pipe.delete(KEY_DISTGIT_CHANGELOG)
                pipe.zadd(KEY_DISTGIT_CHANGELOG, {b'': serial})
        elif update_id:
            pipe.zadd(KEY_UPDATE_CHANGELOG, {update_id: serial})
        elif distgit_path:
            pipe.zadd(KEY_DISTGIT_CHANGELOG, {distgit_path: serial})

    def _add_to_update_log(self, update_id):
        self.thread_redis_client.transaction(partial(self._do_add_to_log, None, update_id, None))

    def _add_to_distgit_log(self, distgit_path):
        self.thread_redis_client.transaction(partial(self._do_add_to_log, None, None, distgit_path))

    def _reset_changelog(self, queue_name):
        self.thread_redis_client.transaction(partial(self._do_add_to_log, queue_name, None, None))

    def _update_from_message(self, routing_key, body_json):
        body = json.loads(body_json)

        if routing_key == 'org.fedoraproject.prod.git.receive':
            path = body['commit']['namespace'] + '/' + body['commit']['repo']
            logger.info("Saw commit on %s", path)
            self._add_to_distgit_log(path)
        else:
            update_id = body['update']['alias']
            logger.info("Saw change to Bodhi Update %s", update_id)
            self._add_to_update_log(update_id)

    def _wait_for_messages(self):
        with self.lock:
            if self.stopping:
                return

        cert_dir = os.path.join(os.path.dirname(__file__), 'messaging-certs')

        ssl_context = ssl.create_default_context(cafile=os.path.join(cert_dir, "cacert.pem"))
        ssl_context.load_cert_chain(os.path.join(cert_dir, "fedora-cert.pem"),
                                    os.path.join(cert_dir, "fedora-key.pem"))
        ssl_options = pika.SSLOptions(ssl_context, "rabbitmq.fedoraproject.org")

        credentials = pika.credentials.ExternalCredentials()

        conn_params = pika.ConnectionParameters(host="rabbitmq.fedoraproject.org",
                                                credentials=credentials,
                                                ssl_options=ssl_options,
                                                virtual_host="/public_pubsub")

        connection = pika.BlockingConnection(conn_params)
        channel = connection.channel()

        queue_name_raw = self.thread_redis_client.get('fedora-messaging-queue')
        queue_name = queue_name_raw.decode('utf-8') if queue_name_raw else None

        if queue_name:
            try:
                channel.queue_declare(queue_name,
                                      passive=True, durable=True, exclusive=False,
                                      auto_delete=False)
            except pika.exceptions.ChannelClosedByBroker as e:
                if e.reply_code == 404:
                    queue_name = None
                    # The exception closed the channel
                    channel = connection.channel()
                else:
                    raise

        if not queue_name:
            queue_name = str(uuid.uuid4())
            channel.queue_declare(queue_name,
                                  passive=False, durable=True, exclusive=False,
                                  auto_delete=False)

            self._reset_changelog(queue_name)

        logger.info(f"Connected to fedora-messaging, queue={queue_name}")

        if self.watch_bodhi_updates:
            channel.queue_bind(queue_name, 'amq.topic',
                               routing_key="org.fedoraproject.prod.bodhi.update.request.#")
            channel.queue_bind(queue_name, 'amq.topic',
                               routing_key="org.fedoraproject.prod.bodhi.update.complete.#")

        if self.watch_distgit_changes:
            channel.queue_bind(queue_name, 'amq.topic',
                               routing_key="org.fedoraproject.prod.git.receive")

        # We first consume messages with a timeout of zero until we block to clean
        # out anything queued
        for method, properties, body_json in channel.consume(queue_name,
                                                             inactivity_timeout=0):
            if method is None:
                break
            else:
                self._update_from_message(method.routing_key, body_json)
                channel.basic_ack(method.delivery_tag)
        else:
            # If the iterator exits, that means the channel has been cancelled
            connection.close()
            raise ChannelCancelled()

        with self.lock:
            if self.stopping:
                # stop() called during reconnection
                return
            self.connection = connection

        self.reconnect_timeout = self.INITIAL_RECONNECT_TIMEOUT
        self.started.set()

        # Then we use a timeout of None (never), until the channel is closed
        for method, properties, body_json in channel.consume(queue_name,
                                                             inactivity_timeout=None):
            assert method is not None  # only should occur on timeout
            self._update_from_message(method.routing_key, body_json)
            channel.basic_ack(method.delivery_tag)
        else:
            with self.lock:
                if self.stopping:
                    # If we called connection.close(), we're done
                    return
                else:
                    # Otherwise, channel was cancelled, trigger a reconnection
                    connection.close()
                    raise ChannelCancelled()

    def _run(self):
        self.thread_redis_client = get_redis_client(self.config)

        while True:
            try:
                self._wait_for_messages()
                return
            except ChannelCancelled:
                logger.warning("fedora-messaging channel was cancelled")
            except (pika.exceptions.AMQPConnectionError, ssl.SSLError) as e:
                # This includes stream-lost and connection-refused, which
                # we might get from a broker restart, but also authentication
                # failures, protocol failures, etc. Trying to parse out
                # the exact case would be a future-compat headache.
                logger.warning("fedora-messaging connection failure (%r)",
                               e, exc_info=e)
            except Exception as e:
                # The main loop might be in a timeout waiting for the next time
                # to poll - we don't have an easy way to interrupt that, so just
                # store the exception away until get_changed() or stop() is called.
                with self.lock:
                    self.failure = e
                if self.started.is_set():
                    # In the case where we're storing the error away, log the error
                    # immediately
                    logger.error("Error communicating with fedora-messaging", exc_info=e)
                self.started.set()
                return

            logger.warning("sleeping for %ss before retrying to connect", self.reconnect_timeout)
            time.sleep(self.reconnect_timeout)
            self.reconnect_timeout = min(
                self.reconnect_timeout * self.RECONNECT_TIMEOUT_MULTIPLIER,
                self.MAX_RECONNECT_TIMEOUT
            )

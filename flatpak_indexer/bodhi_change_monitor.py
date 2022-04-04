from functools import partial
import json
import logging
import os
import ssl
import time
import threading
from typing import Optional, Set, Tuple, cast
import uuid

import pika
import pika.credentials
import pika.exceptions
import redis


from .config import Config
from .redis_utils import get_redis_client


logger = logging.getLogger(__name__)


class ChannelCancelled(Exception):
    pass


class BodhiChangeMonitor:
    """
    Monitor for changes to Bodhi updates

    The BodhiChangeMonitor class is used to track when the status of Bodhi updates
    changes by listing to Fedora Messaging messages. The basic way it works is
    that there's a worker thread that receives the messages, and enters them
    into a "changelog" stores in Redis. The get_changed() and clear_changed()
    methods are then used to retrieve and (after processing) retire the
    changelog entries.
    """
    INITIAL_RECONNECT_TIMEOUT = 1
    MAX_RECONNECT_TIMEOUT = 10 * 60
    RECONNECT_TIMEOUT_MULTIPLIER = 5

    def __init__(self, config: Config):
        self.config = config

        # This is the redis client for the main thread - the worker thread
        # needs to use self.thread_redis_client
        self.redis_client = get_redis_client(config)

        self.thread = threading.Thread(name="BodhiChangeMonitor", target=self._run)
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

    def get_changed(self) -> Tuple[Optional[Set[str]], int]:
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

        self._maybe_reraise_failure("Error communicating with fedora-messaging")

        result: Optional[Set[str]] = set()
        entries = self.redis_client.zrangebyscore(
            "updatechangelog", 0, float("inf"), withscores=True, score_cast_func=int
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

    def clear_changed(self, serial):
        """Remove old changelog entries after they have been processed"""

        self.redis_client.zremrangebyscore("updatechangelog", 0, serial)

    def _maybe_reraise_failure(self, msg):
        with self.lock:
            if self.failure:
                raise RuntimeError(msg) from self.failure

    def _do_add_to_log(self, new_queue_name, update_id, pipe: "redis.client.Pipeline[bytes]"):
        pipe.watch("updatechangelog:serial")
        pipe.watch("updatechangelog")

        pre = cast("redis.Redis[bytes]", pipe)
        serial = 1 + int(pre.get("updatequeue:serial") or 0)

        pipe.multi()
        pipe.set("updatechangelog:serial", serial)
        if new_queue_name:
            pipe.set("fedora-messaging-queue", new_queue_name)
            pipe.delete("updatechangelog")
            pipe.zadd("updatechangelog", {b'': serial})
        else:
            pipe.zadd("updatechangelog", {update_id: serial})

    def _add_to_log(self, update_id):
        self.thread_redis_client.transaction(partial(self._do_add_to_log, None, update_id))

    def _reset_changelog(self, queue_name):
        self.thread_redis_client.transaction(partial(self._do_add_to_log, queue_name, None))

    def _update_from_message(self, body_json):
        body = json.loads(body_json)
        update_id = body['update']['alias']
        logger.info("Saw change to Bodhi Update %s", update_id)
        self._add_to_log(update_id)

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

        channel.queue_bind(queue_name, 'amq.topic',
                           routing_key="org.fedoraproject.prod.bodhi.update.request.#")
        channel.queue_bind(queue_name, 'amq.topic',
                           routing_key="org.fedoraproject.prod.bodhi.update.complete.#")

        # We first consume messages with a timeout of zero until we block to clean
        # out anything queued
        for method, properties, body_json in channel.consume(queue_name,
                                                             inactivity_timeout=0):
            if method is None:
                break
            else:
                self._update_from_message(body_json)
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
            self._update_from_message(body_json)
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

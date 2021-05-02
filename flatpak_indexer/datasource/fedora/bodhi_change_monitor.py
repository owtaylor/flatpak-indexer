import json
import logging
import os
import ssl
import time
import threading
import uuid

import pika


logger = logging.getLogger(__name__)


class BodhiChangeMonitor:
    INITIAL_RECONNECT_TIMEOUT = 1
    MAX_RECONNECT_TIMEOUT = 10 * 60
    RECONNECT_TIMEOUT_MULTIPLIER = 5

    def __init__(self, queue_name=None):
        self.queue_name = queue_name
        self.lock = threading.Lock()
        self.failure = None
        self.started = threading.Event()
        self.thread = threading.Thread(name="BodhiChangeMonitor", target=self._run)
        self.changed_updates = set()
        self.reconnect_timeout = self.INITIAL_RECONNECT_TIMEOUT

    def maybe_reraise_failure(self, msg):
        with self.lock:
            if self.failure:
                raise RuntimeError(msg) from self.failure

    def start(self):
        self.thread.start()
        self.started.wait()

        self.maybe_reraise_failure("Failed to start connection to fedora-messaging")

        return self.queue_name

    def stop(self):
        connection = self.connection

        def do_stop():
            connection.close()

        self.connection.add_callback_threadsafe(do_stop)
        self.thread.join()

        self.maybe_reraise_failure("Failed to stop connection to fedora-messaging")

    def get_changed(self):
        self.maybe_reraise_failure("Error communicating with fedora-messaging")

        with self.lock:
            changed_updates = self.changed_updates
            self.changed_updates = set()
            return changed_updates

    def _update_from_message(self, body_json):
        body = json.loads(body_json)
        with self.lock:
            self.changed_updates.add(body['update']['alias'])

    def _wait_for_messages(self):
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

        queue_name = self.queue_name

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
            self.queue_name = queue_name

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
                self.connection = connection
                self.started.set()
                channel.cancel
                break
            else:
                self._update_from_message(body_json)

        self.reconnect_timeout = self.INITIAL_RECONNECT_TIMEOUT

        # Then we use a timeout of None (never), until the channel is closed
        for method, properties, body_json in channel.consume(queue_name,
                                                             inactivity_timeout=None):
            self._update_from_message(body_json)

    def _run(self):
        while True:
            try:
                self._wait_for_messages()
                return
            except pika.exceptions.AMQPConnectionError as e:
                # This includes stream-lost and connection-refused, which
                # we might get from a broker restart, but also authentication
                # failures, protocol failures, etc. Trying to parse out
                # the exact case would be a future-compat headache.
                logger.warning("fedora-messaging connection failure (%r), "
                               "sleeping for %ss and retrying",
                               e, self.reconnect_timeout, exc_info=e)
                time.sleep(self.reconnect_timeout)
                self.reconnect_timeout = min(
                    self.reconnect_timeout * self.RECONNECT_TIMEOUT_MULTIPLIER,
                    self.MAX_RECONNECT_TIMEOUT
                )
            except Exception as e:
                # The main loop might be in a timeout waiting for the next time
                # to poll - we don't have an easy way to interrupt that, so just
                # store the exception away until get_changed() or stop() is called.
                with self.lock:
                    self.failure = e
                self.started.set()
                return

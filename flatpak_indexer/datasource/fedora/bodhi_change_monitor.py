import json
import os
import ssl
import threading
import uuid

import pika

import logging
logging.basicConfig(level=logging.INFO)


class BodhiChangeMonitor:
    def __init__(self, queue_name=None):
        self.queue_name = queue_name
        self.lock = threading.Lock()
        self.failure = None
        self.started = threading.Event()
        self.thread = threading.Thread(name="BodhiChangeMonitor", target=self._run)
        self.changed_updates = set()

    def start(self):
        self.thread.start()
        self.started.wait()

        if self.failure:
            raise RuntimeError("Failed to start connection to fedora-messaging") from self.failure

        return self.queue_name

    def stop(self):
        connection = self.connection

        def do_stop():
            connection.close()

        self.connection.add_callback_threadsafe(do_stop)
        self.thread.join()

        if self.failure:
            raise RuntimeError("Failed to stop connection to fedora-messaging") from self.failure

    def get_changed(self):
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

        # Then we use a timeout of None (never), until the channel is closed
        for method, properties, body_json in channel.consume(queue_name,
                                                             inactivity_timeout=None):
            self._update_from_message(body_json)

    def _run(self):
        try:
            self._wait_for_messages()
        except Exception as e:
            self.failure = e
            self.started.set()

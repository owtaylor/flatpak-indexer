from contextlib import contextmanager
import json
import threading
from unittest.mock import create_autospec, patch

import pika
import pika.adapters
import pika.adapters.blocking_connection
import pika.exceptions
import pika.spec

from flatpak_indexer.test.decorators import WithArgDecorator


def make_bodhi_message(alias):
    return json.dumps({
        'update': {
            'alias': alias
        }
    })


def make_distgit_message(namespace, repo):
    return json.dumps({
        'commit': {
            'namespace': namespace,
            'repo': repo
        }
    })


class ChannelCancelledMarker:
    pass


class MockConnection():
    def __init__(self, messaging: 'MockMessaging', passive_behavior: bool, raise_on_close: bool):
        self.messaging = messaging
        self.passive_behavior = passive_behavior
        self.raise_on_close = raise_on_close

        self._closed = False
        self._channel = \
            create_autospec(pika.adapters.blocking_connection.BlockingChannel)  # type: ignore
        self._channel.queue_declare.side_effect = self._queue_declare
        self._channel.consume.side_effect = self._consume

    def channel(self):
        return self._channel

    def close(self):
        if self.raise_on_close:
            raise RuntimeError("door broken")

        with self.messaging.condition:
            self._closed = True
            self.messaging.condition.notify()

    def add_callback_threadsafe(self, callback):
        self.messaging.plan_put((callback,))

    def _consume(self, queue_name, inactivity_timeout=None):
        while True:
            with self.messaging.condition:
                while not self._closed and len(self.messaging.plan) == 0:
                    if inactivity_timeout == 0:
                        yield (None, None, None)
                    else:
                        self.messaging.condition.wait()

                if self._closed:
                    return
                else:
                    item = self.messaging.plan_get()

            if len(item) == 1:
                task = item[0]
                if isinstance(task, Exception):
                    raise task
                elif task == ChannelCancelledMarker:
                    return
                else:
                    task()
                item[0]()
            else:
                yield item

    def _queue_declare(self, queue,
                       passive=False,
                       durable=False,
                       exclusive=False,
                       auto_delete=False):
        if passive:
            if self.passive_behavior == "not_exist":
                raise pika.exceptions.ChannelClosedByBroker(404, f"NOT_FOUND - no queue '{queue}'")
            elif self.passive_behavior == "exist":
                return
            else:
                raise pika.exceptions.ChannelClosedByBroker(500, "everything went south")


class MockMessaging():
    def __init__(self,
                 passive_behavior="not_exist",
                 raise_on_close=False):
        self.passive_behavior = passive_behavior
        self.raise_on_close = raise_on_close

        self.condition: threading.Condition = threading.Condition()
        self.plan = []

    def put_update_message(self, update_alias):
        self.plan_put((
            pika.spec.Basic.Deliver(
                routing_key='org.fedoraproject.prod.bodhi.update.complete.stable'
            ),
            'X',
            make_bodhi_message(update_alias)
        ))

    def put_distgit_message(self, namespace, repo):
        self.plan_put((
            pika.spec.Basic.Deliver(routing_key='org.fedoraproject.prod.git.receive'),
            'X',
            make_distgit_message(namespace, repo)
        ))

    def put_inactivity_timeout(self):
        self.plan_put((None, None, None))

    def put_stream_lost(self):
        self.plan_put((pika.exceptions.StreamLostError("Stream connection lost"),))

    def put_connection_error(self):
        self.plan_put((pika.exceptions.AMQPConnectionError("Could not connect"),))

    def put_failure(self):
        self.plan_put((RuntimeError("Something went wrong"),))

    def put_channel_cancelled(self):
        self.plan_put((ChannelCancelledMarker,))

    def put_callback(self, callback):
        self.plan_put((callback,))

    def plan_put(self, item):
        with self.condition:
            self.plan.append(item)
            self.condition.notify()

    def plan_get(self):
        with self.condition:
            return self.plan.pop(0)

    def wait(self):
        event = threading.Event()
        self.plan_put((event.set,))
        event.wait()

    def create_connection(self, *args):
        return MockConnection(self, self.passive_behavior, self.raise_on_close)


@contextmanager
def _setup_fedora_messaging(**kwargs):
    with patch('pika.BlockingConnection', autospec=True) as connection_mock:
        mock_messaging = MockMessaging(**kwargs)
        connection_mock.side_effect = mock_messaging.create_connection
        yield mock_messaging


mock_fedora_messaging = WithArgDecorator('connection_mock', _setup_fedora_messaging)

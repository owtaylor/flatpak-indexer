from functools import partial, update_wrapper
import inspect
import json
import queue
from unittest.mock import patch, create_autospec

import pika


def make_message(alias):
    return json.dumps({
        'update': {
            'alias': alias
        }
    })


class MockConnection():
    def __init__(self, connection,
                 passive_behavior="not_exist",
                 raise_on_close=False):
        self.connection = connection
        self.passive_behavior = passive_behavior
        self.raise_on_close = raise_on_close

        connection.add_callback_threadsafe.side_effect = self._add_callback_threadsafe
        connection.close.side_effect = self._close_connection

        self.queue = queue.Queue()

        self.channel = create_autospec(pika.adapters.blocking_connection.BlockingChannel)
        self.channel.queue_declare.side_effect = self._queue_declare
        self.channel.consume.side_effect = self._consume

        connection.channel.return_value = self.channel

    def put_update_message(self, update_alias):
        self.queue.put(('X', 'X', make_message(update_alias)))

    def put_inactivity_timeout(self):
        self.queue.put((None, None, None))

    def _close_connection(self):
        if self.raise_on_close:
            raise RuntimeError("door broken")

        self.queue.put(())

    def _add_callback_threadsafe(self, callback):
        self.queue.put((callback,))

    def _consume(self, queue_name, inactivity_timeout=None):
        while True:
            item = self.queue.get()
            if item == ():
                return
            elif len(item) == 1:
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


def mock_fedora_messaging(f=None, **mock_connection_kwargs):
    if f is None:
        # Handle arguments to the decorator: when called with only kwargs, return a function
        # that when called wth single function argument, invokes this function
        # including the function *and* kwargs
        return partial(mock_fedora_messaging, **mock_connection_kwargs)

    sig = inspect.signature(f)
    need_mock_connection = 'mock_connection' in sig.parameters

    def wrapper(*args, **kwargs):
        with patch('pika.BlockingConnection', autospec=True) as connection_mock:
            if need_mock_connection:
                kwargs['mock_connection'] = MockConnection(connection_mock.return_value,
                                                           **mock_connection_kwargs)

            return f(*args, **kwargs)

    update_wrapper(wrapper, f)

    if need_mock_connection:
        # We need the computed signature of the final function to not include the
        # mock_connection argument, since pytest will think it's a fixture.
        # We remove the extra from the function we return using functools.partial.
        #
        # functools.update_wrapper does things we need, like updating __dict__ with
        # the pytest marks from the original function. But it also sets result.__wrapped__
        # to point back to the original function, and this results in inspect.signature
        # using the original function for the signature, bringing back the
        # mock_connection argument.

        result = partial(wrapper, mock_connection=None)
        update_wrapper(result, wrapper)
        del result.__dict__['__wrapped__']

        return result
    else:
        return wrapper

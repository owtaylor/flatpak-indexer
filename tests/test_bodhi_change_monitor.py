import threading
import time
from unittest.mock import patch

import pika.exceptions
import pytest

from flatpak_indexer.datasource.fedora.bodhi_change_monitor import BodhiChangeMonitor

from .fedora_messaging import mock_fedora_messaging


def assert_changes(monitor, expected_changes):
    _, changes = monitor.get_changed()

    assert changes == expected_changes


@mock_fedora_messaging
def test_bodhi_change_monitor(connection_mock):
    monitor = BodhiChangeMonitor()

    connection_mock.put_update_message('FEDORA-2018-1a0cf961a1')
    connection_mock.put_inactivity_timeout()

    monitor.start()
    assert_changes(monitor, {'FEDORA-2018-1a0cf961a1'})

    connection_mock.put_update_message('FEDORA-2018-5ebe0eb1f2')

    monitor.stop()
    assert_changes(monitor, {'FEDORA-2018-5ebe0eb1f2'})


@mock_fedora_messaging(raise_on_close=False)
@pytest.mark.parametrize('passive_behavior', ["exist", "not_exist", "exception"])
def test_bodhi_change_monitor_reuse(connection_mock, passive_behavior):
    connection_mock.passive_behavior = passive_behavior

    monitor = BodhiChangeMonitor("MYQUEUE")

    connection_mock.put_update_message('FEDORA-2018-1a0cf961a1')
    connection_mock.put_inactivity_timeout()

    if passive_behavior == "exception":
        with pytest.raises(RuntimeError,
                           match=r'Failed to start connection to fedora-messaging') as exc_info:
            monitor.start()
        assert 'everything went south' in str(exc_info.value.__cause__)

        return

    monitor.start()
    queue_name, changes = monitor.get_changed()
    if passive_behavior == 'exist':
        assert queue_name == "MYQUEUE"
    else:
        assert queue_name != "MYQUEUE"

    assert changes == {'FEDORA-2018-1a0cf961a1'}

    monitor.stop()


@mock_fedora_messaging(raise_on_close=True)
def test_bodhi_change_monitor_stop_exception(connection_mock):
    monitor = BodhiChangeMonitor()

    connection_mock.put_inactivity_timeout()
    monitor.start()

    with pytest.raises(RuntimeError,
                       match=r'Failed to stop connection to fedora-messaging') as exc_info:
        monitor.stop()
    assert "door broken" in str(exc_info.value.__cause__)


@patch(
    'flatpak_indexer.datasource.fedora.bodhi_change_monitor.BodhiChangeMonitor.'
    'INITIAL_RECONNECT_TIMEOUT',
    0.01,
)
@mock_fedora_messaging
def test_bodhi_change_monitor_lost_stream(connection_mock):
    monitor = BodhiChangeMonitor()

    connection_mock.put_inactivity_timeout()
    monitor.start()

    # Lost the connection
    connection_mock.put_stream_lost()

    # First retry failed
    connection_mock.put_connection_error()

    # And then successful reconnection
    connection_mock.put_update_message('FEDORA-2018-1a0cf961a1')
    connection_mock.put_inactivity_timeout()

    connection_mock.wait()
    assert_changes(monitor, {'FEDORA-2018-1a0cf961a1'})

    monitor.stop()


@mock_fedora_messaging
def test_bodhi_change_monitor_failure(connection_mock):
    monitor = BodhiChangeMonitor()

    connection_mock.put_inactivity_timeout()
    monitor.start()

    connection_mock.put_failure()
    connection_mock.put_update_message('FEDORA-2018-1a0cf961a1')

    with pytest.raises(RuntimeError, match=r"Error communicating with fedora-messaging"):
        # Busy loop until the thread handles the exception
        while True:
            monitor.get_changed()


@patch(
    'flatpak_indexer.datasource.fedora.bodhi_change_monitor.BodhiChangeMonitor.'
    'INITIAL_RECONNECT_TIMEOUT',
    0.01,
)
@mock_fedora_messaging
def test_bodhi_change_monitor_stop_during_reconnect(connection_mock):
    """Check that stopping works before we set monitor.connection"""

    monitor = BodhiChangeMonitor()

    event = threading.Event()

    def on_reconnection():
        event.set()

        # Busy loop until we're actually stopping, then let things continue
        while True:
            with monitor.lock:
                if monitor.stopping:
                    return

            time.sleep(0.01)

    connection_mock.put_inactivity_timeout()
    connection_mock.put_stream_lost()
    connection_mock.put_callback(on_reconnection)
    monitor.start()

    # Wait until we are in the consume messages until timeout loop
    event.wait()
    # And then stop
    monitor.stop()


@patch(
    'flatpak_indexer.datasource.fedora.bodhi_change_monitor.BodhiChangeMonitor.'
    'INITIAL_RECONNECT_TIMEOUT',
    0.01,
)
@mock_fedora_messaging
def test_bodhi_change_monitor_stop_during_connection_failure(connection_mock):
    """Check that stopping works even if we never get to the point of consuming messages"""

    monitor = BodhiChangeMonitor()

    # Connect
    connection_mock.put_inactivity_timeout()
    monitor.start()
    connection_mock.wait()

    # Break things so that we can't reconnect
    event = threading.Event()

    def queue_declare(*args, **kwargs):
        event.set()
        raise pika.exceptions.AMQPConnectionError("Could not connect")

    with patch("tests.fedora_messaging.MockConnection._queue_declare", side_effect=queue_declare):
        # Force a reconnection
        connection_mock.put_stream_lost()

        # Wait until we are into the reconnect loop, and then try to stop
        event.wait()
        monitor.stop()

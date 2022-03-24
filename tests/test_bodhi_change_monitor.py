import threading
import time
from unittest.mock import patch

import pika.exceptions
import pytest
import redis
import yaml

from flatpak_indexer.datasource.fedora.bodhi_change_monitor import BodhiChangeMonitor

from .fedora_messaging import mock_fedora_messaging
from .redis import mock_redis
from .utils import get_config


CONFIG = yaml.safe_load("""
redis_url: redis://localhost
koji_config: fedora
""")


@pytest.fixture
def config(tmp_path):
    return get_config(tmp_path, CONFIG)


def assert_changes(monitor, expected_changes):
    changes, serial = monitor.get_changed()

    assert changes == expected_changes
    monitor.clear_changed(serial)


def set_queue_name(config, queue_name="MYQUEUE"):
    redis_client = redis.Redis.from_url(config.redis_url)
    redis_client.set("fedora-messaging-queue", queue_name)


@mock_fedora_messaging(passive_behavior="exist")
@mock_redis
def test_bodhi_change_monitor(connection_mock, config):
    set_queue_name(config)

    monitor = BodhiChangeMonitor(config)

    connection_mock.put_update_message('FEDORA-2018-1a0cf961a1')
    connection_mock.put_inactivity_timeout()

    monitor.start()
    assert_changes(monitor, {'FEDORA-2018-1a0cf961a1'})

    connection_mock.put_update_message('FEDORA-2018-5ebe0eb1f2')

    monitor.stop()
    assert_changes(monitor, {'FEDORA-2018-5ebe0eb1f2'})
    assert_changes(monitor, set())


@mock_fedora_messaging(raise_on_close=False)
@mock_redis
@pytest.mark.parametrize('passive_behavior', ["exist", "not_exist", "exception"])
def test_bodhi_change_monitor_reuse(connection_mock, config, passive_behavior):
    connection_mock.passive_behavior = passive_behavior
    set_queue_name(config)

    monitor = BodhiChangeMonitor(config)

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
        assert_changes(monitor, {'FEDORA-2018-1a0cf961a1'})
    else:
        assert_changes(monitor, None)

    monitor.stop()


@mock_fedora_messaging(raise_on_close=True)
@mock_redis
def test_bodhi_change_monitor_stop_exception(connection_mock, config):
    monitor = BodhiChangeMonitor(config)

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
@mock_fedora_messaging(passive_behavior="exist")
@mock_redis
def test_bodhi_change_monitor_lost_stream(connection_mock, config):
    set_queue_name(config)
    monitor = BodhiChangeMonitor(config)

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


@patch(
    'flatpak_indexer.datasource.fedora.bodhi_change_monitor.BodhiChangeMonitor.'
    'INITIAL_RECONNECT_TIMEOUT',
    0.01,
)
@mock_fedora_messaging
@mock_redis
def test_bodhi_change_monitor_channel_cancelled(connection_mock, config):
    monitor = BodhiChangeMonitor(config)

    connection_mock.put_inactivity_timeout()
    monitor.start()

    # Channel was cancelled (queue deleted?)
    connection_mock.put_channel_cancelled()

    # Successful reconnection
    connection_mock.put_update_message('FEDORA-2018-1a0cf961a1')
    connection_mock.put_inactivity_timeout()

    connection_mock.wait()
    assert_changes(monitor, None)

    monitor.stop()


@mock_fedora_messaging
@mock_redis
def test_bodhi_change_monitor_failure(connection_mock, config):
    monitor = BodhiChangeMonitor(config)

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
@mock_redis
def test_bodhi_change_monitor_stop_during_reconnect(connection_mock, config):
    """Check that stopping works before we set monitor.connection"""

    monitor = BodhiChangeMonitor(config)

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
@mock_redis
def test_bodhi_change_monitor_stop_during_connection_failure(connection_mock, config):
    """Check that stopping works even if we never get to the point of consuming messages"""

    monitor = BodhiChangeMonitor(config)

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


@patch(
    'flatpak_indexer.datasource.fedora.bodhi_change_monitor.BodhiChangeMonitor.'
    'INITIAL_RECONNECT_TIMEOUT',
    0.01,
)
@mock_fedora_messaging
@mock_redis
def test_bodhi_change_monitor_cancelled_during_connect(connection_mock, config):
    monitor = BodhiChangeMonitor(config)

    connection_mock.put_channel_cancelled()
    connection_mock.put_inactivity_timeout()

    monitor.start()
    connection_mock.wait()
    assert_changes(monitor, None)

    monitor.stop()

import pytest

from flatpak_indexer.datasource.fedora.bodhi_change_monitor import BodhiChangeMonitor

from .fedora_messaging import mock_fedora_messaging


@mock_fedora_messaging
def test_bodhi_change_monitor(mock_connection):
    monitor = BodhiChangeMonitor()

    mock_connection.put_update_message('FEDORA-2018-1a0cf961a1')
    mock_connection.put_inactivity_timeout()

    monitor.start()
    assert monitor.get_changed() == {'FEDORA-2018-1a0cf961a1'}

    mock_connection.put_update_message('FEDORA-2018-5ebe0eb1f2')

    monitor.stop()
    assert monitor.get_changed() == {'FEDORA-2018-5ebe0eb1f2'}


@mock_fedora_messaging(raise_on_close=False)
@pytest.mark.parametrize('passive_behavior', ["exist", "not_exist", "exception"])
def test_bodhi_change_monitor_reuse(mock_connection, passive_behavior):
    mock_connection.passive_behavior = passive_behavior

    monitor = BodhiChangeMonitor("MYQUEUE")

    mock_connection.put_update_message('FEDORA-2018-1a0cf961a1')
    mock_connection.put_inactivity_timeout()

    if passive_behavior == "exception":
        with pytest.raises(RuntimeError,
                           match=r'Failed to start connection to fedora-messaging') as exc_info:
            monitor.start()
        assert 'everything went south' in str(exc_info.value.__cause__)

        return

    queue_name = monitor.start()
    if passive_behavior == 'exist':
        assert queue_name == "MYQUEUE"
    else:
        assert queue_name != "MYQUEUE"

    assert monitor.get_changed() == {'FEDORA-2018-1a0cf961a1'}

    monitor.stop()


@mock_fedora_messaging(raise_on_close=True)
def test_bodhi_change_monitor_stop_exception(mock_connection):
    monitor = BodhiChangeMonitor()

    mock_connection.put_inactivity_timeout()
    monitor.start()

    with pytest.raises(RuntimeError,
                       match=r'Failed to stop connection to fedora-messaging') as exc_info:
        monitor.stop()
    assert "door broken" in str(exc_info.value.__cause__)

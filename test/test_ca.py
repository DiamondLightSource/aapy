from datetime import datetime
import mock
import pytest
from aa import ca


CA_EVENT_1D = {'value': [1],
               'secs': 100,
               'nano': 1e8,
               'sevr': 0
               }


CA_EVENT_2D = {'value': [1.1, 2, 3],
               'secs': 10,
               'nano': 21e7,
               'sevr': 1
               }


@pytest.fixture
def ca_fetcher():
    fetcher = ca.CaFetcher('http://url')
    fetcher._client = mock.MagicMock()
    return fetcher


@pytest.fixture
def ca_client():
    """Mock the xmlrpc ServerProxy out of the CaClient.

    This violates the rule "don't mock what you don't own", but this class
    is the shim I've used to avoid violating that rule elsewhere.

    Returns:
        CaClient object with mock xmlrpcproxy.
    """
    with mock.patch('aa.ca.ServerProxy'):
        cac = ca.CaClient('http:url')
        cac._proxy.archiver.values.return_value = [{'values': [CA_EVENT_1D]}]
        return cac


def test_CaClient_create_archive_event_handles_1d_event(ca_client, dummy_pv, event_1d):
    assert ca_client._create_archive_event(dummy_pv, CA_EVENT_1D) == event_1d


def test_CaClient_create_archive_event_handles_2d_event(ca_client, dummy_pv, event_2d):
    assert ca_client._create_archive_event(dummy_pv, CA_EVENT_2D) == event_2d


def test_CaClient_get_returns_event(ca_client, dummy_pv, event_1d):
    print(ca_client)
    with mock.patch('aa.utils.datetime_to_epoch'):
        events = ca_client.get(dummy_pv, datetime.now(), datetime.now(), 2)
    assert events == [event_1d]


def test_CaFetcher_get_values_calls_client_get_once_if_response_less_than_count(ca_fetcher, event_1d, data_1d):
    # One value will be returned.
    ca_fetcher._client.get.return_value = [event_1d]
    # Ask for two values.
    response = ca_fetcher.get_values('dummy', datetime.now(), datetime.now(), 2)
    assert len(ca_fetcher._client.get.call_args_list) == 1
    assert response == data_1d


def test_CaFetcher_get_values_calls_client_get_twice_if_count_exceeds_10000(ca_fetcher, event_1d, event_1d_alt, data_2_events):
    events = [event_1d] * 10000
    ca_fetcher._client.get.side_effect = (events, [event_1d_alt])
    # Ask for two values.
    ca_fetcher.get_values('dummy', datetime.now(), datetime.now(), 10001)
    assert len(ca_fetcher._client.get.call_args_list) == 2

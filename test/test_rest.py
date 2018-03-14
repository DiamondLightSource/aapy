import mock
import pytest
import requests
from aa import rest


HOSTNAME = 'host'
SOCKET = 'host:80'
PV = 'dummy-pv'


@pytest.fixture()
def aa_client():
    return rest.AaRestClient(HOSTNAME)


@pytest.mark.parametrize('kwargs', ({}, {'a': 'b'}, {'a': 'b', 'c': 'd'}))
def test_construct_url(kwargs):
    url = rest.construct_url(HOSTNAME, 'cmd', **kwargs)
    assert url.startswith('http://host/mgmt/bpl/cmd')
    for k, v in kwargs.items():
        assert '{}={}'.format(k, v) in url


@mock.patch('aa.rest.make_rest_call')
def test_AaRestClient_get_all_pvs(mock_rest_call,aa_client):
    aa_client.get_all_pvs()
    mock_rest_call.assert_called_with(SOCKET, 'getAllPVs', limit=-1)


@mock.patch('aa.rest.make_rest_call')
def test_AaRestClient_get_pv_info(mock_rest_call,aa_client):
    aa_client.get_pv_info(PV)
    mock_rest_call.assert_called_with(SOCKET, 'getPVTypeInfo', pv=PV)


def test_delete_or_abort_calls_remove_pv(aa_client):
    aa_client.remove_pv = mock.MagicMock()
    aa_client.abort_request = mock.MagicMock()
    aa_client.delete_or_abort(PV)
    aa_client.remove_pv.assert_called_with(PV)
    aa_client.abort_request.assert_not_called()


def test_delete_or_abort_calls_abort_pv_if_remove_throws_HTTPError(aa_client):
    exception = requests.exceptions.HTTPError()
    aa_client.remove_pv = mock.MagicMock(side_effect=exception)
    aa_client.abort_request = mock.MagicMock()
    aa_client.delete_or_abort(PV)
    aa_client.remove_pv.assert_called_with(PV)
    aa_client.abort_request.assert_called_with(PV)


def test_remove_pv_calls_pause_pv_and_delete_pv(aa_client):
    aa_client.pause_pv = mock.MagicMock()
    aa_client.delete_pv = mock.MagicMock()
    aa_client.remove_pv(PV)
    aa_client.pause_pv.assert_called_with(PV)
    aa_client.delete_pv.assert_called_with(PV)

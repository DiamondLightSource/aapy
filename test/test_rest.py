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
def test_AaRestClient_construct_url(kwargs, aa_client):
    url = aa_client._construct_url('cmd', **kwargs)
    assert url.startswith('http://host:80/mgmt/bpl/cmd')
    for k, v in kwargs.items():
        assert '{}={}'.format(k, v) in url


@mock.patch('aa.utils.urlget')
def test_AaRestClient_get_all_pvs(mock_urlget, aa_client):
    aa_client.get_all_pvs()
    command = 'getAllPVs'
    target_url = 'http://{}/mgmt/bpl/{}?limit=-1'.format(SOCKET, command)
    mock_urlget.assert_called_with(target_url)


@mock.patch('aa.utils.urlget')
def test_AaRestClient_get_all_pvs(mock_urlget, aa_client):
    pv = 'dummy'
    aa_client.get_pv_info(pv)
    command = 'getPVTypeInfo'
    target_url = 'http://{}/mgmt/bpl/{}?pv={}'.format(SOCKET, command, pv)
    mock_urlget.assert_called_with(target_url)


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

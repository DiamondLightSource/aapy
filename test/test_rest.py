import mock
import pytest
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
    base_url = 'http://host:80/mgmt/bpl/cmd'
    if not kwargs:
        assert url == base_url
    else:
        assert url.startswith(base_url + '?')
        for k, v in kwargs.items():
            assert '{}={}'.format(k, v) in url


@pytest.mark.parametrize('command,method,kwargs', [
        ('getAllPVs', 'get_all_pvs', {'limit': -1}),
        ('getPVTypeInfo', 'get_pv_info', {'pv': 'dummy'}),
        ('getPVStatus', 'get_pv_status', {'pv': 'dummy'}),
        ('getNeverConnectedPVs', 'get_never_connected_pvs', {}),
        ('getCurrentlyDisconnectedPVs', 'get_disconnected_pvs', {}),
        ('pauseArchivingPV', 'pause_pv', {'pv': 'dummy'}),
        ('deletePV', 'delete_pv', {'pv': 'dummy'}),
        ('abortArchivingPV', 'abort_request', {'pv': 'dummy'})
])
@mock.patch('aa.utils.urlget')
def test_AaRestClient_simple_gets(mock_urlget, command, method, kwargs, aa_client):
    target_url = aa_client._construct_url(command, **kwargs)
    getattr(aa_client, method)(**kwargs)
    mock_urlget.assert_called_with(target_url)


@mock.patch('aa.utils.urlget')
def test_AaRestClient_get_all_pvs(mock_urlget, aa_client):
    aa_client.get_all_pvs()
    command = 'getAllPVs'
    target_url = 'http://{}/mgmt/bpl/{}?limit=-1'.format(SOCKET, command)
    mock_urlget.assert_called_with(target_url)


@mock.patch('aa.utils.urlget')
def test_AaRestClient_get_pv_info(mock_urlget, aa_client):
    pv = 'dummy'
    aa_client.get_pv_info(pv)
    command = 'getPVTypeInfo'
    target_url = 'http://{}/mgmt/bpl/{}?pv={}'.format(SOCKET, command, pv)
    mock_urlget.assert_called_with(target_url)



import mock
import pytest
from aa import rest


HOSTNAME = 'host'
SOCKET = 'host:80'


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
    pv = 'dummy-pv'
    aa_client.get_pv_info(pv)
    mock_rest_call.assert_called_with(SOCKET, 'getPVTypeInfo', pv=pv)

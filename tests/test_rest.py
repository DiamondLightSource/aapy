import mock
import pytest

from aa import MONITOR, rest

HOSTNAME = "host"
SOCKET = "host:80"
PV = "dummy-pv"


@pytest.fixture()
def aa_client():
    return rest.AaRestClient(HOSTNAME)


@pytest.mark.parametrize("kwargs", ({}, {"a": "b"}, {"a": "b", "c": "d"}))
def test_AaRestClient_construct_url(kwargs, aa_client):
    url = aa_client._construct_url("cmd", **kwargs)
    base_url = "http://host:80/mgmt/bpl/cmd"
    if not kwargs:
        assert url == base_url
    else:
        assert url.startswith(base_url + "?")
        for k, v in kwargs.items():
            assert "{}={}".format(k, v) in url


@pytest.mark.parametrize(
    "command,method,kwargs",
    [
        ("getAllPVs", "get_all_pvs", {"limit": -1}),
        ("getPVTypeInfo", "get_pv_type_info", {"pv": "dummy"}),
        ("getPVStatus", "get_pv_status", {"pv": "dummy"}),
        ("getNeverConnectedPVs", "get_never_connected_pvs", {}),
        ("getCurrentlyDisconnectedPVs", "get_currently_disconnected_pvs", {}),
        (
            "archivePV",
            "archive_pv",
            {"pv": "dummy", "samplingperiod": 10, "samplingmethod": MONITOR},
        ),
        ("pauseArchivingPV", "pause_archiving_pv", {"pv": "dummy"}),
        ("resumeArchivingPV", "resume_archiving_pv", {"pv": "dummy"}),
        ("deletePV", "delete_pv", {"pv": "dummy"}),
        ("abortArchivingPV", "abort_archiving_pv", {"pv": "dummy"}),
    ],
)
@mock.patch("requests.get")
def test_AaRestClient_simple_gets(mock_get, command, method, kwargs, aa_client):
    target_url = aa_client._construct_url(command, **kwargs)
    getattr(aa_client, method)(**kwargs)
    mock_get.assert_called_with(target_url)


@mock.patch("requests.post")
def test_AaRestClient_get_pv_statuses(mock_post, aa_client):
    pv_names = ["dummy1", "dummy2"]
    pv_name_payload = "pv=dummy1,dummy2"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    target_url = aa_client._construct_url("getPVStatus")
    aa_client.get_pv_statuses(pv_names)
    mock_post.assert_called_with(target_url, pv_name_payload, headers=headers)


def test_AaRestClient_archive_pv_raises_ValueError_if_method_invalid(aa_client):
    with pytest.raises(ValueError):
        aa_client.archive_pv("dummy", 10, "not-scan-or-monitor")

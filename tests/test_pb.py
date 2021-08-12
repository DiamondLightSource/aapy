import os

import mock
import numpy
import pytest
import requests
import utils as testutils

from aa import data
from aa import epics_event_pb2 as ee
from aa import pb, utils

TIMESTAMP_2001 = 978307200
TIMESTAMP_INACCURACY = 1e-6


PV = "dummy"
# The raw bytes containing some Archiver Appliance data.
RAW_PAYLOAD_INFO = (
    b"\x08\x06\x12\x14\x53\x52\x2d\x44\x49\x2d\x44\x43\x43\x54\x2d\x30\x31\x3a\x53"
    b"\x49\x47\x4e\x41\x4c\x18\xdf\x0f\x20\x01"
)
RAW_EVENT = (
    b"\x08\x8f\xd9\xa3\x01\x10\x86\x8f\xfd\x01\x19\x0c\x19\x52\xcb\x7d\x8a\x70\x40"
)
# The contents of a PB file with a header and one event.
PB_CHUNK = RAW_PAYLOAD_INFO + b"\n" + RAW_EVENT
# The actual contents of the above raw strings (PV name is not stored)
EVENT = data.ArchiveEvent(PV, numpy.array([264.65571148]), 1422752399.004147078, 0)


def test_parse_PayloadInfo():
    pi = ee.PayloadInfo()
    pi.ParseFromString(RAW_PAYLOAD_INFO)
    assert pi.year == 2015
    assert pi.type == 6


def test_parse_ScalarDouble():
    e = ee.ScalarDouble()
    e.ParseFromString(RAW_EVENT)
    assert abs(e.val - EVENT.value[0]) < 1e-7
    year_timestamp = utils.year_timestamp(2015)
    assert year_timestamp + e.secondsintoyear + 1e-9 * e.nano == EVENT.timestamp
    assert e.severity == EVENT.severity


def test_parse_pb_data():
    start = utils.utc_datetime(2015, 1, 1)
    end = utils.utc_datetime(2015, 12, 1)
    result = pb.parse_pb_data(PB_CHUNK, PV, start, end)
    event = result.get_event(0)
    assert event == EVENT


def test_unescape_line_does_not_change_regular_bytes():
    test_bytes = b"hello:-1|bye"
    assert pb.unescape_bytes(test_bytes) == test_bytes


def test_unescape_bytes_handles_example_escaped_bytes():
    test_bytes = b"hello" + pb.ESC_BYTE + b"\x02" + b"bye"
    assert pb.unescape_bytes(test_bytes) == b"hello" + pb.NL_BYTE + b"bye"


def test_escape_bytes_does_not_change_regular_bytes():
    test_bytes = b"hello:-1|bye"
    assert pb.escape_bytes(test_bytes) == test_bytes


def test_escape_bytes_handles_example_unescaped_bytes():
    test_bytes = b"hello" + b"\x0A" + b"bye" + b"\x1B"
    expected = b"hello" + pb.ESC_BYTE + b"\x02" + b"bye" + pb.ESC_BYTE + b"\x01"
    assert pb.escape_bytes(test_bytes) == expected


def test_unescape_bytes_works_in_correct_order_and_is_reversible():
    test_bytes = b"hello \x1B\x01\x02 bye \x1B\x01\x03"
    expected = b"hello \x1B\x02 bye \x1B\x03"
    unescaped = pb.unescape_bytes(test_bytes)
    assert unescaped == expected
    escaped = pb.escape_bytes(unescaped)
    assert escaped == test_bytes


def test_event_timestamp_gives_correct_answer_1970():
    event = mock.MagicMock()
    event.secondsintoyear = 10
    event.nano = 1e7
    assert pb.event_timestamp(1970, event) == 10.01


def test_event_timestamp_gives_correct_answer_2001():
    seconds = 1000
    nanos = 12345
    expected = TIMESTAMP_2001 + seconds + 1e9 * nanos
    event = mock.MagicMock()
    event.secondsintoyear = seconds
    event.nano = nanos
    assert pb.event_timestamp(2001, event) - expected < TIMESTAMP_INACCURACY


def test_PbFetcher_creates_correct_url():
    pb_fetcher = pb.PbFetcher("dummy.com", 8000)
    assert pb_fetcher._url == "http://dummy.com:8000/retrieval/data/getData.raw"


def test_PbFetcher_get_calls_get_with_correct_url(dummy_pv, jan_2018):
    with mock.patch("requests.get") as mock_get:
        mock_get.return_value = testutils.mock_response(raw=PB_CHUNK)
        pb_fetcher = pb.PbFetcher("dummy.com", 8000)
        pb_fetcher.get_event_at(dummy_pv, jan_2018)
        expected_url = (
            "http://dummy.com:8000/retrieval/data/getData.raw"
            "?pv=dummy&from=2018-01-01T00:00:00Z&to=2018-01-01T00:00:00Z"
            "&fetchLatestMetadata=true"
        )
        mock_get.assert_called_with(expected_url, stream=True)


def test_PbFetcher_get_returns_empty_data_if_get_throws_HTTPError_404(
    dummy_pv, jan_2018, empty_data
):
    with mock.patch("requests.get") as mock_get:
        mock_response = mock.MagicMock(status_code=404)
        http_error = requests.exceptions.HTTPError(response=mock_response)
        mock_get.side_effect = http_error
        pb_fetcher = pb.PbFetcher("dummy.com", 8000)
        result = pb_fetcher.get_values(dummy_pv, jan_2018, jan_2018)
        assert result == empty_data


def test_PbFetcher_get_raises_if_get_throws_HTTPError_not_404(
    dummy_pv, jan_2018, empty_data
):
    with mock.patch("requests.get") as mock_get:
        mock_response = mock.MagicMock(status_code=405)
        http_error = requests.exceptions.HTTPError(response=mock_response)
        mock_get.side_effect = http_error
        pb_fetcher = pb.PbFetcher("dummy.com", 8000)
        with pytest.raises(requests.exceptions.HTTPError):
            pb_fetcher.get_values(dummy_pv, jan_2018, jan_2018)


def test_PbFileFetcher_get_all_pb_files_of_pv():
    dummy_files = [
        "root/LTS/a/b/c/d_2001.pb",
        "root/MTS/a/b/c/d_2001_02_03.pb",
        "root/STS/a/b/c/d_2001_02_03_04.pb",
    ]

    def side_effect(path_pv_dir):
        for i, s in enumerate(["LTS", "MTS", "STS"]):
            if s in path_pv_dir:
                return [dummy_files[i]]

    with mock.patch("glob.glob") as mock_glob:
        mock_glob.side_effect = side_effect
        root = "root"
        pv = "a-b-c:d"
        fetcher = pb.PbFileFetcher(root)
        assert fetcher._get_all_pb_files_of_pv(pv) == dummy_files


def test_PbFileFetcher_get_all_pb_files_of_pv_handels_pv_with_two_colons():
    dummy_files = [
        "root/LTS/a/b/c/d_2001.pb",
        "root/MTS/a/b/c/d_2001_02_03.pb",
        "root/STS/a/b/c/d_2001_02_03_04.pb",
    ]

    def side_effect(path_pv_dir):
        for i, s in enumerate(["LTS", "MTS", "STS"]):
            if s in path_pv_dir:
                return [dummy_files[i]]

    with mock.patch("glob.glob") as mock_glob:
        mock_glob.side_effect = side_effect
        root = "root"
        pv = "a-b:c:d"
        fetcher = pb.PbFileFetcher(root)
        assert fetcher._get_all_pb_files_of_pv(pv) == dummy_files


def test_PbFileFetcher_create_datetime_for_pb_file():
    root = "root"
    filepath = os.path.join("root", "a", "b", "c", "d:2001_02_03_04_05.pb")
    fetcher = pb.PbFileFetcher(root)
    expected = utils.utc_datetime(2001, 2, 3, 4, 5)
    assert fetcher._create_datetime_for_pb_file(filepath) == expected


def test_PbFileFetcher_create_datetime_for_pb_file_only_year():
    root = "root"
    filepath = os.path.join("root", "a", "b", "c", "d:2001.pb")
    fetcher = pb.PbFileFetcher(root)
    expected = utils.utc_datetime(2001, 1, 1)
    assert fetcher._create_datetime_for_pb_file(filepath) == expected


def test_PbFileFetcher_get_pb_files_no_file_found():
    with mock.patch("glob.glob") as mock_glob:
        mock_glob.return_value = []
        root = "root"
        pv = "a-b-c:d"
        fetcher = pb.PbFileFetcher(root)
        start = utils.utc_datetime(2001, 1, 1)
        end = utils.utc_datetime(2001, 1, 2)
        expected = []
        assert fetcher._get_pb_files(pv, start, end) == expected


def test_PbFileFetcher_get_pb_files_too_early():
    dummy_files = [
        "/root/LTS/a/b/c/d_2001.pb",
        "/root/MTS/a/b/c/d_2001_02_02.pb",
        "/root/MTS/a/b/c/d_2001_02_03.pb",
        "/root/STS/a/b/c/d_2001_02_03_04.pb",
        "/root/STS/a/b/c/d_2001_02_03_05.pb",
    ]
    with mock.patch("aa.pb.PbFileFetcher._get_all_pb_files_of_pv") as mock_get_files:
        mock_get_files.return_value = dummy_files
        root = "root"
        pv = "a-b-c:d"
        fetcher = pb.PbFileFetcher(root)
        start = utils.utc_datetime(2000, 1, 1)
        end = utils.utc_datetime(2000, 1, 2)
        expected = [dummy_files[0]]
        assert fetcher._get_pb_files(pv, start, end) == expected


def test_PbFileFetcher_get_pb_files_first_recordings():
    dummy_files = [
        "/root/LTS/a/b/c/d_2001.pb",
        "/root/MTS/a/b/c/d_2001_02_02.pb",
        "/root/MTS/a/b/c/d_2001_02_03.pb",
        "/root/STS/a/b/c/d_2001_02_03_04.pb",
        "/root/STS/a/b/c/d_2001_02_03_05.pb",
    ]
    with mock.patch("aa.pb.PbFileFetcher._get_all_pb_files_of_pv") as mock_get_files:
        mock_get_files.return_value = dummy_files
        root = "root"
        pv = "a-b-c:d"
        fetcher = pb.PbFileFetcher(root)
        start = utils.utc_datetime(2000, 12, 1)
        end = utils.utc_datetime(2001, 1, 2)
        expected = [dummy_files[0]]
        assert fetcher._get_pb_files(pv, start, end) == expected


def test_PbFileFetcher_get_pb_files_from_middle():
    dummy_files = [
        "/root/LTS/a/b/c/d_2001.pb",
        "/root/MTS/a/b/c/d_2001_02_02.pb",
        "/root/MTS/a/b/c/d_2001_02_03.pb",
        "/root/STS/a/b/c/d_2001_02_03_04.pb",
        "/root/STS/a/b/c/d_2001_02_03_05.pb",
    ]
    with mock.patch("aa.pb.PbFileFetcher._get_all_pb_files_of_pv") as mock_get_files:
        mock_get_files.return_value = dummy_files
        root = "root"
        pv = "a-b-c:d"
        fetcher = pb.PbFileFetcher(root)
        start = utils.utc_datetime(2001, 2, 2)
        end = utils.utc_datetime(2001, 2, 3, 4, 5)
        expected = dummy_files[1:4]
        assert fetcher._get_pb_files(pv, start, end) == expected


def test_PbFileFetcher_get_pb_files_newest_recordings():
    dummy_files = [
        "/root/LTS/a/b/c/d_2001.pb",
        "/root/MTS/a/b/c/d_2001_02_02.pb",
        "/root/MTS/a/b/c/d_2001_02_03.pb",
        "/root/STS/a/b/c/d_2001_02_03_04.pb",
        "/root/STS/a/b/c/d_2001_02_03_05.pb",
    ]
    with mock.patch("aa.pb.PbFileFetcher._get_all_pb_files_of_pv") as mock_get_files:
        mock_get_files.return_value = dummy_files
        root = "root"
        pv = "a-b-c:d"
        fetcher = pb.PbFileFetcher(root)
        start = utils.utc_datetime(2001, 2, 3, 5, 6)
        end = utils.utc_datetime(2001, 2, 3, 5, 30)
        expected = [dummy_files[4]]
        assert fetcher._get_pb_files(pv, start, end) == expected


def test_PbFileFetcher_get_pb_files_too_late():
    dummy_files = [
        "/root/LTS/a/b/c/d_2001.pb",
        "/root/MTS/a/b/c/d_2001_02_02.pb",
        "/root/MTS/a/b/c/d_2001_02_03.pb",
        "/root/STS/a/b/c/d_2001_02_03_04.pb",
        "/root/STS/a/b/c/d_2001_02_03_05.pb",
    ]
    with mock.patch("aa.pb.PbFileFetcher._get_all_pb_files_of_pv") as mock_get_files:
        mock_get_files.return_value = dummy_files
        root = "root"
        pv = "a-b-c:d"
        fetcher = pb.PbFileFetcher(root)
        start = utils.utc_datetime(2001, 2, 4)
        expected = [dummy_files[4]]
        # using default for end
        assert fetcher._get_pb_files(pv, start) == expected


def test_PbFileFetcher_get_pb_files_wrong_time_order():
    dummy_files = [
        "/root/LTS/a/b/c/d_2001.pb",
        "/root/MTS/a/b/c/d_2001_02_02.pb",
        "/root/MTS/a/b/c/d_2001_02_03.pb",
        "/root/STS/a/b/c/d_2001_02_03_04.pb",
        "/root/STS/a/b/c/d_2001_02_03_05.pb",
    ]
    with mock.patch("aa.pb.PbFileFetcher._get_all_pb_files_of_pv") as mock_get_files:
        mock_get_files.return_value = dummy_files
        root = "root"
        pv = "a-b-c:d"
        fetcher = pb.PbFileFetcher(root)
        end = utils.utc_datetime(2001, 2, 2)
        start = utils.utc_datetime(2001, 2, 3, 4, 5)
        expected = dummy_files[1:4]
        assert fetcher._get_pb_files(pv, start, end) == expected


def test_PbFileFetcher_read_pb_files(dummy_pv, jan_2001, jan_2018):
    filepath = testutils.get_data_filepath("string_event.pb")
    root = "root"
    fetcher = pb.PbFileFetcher(root)
    data = fetcher._read_pb_files([filepath], dummy_pv, jan_2001, jan_2018, None)
    assert data.values[0] == "2015-01-08 19:47:01 UTC"
    assert data.timestamps[0] == 1507712433.235971


def test_PbFileFetcher_read_pb_files_returns_preceding_event(dummy_pv, jan_2018):
    filepath = testutils.get_data_filepath("jan_2016.pb")
    root = "root"
    fetcher = pb.PbFileFetcher(root)
    feb_2016 = utils.utc_datetime(2016, 2, 1)
    data = fetcher._read_pb_files([filepath], dummy_pv, feb_2016, jan_2018, None)
    assert data.values[0] == [0]
    assert data.timestamps[0] == 1453202116.1772349


def test_PbFileFetcher_read_pb_files_omits_subsequent_event(dummy_pv, jan_2001):
    filepath = testutils.get_data_filepath("jan_2016.pb")
    root = "root"
    fetcher = pb.PbFileFetcher(root)
    dec_2015 = utils.utc_datetime(2015, 12, 1)
    data = fetcher._read_pb_files([filepath], dummy_pv, jan_2001, dec_2015, None)
    assert len(data) == 0


def test_get_iso_timestamp_for_event_has_expected_output():
    event = ee.ScalarInt()
    event.secondsintoyear = 15156538
    event.nano = 381175701
    year = 2017
    expected = "2017-06-25T11:08:58.381176+01:00"
    assert pb.get_iso_timestamp_for_event(year, event) == expected

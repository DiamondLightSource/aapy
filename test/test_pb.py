import os

import mock
import pytest
import requests

from aa import data, pb, utils
from aa import epics_event_pb2 as ee
import utils as testutils


TIMESTAMP_2001 = 978307200
TIMESTAMP_INACCURACY = 1e-6


PV = 'dummy'
# The raw bytes containing some Archiver Appliance data.
RAW_PAYLOAD_INFO = b'\x08\x06\x12\x14\x53\x52\x2d\x44\x49\x2d\x44\x43\x43\x54\x2d\x30\x31\x3a\x53\x49\x47\x4e\x41\x4c\x18\xdf\x0f\x20\x01'
RAW_EVENT = b'\x08\x8f\xd9\xa3\x01\x10\x86\x8f\xfd\x01\x19\x0c\x19\x52\xcb\x7d\x8a\x70\x40'
# The contents of a PB file with a header and one event.
PB_CHUNK = RAW_PAYLOAD_INFO + b'\n' + RAW_EVENT
# The actual contents of the above raw strings (PV name is not stored)
EVENT = data.ArchiveEvent(PV, 264.65571148, 1422752399.004147078, 0)


def test_parse_PayloadInfo():
    pi = ee.PayloadInfo()
    pi.ParseFromString(RAW_PAYLOAD_INFO)
    assert pi.year == 2015
    assert pi.type == 6


def test_parse_ScalarDouble():
    e = ee.ScalarDouble()
    e.ParseFromString(RAW_EVENT)
    assert abs(e.val - EVENT.value) < 1e-7
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
    test_bytes = b'hello:-1|bye'
    assert pb.unescape_bytes(test_bytes) == test_bytes


def test_unescape_bytes_handles_example_escaped_bytes():
    test_bytes = b'hello' + pb.ESC_BYTE + b'\x02' + b'bye'
    assert pb.unescape_bytes(test_bytes) == b'hello' + pb.NL_BYTE + b'bye'


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
    pb_fetcher = pb.PbFetcher('dummy.com', 8000)
    assert pb_fetcher._url == 'http://dummy.com:8000/retrieval/data/getData.raw'


def test_PbFetcher_get_calls_get_with_correct_url(dummy_pv, jan_2018):
    with mock.patch('requests.get') as mock_get:
        mock_get.return_value = testutils.mock_response(raw=PB_CHUNK)
        pb_fetcher = pb.PbFetcher('dummy.com', 8000)
        pb_fetcher.get_event_at(dummy_pv, jan_2018)
        expected_url = 'http://dummy.com:8000/retrieval/data/getData.raw?pv=dummy&from=2018-01-01T00:00:00Z&to=2018-01-01T00:00:00Z'
        mock_get.assert_called_with(expected_url, stream=True)


def test_PbFetcher_get_returns_empty_data_if_get_throws_HTTPError_404(dummy_pv, jan_2018, empty_data):
    with mock.patch('requests.get') as mock_get:
        mock_response = mock.MagicMock(status_code=404)
        http_error = requests.exceptions.HTTPError(response=mock_response)
        mock_get.side_effect = http_error
        pb_fetcher = pb.PbFetcher('dummy.com', 8000)
        result = pb_fetcher.get_values(dummy_pv, jan_2018, jan_2018)
        assert result == empty_data


def test_PbFetcher_get_raises_if_get_throws_HTTPError_not_404(dummy_pv, jan_2018, empty_data):
    with mock.patch('requests.get') as mock_get:
        mock_response = mock.MagicMock(status_code=405)
        http_error = requests.exceptions.HTTPError(response=mock_response)
        mock_get.side_effect = http_error
        pb_fetcher = pb.PbFetcher('dummy.com', 8000)
        with pytest.raises(requests.exceptions.HTTPError):
            pb_fetcher.get_values(dummy_pv, jan_2018, jan_2018)


def test_PbFileFetcher_get_pb_file_handles_pv_with_one_colon():
    root = 'root'
    year = 2001
    pv = 'a-b-c:d'
    fetcher = pb.PbFileFetcher(root)
    expected = os.path.join('root', 'a', 'b', 'c', 'd:2001.pb')
    assert fetcher._get_pb_file(pv, year) == expected


def test_PbFileFetcher_get_pb_file_handles_pv_with_two_colons():
    root = 'root'
    year = 2001
    pv = 'a-b-c:d:e'
    fetcher = pb.PbFileFetcher(root)
    expected = os.path.join('root', 'a', 'b', 'c', 'd', 'e:2001.pb')
    assert fetcher._get_pb_file(pv, year) == expected


def test_PbFileFetcher_read_pb_files(dummy_pv, jan_2001, jan_2018):
    filepath = testutils.get_data_filepath('string_event.pb')
    root = 'root'
    fetcher = pb.PbFileFetcher(root)
    data = fetcher._read_pb_files([filepath], dummy_pv, jan_2001, jan_2018, None)
    assert data.values[0] == '2015-01-08 19:47:01 UTC'
    assert data.timestamps[0] == 1507712433.235971


def test_PbFileFetcher_read_pb_files_returns_preceding_event(dummy_pv, jan_2018):
    filepath = testutils.get_data_filepath('jan_2016.pb')
    root = 'root'
    fetcher = pb.PbFileFetcher(root)
    feb_2016 = utils.utc_datetime(2016, 2, 1)
    data = fetcher._read_pb_files([filepath], dummy_pv, feb_2016, jan_2018, None)
    assert data.values[0] == [0]
    assert data.timestamps[0] == 1453202116.1772349


def test_PbFileFetcher_read_pb_files_omits_subsequent_event(dummy_pv, jan_2001):
    filepath = testutils.get_data_filepath('jan_2016.pb')
    root = 'root'
    fetcher = pb.PbFileFetcher(root)
    dec_2015 = utils.utc_datetime(2015, 12, 1)
    data = fetcher._read_pb_files([filepath], dummy_pv, jan_2001, dec_2015, None)
    assert len(data) == 0

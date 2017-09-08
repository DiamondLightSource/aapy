from aa import pb
import pytest
import mock
import os
from curses import ascii


TIMESTAMP_2001 = 978307200
TIMESTAMP_INACCURACY = 1e-6


def test_unescape_line_does_not_change_regular_bytes():
    test_bytes = b'hello:-1|bye'
    assert pb.unescape_bytes(test_bytes) == test_bytes


def test_unescape_bytes_handles_example_escaped_bytes():
    test_bytes = b'hello' + pb.ESC_BYTE + b'\x02' + b'bye'
    assert pb.unescape_bytes(test_bytes) == b'hello' + pb.NL_BYTE + b'bye'


@pytest.mark.parametrize('year,timestamp', ((1970, 0), (2001, TIMESTAMP_2001)))
def test_year_timestamp_gives_correct_answer(year, timestamp):
    assert pb.year_timestamp(year) == timestamp


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


def test_binary_search_raises_IndexError_for_empty_seq():
    with pytest.raises(IndexError):
        pb.binary_search([], lambda x: x, 1) == -1


def test_binary_search_returns_zero_if_target_below_range():
    f = lambda x: x
    assert pb.binary_search([2, 3, 4], f, 1) == 0


def test_binary_search_returns_len_seq_if_target_above_range():
    f = lambda x: x
    assert pb.binary_search([1, 2, 3], f, 4) == 3


def test_binary_search_returns_lower_index():
    f = lambda x: x
    assert pb.binary_search([1, 2], f, 1.5) == 0


def test_binary_search_returns_index_if_value_equals_item_in_seq():
    f = lambda x: x
    assert pb.binary_search([1, 2], f, 1) == 0


def test_PbFileFetcher_get_pb_file_handles_pv_with_one_colon():
    root = 'root'
    year = 2001
    pv = 'a-b-c:d'
    fetcher = pb.PbFileFetcher(root)
    expected = os.path.join('root', 'a', 'b', 'c', 'd:2001.pb')
    assert fetcher._get_pb_file(pv, year) == expected


def test_PbFileFetcher_get_pb_file_handles_pv_with_two_colon():
    root = 'root'
    year = 2001
    pv = 'a-b-c:d:e'
    fetcher = pb.PbFileFetcher(root)
    expected = os.path.join('root', 'a', 'b', 'c', 'd', 'e:2001.pb')
    assert fetcher._get_pb_file(pv, year) == expected

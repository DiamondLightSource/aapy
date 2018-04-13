from aa import utils
from datetime import datetime
import pytz
import pytest
import tzlocal


TZ_BST = pytz.timezone('Europe/London')
UNIX_TIME = 1504023969
JUST_AFTER_EPOCH = datetime(1970, 1, 1, 0, 0, 15, tzinfo=pytz.UTC)
DATETIME_UTC = pytz.UTC.localize(datetime(2017, 8, 29, 16, 26, 9))
DATETIME_BST = TZ_BST.localize(datetime(2017, 8, 29, 17, 26, 9))
TIMESTAMP_2001 = 978307200


def test_utc_datetime_correctly_creates_datetime():
    utc_2001 = utils.utc_datetime(2001, 1, 1)
    assert utc_2001.year == 2001
    assert utc_2001.month == 1
    assert utc_2001.day == 1
    assert utc_2001.tzinfo == pytz.UTC


def test_datetime_to_epoch_works_for_short_difference():
    assert utils.datetime_to_epoch(JUST_AFTER_EPOCH) == 15


def test_datetime_to_epoch_works_for_current_datetime_utc():
    assert utils.datetime_to_epoch(DATETIME_UTC) == UNIX_TIME


def test_datetime_to_epoch_works_for_current_datetime_bst():
    assert utils.datetime_to_epoch(DATETIME_BST) == UNIX_TIME


def test_epoch_to_datetime_works_for_short_difference():
    assert utils.epoch_to_datetime(15) == JUST_AFTER_EPOCH


def test_epoch_to_datetime_works_for_current_datetime_utc():
    assert utils.epoch_to_datetime(UNIX_TIME) == DATETIME_UTC


def test_add_local_timezone_adds_timezone():
    dt = utils.add_local_timezone(datetime.now())
    localtz = tzlocal.get_localzone()
    assert dt.tzinfo == localtz


@pytest.mark.parametrize('year,timestamp', ((1970, 0), (2001, TIMESTAMP_2001)))
def test_year_timestamp_gives_correct_answer(year, timestamp):
    assert utils.year_timestamp(year) == timestamp


def test_binary_search_returns_zero_for_empty_seq():
    assert utils.binary_search([], lambda x: x, 1) == 0


def test_binary_search_returns_zero_if_target_below_range():
    f = lambda x: x
    assert utils.binary_search([2, 3, 4], f, 1) == 0


def test_binary_search_returns_len_seq_if_target_above_range():
    f = lambda x: x
    assert utils.binary_search([1, 2, 3], f, 4) == 3


def test_binary_search_returns_upper_index():
    f = lambda x: x
    assert utils.binary_search([1, 2], f, 1.5) == 1


def test_binary_search_returns_upper_index_for_larger_sequence():
    # This tests an otherwise-untested block in the algorithm.
    f = lambda x: x
    assert utils.binary_search([1, 2, 3, 4], f, 3.5) == 3


def test_binary_search_returns_index_plus_one_if_value_equals_item_in_seq():
    f = lambda x: x
    assert utils.binary_search([1, 2], f, 1) == 1


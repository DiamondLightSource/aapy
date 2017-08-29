from aa import utils
from datetime import datetime
import pytz


TZ_BST = pytz.timezone('Europe/London')
UNIX_TIME = 1504023969
JUST_AFTER_EPOCH = datetime(1970, 1, 1, 0, 0, 15, tzinfo=pytz.UTC)
DATETIME_UTC = pytz.UTC.localize(datetime(2017, 8, 29, 16, 26, 9))
DATETIME_BST = TZ_BST.localize(datetime(2017, 8, 29, 17, 26, 9))


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

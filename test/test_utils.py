from aa import utils
from datetime import datetime
import pytz
import numpy
import pytest


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


def test_concatenate_returns_first_item_for_sequence_of_length_one():
    array = numpy.zeros((1,))
    data = utils.ArchiveData('dummy', array, array, array)
    output = utils.concatenate([data])
    assert output == data


def test_concatenate_works_for_two_items():
    zeros = numpy.zeros((1,))
    ones = numpy.ones((1,))
    data1 = utils.ArchiveData('dummy', zeros, zeros, zeros)
    data2 = utils.ArchiveData('dummy', ones, ones, ones)
    expected = numpy.array((0,1))
    output = utils.concatenate([data1, data2])
    numpy.testing.assert_equal(output.values, expected)
    numpy.testing.assert_equal(output.timestamps, expected)
    numpy.testing.assert_equal(output.severities, expected)


def test_concatenate_works_for_2d_arrays():
    zeros = numpy.zeros((1,))
    zeros_2d = numpy.zeros((1,2))
    ones = numpy.ones((1,))
    ones_2d = numpy.ones((1,2))
    data1 = utils.ArchiveData('dummy', zeros_2d, zeros, zeros)
    data2 = utils.ArchiveData('dummy', ones_2d, ones, ones)
    expected = numpy.array((0,1))
    expected_2d = numpy.array(((0,0),(1,1)))
    output = utils.concatenate([data1, data2])
    numpy.testing.assert_equal(output.values, expected_2d)
    numpy.testing.assert_equal(output.timestamps, expected)
    numpy.testing.assert_equal(output.severities, expected)


def test_concatenate_with_different_pv_names_raises_AssertionError():
    array = numpy.zeros((1,))
    data1 = utils.ArchiveData('dummy1', array, array, array)
    data2 = utils.ArchiveData('dummy2', array, array, array)
    with pytest.raises(AssertionError):
        utils.concatenate([data1, data2])

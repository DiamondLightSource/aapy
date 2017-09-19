import numpy
import pytest
from aa import data


def test_ArchiveData_append_works_for_two_items():
    zeros = numpy.zeros((1,))
    ones = numpy.ones((1,))
    data1 = data.ArchiveData('dummy', zeros, zeros, zeros)
    data2 = data.ArchiveData('dummy', ones, ones, ones)
    expected = numpy.array((0,1))
    data1.append(data2)
    numpy.testing.assert_equal(data1.values, expected)
    numpy.testing.assert_equal(data1.timestamps, expected)
    numpy.testing.assert_equal(data1.severities, expected)


def test_ArchiveData_append_works_for_2d_arrays():
    zeros = numpy.zeros((1,))
    zeros_2d = numpy.zeros((1,2))
    ones = numpy.ones((1,))
    ones_2d = numpy.ones((1,2))
    data1 = data.ArchiveData('dummy', zeros_2d, zeros, zeros)
    data2 = data.ArchiveData('dummy', ones_2d, ones, ones)
    expected = numpy.array((0,1))
    expected_2d = numpy.array(((0,0),(1,1)))
    data1.append(data2)
    numpy.testing.assert_equal(data1.values, expected_2d)
    numpy.testing.assert_equal(data1.timestamps, expected)
    numpy.testing.assert_equal(data1.severities, expected)


def test_ArchiveData_append_with_different_pv_names_raises_AssertionError():
    array = numpy.zeros((1,))
    data1 = data.ArchiveData('dummy1', array, array, array)
    data2 = data.ArchiveData('dummy2', array, array, array)
    with pytest.raises(AssertionError):
        data1.append(data2)


def test_empty_ArchiveData_iterates_zero_times():
    array = numpy.zeros((0,))
    empty_data = data.ArchiveData('dummy', array, array, array)
    for _ in empty_data:
        assert False


def test_empty_ArchiveData_iterates_multiple_times():
    array = numpy.zeros((1,))
    zero_event = data.ArchiveEvent('dummy', array, array, array)
    for i in range(3):
        array = numpy.zeros((3,))
        d = data.ArchiveData('dummy', array, array, array)
        for event in d:
            assert event == zero_event

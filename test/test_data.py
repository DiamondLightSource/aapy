import numpy
import pytest
from aa import data


def test_ArchiveData_concatenate_with_different_pv_names_raises_AssertionError():
    array = numpy.zeros((1,))
    data1 = data.ArchiveData('dummy1', array, array, array)
    data2 = data.ArchiveData('dummy2', array, array, array)
    with pytest.raises(AssertionError):
        data1.concatenate(data2)


def test_ArchiveData_concatenate_raises_ValueError_for_different_sized_array(data_1d, data_2d):
    with pytest.raises(ValueError):
        # default is zero_pad=False and the arrays are different dimensions
        data_2d.concatenate(data_1d)


def test_ArchiveData_concatenate_correctly_zero_pads(data_1d, data_2d):
    result = data_2d.concatenate(data_1d, zero_pad=True)
    expected = numpy.array(((1.1, 2, 3), (1, 0, 0)))
    numpy.testing.assert_equal(result.values, expected)


def test_ArchiveData_concatenate_works_for_two_items():
    zeros = numpy.zeros((1,))
    ones = numpy.ones((1,))
    data1 = data.ArchiveData('dummy', zeros, zeros, zeros)
    data2 = data.ArchiveData('dummy', ones, ones, ones)
    expected = numpy.array((0, 1))
    data3 = data1.concatenate(data2)
    # Note that values is always a 2d array.
    numpy.testing.assert_equal(data3.values, expected.reshape(2, 1))
    numpy.testing.assert_equal(data3.timestamps, expected)
    numpy.testing.assert_equal(data3.severities, expected)


def test_ArchiveData_concatenate_works_for_2d_arrays():
    zeros = numpy.zeros((1,))
    zeros_2d = numpy.zeros((1,2))
    ones = numpy.ones((1,))
    ones_2d = numpy.ones((1,2))
    data1 = data.ArchiveData('dummy', zeros_2d, zeros, zeros)
    data2 = data.ArchiveData('dummy', ones_2d, ones, ones)
    expected = numpy.array((0,1))
    expected_2d = numpy.array(((0,0),(1,1)))
    data3 = data1.concatenate(data2)
    numpy.testing.assert_equal(data3.values, expected_2d)
    numpy.testing.assert_equal(data3.timestamps, expected)
    numpy.testing.assert_equal(data3.severities, expected)


def test_ArchiveData_constructor_raises_AssertionError_if_array_lengths_different(dummy_pv):
    empty_10 = numpy.zeros((10,))
    empty_11 = numpy.zeros((11,))
    with pytest.raises(AssertionError):
        data.ArchiveData(dummy_pv, empty_10, empty_10, empty_11)


def test_ArchiveData_constructor_raises_AssertionError_if_timestamps_descending(dummy_pv):
    empty_array = numpy.zeros((10,))
    desc = numpy.arange(2, 1, -0.1)
    with pytest.raises(AssertionError):
        data.ArchiveData(dummy_pv, empty_array, desc, empty_array)


# Test both ascending array and constant array.
@pytest.mark.parametrize('timestamps', (numpy.arange(1, 2, 0.1), numpy.ones(10,)))
def test_ArchiveData_constructor_raises_no_exception_if_timestamps_valid(dummy_pv, timestamps):
    empty_array = numpy.zeros((10,))
    data.ArchiveData(dummy_pv, empty_array, timestamps, empty_array)


def test_empty_ArchiveData_iterates_zero_times(empty_data):
    for _ in empty_data:
        assert False  # we shouldn't get here


def test_empty_ArchiveData_raises_IndexError_if_indexed(empty_data):
    with pytest.raises(IndexError):
        empty_data[0]


def test_indexing_ArchiveData(data_1d, event_1d):
    assert data_1d[0] == event_1d


def test_empty_ArchiveData_iterates_multiple_times():
    array = numpy.zeros((1,))
    zero_event = data.ArchiveEvent('dummy', array, array, array)
    for i in range(3):
        array = numpy.zeros((3,))
        d = data.ArchiveData('dummy', array, array, array)
        for event in d:
            assert event == zero_event


def test_data_from_events_returns_empty_data_if_no_events_provided(dummy_pv, empty_data):
    assert data.data_from_events(dummy_pv, []) == empty_data


def test_data_from_events_handles_one_event(dummy_pv, event_1d, data_1d):
    assert data.data_from_events(dummy_pv, (event_1d,)) == data_1d


def test_data_from_events_handles_two_events(dummy_pv, event_1d, event_1d_alt, data_2_events):
    result = data.data_from_events(dummy_pv, (event_1d, event_1d_alt))
    assert result == data_2_events


def test_data_from_events_handles_two_2d_events(dummy_pv, event_2d, event_2d_alt, data_2d_2_events):
    assert data.data_from_events(dummy_pv, (event_2d, event_2d_alt)) == data_2d_2_events

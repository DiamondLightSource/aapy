from aa import compare, data, utils
import numpy


ONE_EVENT_DATA = data.ArchiveData('dummy',
                                   numpy.array((1,)),
                                   numpy.array((2,)),
                                   numpy.array((1,)))
HIGH_SEV_DATA = data.ArchiveData('dummy',
                                  numpy.array((1,)),
                                  numpy.array((2,)),
                                  numpy.array((4,)))
TWO_EVENT_DATA = data.ArchiveData('dummy',
                                   numpy.array((1,2)),
                                   numpy.array((2,3)),
                                   numpy.array((1,0)))
DUPLICATE_DATA = data.ArchiveData('dummy',
                                   numpy.array((1,1)),
                                   numpy.array((2,2)),
                                   numpy.array((1,1)))
TWO_EVENT_WITH_DUPLICATE_DATA = data.ArchiveData('dummy',
                                                  numpy.array((1,1,2)),
                                                  numpy.array((2,2,3)),
                                                  numpy.array((1,1,0)))
EMPTY_DIFF = compare.ArchiveDataDiff()


def test_compare_archive_data_returns_empty_diff_with_one_event_on_each_side():
    diff = compare.compare_archive_data(ONE_EVENT_DATA, ONE_EVENT_DATA)
    assert diff == EMPTY_DIFF


def test_compare_archive_data_returns_empty_diff_with_two_events_on_each_side():
    diff = compare.compare_archive_data(TWO_EVENT_DATA, TWO_EVENT_DATA)
    assert diff == EMPTY_DIFF


def test_compare_archive_data_locates_high_sev_data():
    diff = compare.compare_archive_data(HIGH_SEV_DATA, HIGH_SEV_DATA)
    high_sev_event = data.ArchiveEvent('dummy', 1, 2, 4)
    assert diff._invalid[diff.LEFT] == [high_sev_event]
    assert diff._invalid[diff.RIGHT] == [high_sev_event]


def test_compare_archive_data_locates_duplicate_event():
    diff = compare.compare_archive_data(ONE_EVENT_DATA, DUPLICATE_DATA)
    assert diff._duplicates[diff.LEFT] == []
    assert diff._duplicates[diff.RIGHT] == [data.ArchiveEvent('dummy', 1, 2, 1)]
    assert diff._extra[diff.LEFT] == []
    assert diff._extra[diff.RIGHT] == []


def test_compare_archive_data_locates_duplicate_event_and_extra_event():
    diff = compare.compare_archive_data(TWO_EVENT_DATA, DUPLICATE_DATA)
    assert diff._duplicates[diff.LEFT] == []
    assert diff._duplicates[diff.RIGHT] == [data.ArchiveEvent('dummy', 1, 2, 1)]
    assert diff._extra[diff.LEFT] == [data.ArchiveEvent('dummy', 2, 3, 0)]
    assert diff._extra[diff.RIGHT] == []


def test_compare_archive_data_matches_two_events_with_one_duplicate():
    diff = compare.compare_archive_data(TWO_EVENT_WITH_DUPLICATE_DATA, TWO_EVENT_DATA)
    assert diff._duplicates[diff.LEFT] == [data.ArchiveEvent('dummy', 1, 2, 1)]
    assert not diff._duplicates[diff.RIGHT]
    assert not diff._extra[diff.LEFT]
    assert not diff._extra[diff.RIGHT]

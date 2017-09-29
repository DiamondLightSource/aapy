from aa import data
import pytest
import numpy
import pytz
from datetime import datetime


@pytest.fixture
def dummy_pv():
    return 'dummy'


@pytest.fixture
def jan_2017():
    return pytz.utc.localize(datetime(2017, 1, 1))



@pytest.fixture
def event_1d(dummy_pv):
    return data.ArchiveEvent(dummy_pv, 1, 100.1, 0)


@pytest.fixture
def event_1d_alt(dummy_pv):
    return data.ArchiveEvent(dummy_pv, 2, 100.2, 1)


@pytest.fixture
def event_2d(dummy_pv):
    return data.ArchiveEvent(dummy_pv, numpy.array((1.1, 2, 3)), 10.21, 1)


@pytest.fixture
def event_2d_alt(dummy_pv):
    return data.ArchiveEvent(dummy_pv, numpy.array((3, 4.5, 6)), 11.01, 5)


@pytest.fixture
def empty_data(dummy_pv):
    return data.ArchiveData.empty(dummy_pv)


@pytest.fixture
def data_1d(dummy_pv):
    return data.ArchiveData(dummy_pv,
                            numpy.array((1,)),
                            numpy.array((100.1,)),
                            numpy.array((0,)))


@pytest.fixture
def data_2_events(dummy_pv):
    """event_1d and event_1d_alt concatenated.
    Returns:
        1d ArchiveData with two events.
    """
    return data.ArchiveData(dummy_pv,
                            numpy.array((1, 2)),
                            numpy.array((100.1, 100.2)),
                            numpy.array((0, 1)))


@pytest.fixture
def data_2d_2_events(dummy_pv):
    """event_2d and event_2d_alt concatenated.
    Returns:
        2d ArchiveData with two events.
    """
    return data.ArchiveData(dummy_pv,
                            numpy.array(((1.1, 2, 3), (3, 4.5, 6))),
                            numpy.array((10.21, 11.01)),
                            numpy.array((1, 5)))


@pytest.fixture
def event_1d(dummy_pv):
    return data.ArchiveEvent(dummy_pv, 1, 100.1, 0)


@pytest.fixture
def data_2d(dummy_pv):
    return data.ArchiveData(dummy_pv,
                            numpy.array(((1.1, 2, 3),)),
                            numpy.array((10.21,)),
                            numpy.array((1,)))

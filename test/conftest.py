from aa import data
import pytest
import numpy


@pytest.fixture
def dummy_pv():
    return 'dummy'


@pytest.fixture
def event_1d(dummy_pv):
    return data.ArchiveEvent(dummy_pv, 1, 100.1, 0)


@pytest.fixture
def event_2d(dummy_pv):
    return data.ArchiveEvent(dummy_pv, numpy.array((1, 2, 3)), 10.21, 1)


@pytest.fixture
def data_1d(dummy_pv):
    return data.ArchiveData(dummy_pv,
                            numpy.array((1,)),
                            numpy.array((100.1,)),
                            numpy.array((0,)))


@pytest.fixture
def data_2d(dummy_pv):
    return data.ArchiveData(dummy_pv,
                            numpy.array(((1, 2, 3),)),
                            numpy.array((10.21,)),
                            numpy.array((1,)))

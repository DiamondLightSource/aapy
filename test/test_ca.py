import pytest
import mock
from aa import ArchiveData
from aa import ca, utils
import numpy


DUMMY_PV = 'dummy'

EVENT_1D = {'value': [1],
            'secs': 100,
            'nano': 1e8,
            'sevr': 0
            }

DATA_1D = ArchiveData(DUMMY_PV,
                      numpy.array((1,)),
                      numpy.array((100.1,)),
                      numpy.array((0,)))

EVENT_2D = {'value': [1, 2, 3],
            'secs': 10,
            'nano': 21e7,
            'sevr': 1
            }

DATA_2D = ArchiveData(DUMMY_PV,
                      numpy.array(((1, 2, 3),)),
                      numpy.array((10.21,)),
                      numpy.array((1,)))


@pytest.fixture
def ca_fetcher():
    fetcher = ca.CaFetcher('http://url')
    fetcher._client = mock.MagicMock()
    return fetcher


def test_CaFetcher_process_raw_data_handles_1d_event(ca_fetcher):
    result = ca_fetcher._process_raw_data([EVENT_1D], DUMMY_PV)
    assert result == DATA_1D


def test_CaFetcher_process_raw_data_handles_2d_event(ca_fetcher):
    result = ca_fetcher._process_raw_data([EVENT_2D], DUMMY_PV)
    utils.assert_archive_data_equal(DATA_2D, result)


def test_CaFetcher_get_values_calls_client_get_once_if_response_less_than_count(ca_fetcher):
    # One value will be returned.
    ca_fetcher._client.get.return_value = [EVENT_1D]
    ca_fetcher._process_raw_data = mock.MagicMock()
    ca_fetcher._process_raw_data.return_value = DATA_1D
    # Ask for two values.
    response = ca_fetcher.get_values('dummy', 'dummy_date_1', 'dummy_date_2', 2)
    assert len(ca_fetcher._client.get.call_args_list) == 1
    assert response == DATA_1D

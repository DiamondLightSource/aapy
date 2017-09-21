import pytest
import mock
from aa import ca, data
import numpy


DUMMY_PV = 'dummy'

CA_EVENT_1D = {'value': [1],
               'secs': 100,
               'nano': 1e8,
               'sevr': 0
               }

EVENT_1D = data.ArchiveEvent(DUMMY_PV, 1, 100.1, 0)

DATA_1D = data.ArchiveData(DUMMY_PV,
                      numpy.array((1,)),
                      numpy.array((100.1,)),
                      numpy.array((0,)))

CA_EVENT_2D = {'value': [1, 2, 3],
               'secs': 10,
               'nano': 21e7,
               'sevr': 1
               }

DATA_2D = data.ArchiveData(DUMMY_PV,
                      numpy.array(((1, 2, 3),)),
                      numpy.array((10.21,)),
                      numpy.array((1,)))


@pytest.fixture
def ca_fetcher():
    fetcher = ca.CaFetcher('http://url')
    fetcher._client = mock.MagicMock()
    return fetcher


def test_CaFetcher_get_values_calls_client_get_once_if_response_less_than_count(ca_fetcher):
    # One value will be returned.
    ca_fetcher._client.get.return_value = [EVENT_1D]
    # Ask for two values.
    response = ca_fetcher.get_values('dummy', 'dummy_date_1', 'dummy_date_2', 2)
    assert len(ca_fetcher._client.get.call_args_list) == 1
    assert response == DATA_1D

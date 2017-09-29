from aa import fetcher, pb
from datetime import datetime
import pytest
import mock


DUMMY_PV = 'a-b:c'
EARLY_DATE = datetime(2001, 1, 1, 1, 1)
LATE_DATE = datetime(2010, 2, 3, 4, 5)


@pytest.fixture
def aa_fetcher():
    return fetcher.AaFetcher('localhost', '3003')


def test_Fetcher_get_values_raises_NotImplementedError():
    f = fetcher.Fetcher()
    with pytest.raises(NotImplementedError):
        f.get_values(1, 2, 3, 4)


def test_Fetcher_get_event_at_raises_ValueError_if_no_data_returned_by_query():
    f = fetcher.Fetcher()
    f.get_values = mock.MagicMock()
    f.get_values.side_effect = IndexError
    with pytest.raises(ValueError):
        f.get_event_at(1, 2)


def test_AaFetcher_constructs_endpoint_correctly(aa_fetcher):
    assert aa_fetcher._endpoint == 'http://localhost:3003'


def test_AaFetcher_format_datetime(aa_fetcher):
    expected = '2001-01-01T01:01:00Z'
    assert aa_fetcher._format_datetime(EARLY_DATE) == expected


def test_AaFetcher_constructs_url_correctly(aa_fetcher):
    aa_fetcher._url = 'dummy-url'
    constructed = aa_fetcher._construct_url(DUMMY_PV, EARLY_DATE, LATE_DATE)
    expected = 'dummy-url?pv=a-b%3Ac&from=2001-01-01T01%3A01%3A00Z&to=2010-02-03T04%3A05%3A00Z'
    assert constructed == expected


def test_AaFetcher_creates_default_for_end_if_not_provided(aa_fetcher):
    dummy_datetime = datetime(2017, 1, 1)
    dummy_get_values = mock.MagicMock()
    aa_fetcher._get_values = dummy_get_values
    aa_fetcher.get_values(DUMMY_PV, dummy_datetime, end=None)
    args, _ = dummy_get_values.call_args
    assert args[0] == DUMMY_PV
    assert args[1] == dummy_datetime
    assert isinstance(args[2], datetime)


def test_Fetcher_parse_raw_data_raises_NotImplementedError(aa_fetcher):
    with pytest.raises(NotImplementedError):
        aa_fetcher._parse_raw_data(1, 2, 3, 4, 5)

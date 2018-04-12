from aa import fetcher
from datetime import datetime
import mock
import pytest
import pytz


EARLY_DATE = datetime(2001, 1, 1, 1, 1)
LATE_DATE = datetime(2010, 2, 3, 4, 5)


@pytest.fixture
def aa_fetcher():
    return fetcher.AaFetcher('localhost', '3003')


def test_Fetcher_get_values_raises_NotImplementedError(dummy_pv):
    f = fetcher.Fetcher()
    with pytest.raises(NotImplementedError):
        f.get_values(dummy_pv, datetime.now(), datetime.now(), 4)


def test_Fetcher_get_event_at_raises_ValueError_if_no_data_returned_by_query():
    f = fetcher.Fetcher()
    f.get_values = mock.MagicMock()
    f.get_values.side_effect = IndexError
    with pytest.raises(ValueError):
        f.get_event_at(1, datetime.now())


def test_AaFetcher_constructs_endpoint_correctly(aa_fetcher):
    assert aa_fetcher._endpoint == 'http://localhost:3003'


def test_AaFetcher_format_datetime(aa_fetcher):
    expected = '2001-01-01T01:01:00Z'
    assert aa_fetcher._format_datetime(EARLY_DATE) == expected


def test_AaFetcher_constructs_url_correctly(dummy_pv, aa_fetcher):
    aa_fetcher._url = 'dummy-url'
    constructed = aa_fetcher._construct_url(dummy_pv, EARLY_DATE, LATE_DATE)
    expected = 'dummy-url?pv=dummy&from=2001-01-01T01:01:00Z&to=2010-02-03T04:05:00Z'
    assert constructed == expected


def test_AaFetcher_creates_default_for_end_if_not_provided(dummy_pv, aa_fetcher):
    dummy_datetime = datetime(2017, 1, 1, tzinfo=pytz.UTC)
    dummy_get_values = mock.MagicMock()
    aa_fetcher._get_values = dummy_get_values
    aa_fetcher.get_values(dummy_pv, dummy_datetime, end=None)
    args, _ = dummy_get_values.call_args
    assert args[0] == dummy_pv
    assert args[1] == dummy_datetime
    assert isinstance(args[2], datetime)

def test_AaFetcher_converts_to_UTC_if_no_timezone(dummy_pv, aa_fetcher):
    dummy_datetime = datetime(2017, 1, 1)
    utc_dummy_datetime = dummy_datetime.replace(tzinfo=pytz.UTC)
    dummy_get_values = mock.MagicMock()
    aa_fetcher._get_values = dummy_get_values
    aa_fetcher.get_values(dummy_pv, dummy_datetime, end=None)
    args, _ = dummy_get_values.call_args
    assert args[0] == dummy_pv
    assert args[1] == utc_dummy_datetime
    assert isinstance(args[2], datetime)

def test_AaFetcher_get_values_raises_NotImplementedError(dummy_pv, jan_2018, aa_fetcher):
    with mock.patch('requests.get'):
        with pytest.raises(NotImplementedError):
            aa_fetcher.get_values(dummy_pv, jan_2018)

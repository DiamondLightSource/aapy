from aa import jfetcher, fetcher, pbfetcher
from datetime import datetime
import pytest
import mock


DUMMY_PV = 'a-b:c'
EARLY_DATE = datetime(2001, 1, 1, 1, 1)
LATE_DATE = datetime(2010, 2, 3, 4, 5)

@pytest.fixture
def aa_fetcher():
    return fetcher.AaFetcher('localhost', '3003')


def test_AaFetcher_constructs_endpoint_correctly(aa_fetcher):
    assert aa_fetcher._endpoint == 'http://localhost:3003'


def test_AaFetcher_format_date(aa_fetcher):
    expected = '2001-01-01T01:01:00Z'
    assert aa_fetcher._format_date(EARLY_DATE) == expected


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


def test_JsonFetcher_constructs_url_correctly():
    j = jfetcher.JsonFetcher('localhost', 5000)
    assert j._url == 'http://localhost:5000/retrieval/data/getData.json'

from aa import jfetcher, fetcher
from datetime import datetime
import pytest


@pytest.fixture
def aa_fetcher():
    return fetcher.AaFetcher('localhost', '3003')


def test_AaFetcher_constructs_endpoint_correctly(aa_fetcher):
    assert aa_fetcher._endpoint == 'http://localhost:3003'


def test_AaFetcher_format_date(aa_fetcher):
    date = datetime(2001, 1, 1, 1, 1)
    expected = '2001-01-01T01:01:00Z'
    assert aa_fetcher._format_date(date) == expected


def test_AaFetcher_constructs_url_correctly(aa_fetcher):
    pv = 'a-b:c'
    start_date = datetime(2001, 1, 1, 1, 1)
    end_date = datetime(2010, 2, 3, 4, 5)
    aa_fetcher._url = 'dummy-url'
    constructed = aa_fetcher._construct_url(pv, start_date, end_date)
    expected = 'dummy-url?pv=a-b%3Ac&from=2001-01-01T01%3A01%3A00Z&to=2010-02-03T04%3A05%3A00Z'
    assert constructed == expected


def test_JsonFetcher_constructs_url_correctly():
    j = jfetcher.JsonFetcher('localhost', 5000)
    assert j._url == 'http://localhost:5000/retrieval/data/getData.json'

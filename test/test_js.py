from aa import js
from datetime import datetime
import numpy
import mock
import pytest


JSON_DEMO = """
[{
  "meta": {},
  "data":
  [
    {
      "secs": 1502963093,
      "nanos": 123,
      "val": 1.23,
      "severity": 1
    }
  ]
}]
"""

DUMMY_PV = 'a-b:c'
EARLY_DATE = datetime(2001, 1, 1, 1, 1)
LATE_DATE = datetime(2010, 2, 3, 4, 5)
EMPTY_ARRAY = numpy.array((0,))


@pytest.fixture
def json_fetcher():
    return js.JsonFetcher('localhost', 5000)


def test_JsonFetcher_constructs_url_correctly(json_fetcher):
    assert json_fetcher._url == 'http://localhost:5000/retrieval/data/getData.json'


def test_JsonFetcher_decodes_empty_json_correctly(json_fetcher):
    json_fetcher._fetch_data = mock.MagicMock(return_value='[]')
    aa_data = json_fetcher.get_values(DUMMY_PV, EARLY_DATE, LATE_DATE)
    assert aa_data.pv == DUMMY_PV
    numpy.testing.assert_equal(aa_data.values, EMPTY_ARRAY)
    numpy.testing.assert_equal(aa_data.timestamps, EMPTY_ARRAY)
    numpy.testing.assert_equal(aa_data.severities, EMPTY_ARRAY)


def test_JsonFetcher_decodes_json_correctly(json_fetcher):
    json_fetcher._fetch_data = mock.MagicMock(return_value=JSON_DEMO)
    aa_data = json_fetcher.get_values(DUMMY_PV, EARLY_DATE, LATE_DATE)
    assert aa_data.pv == DUMMY_PV
    assert aa_data.values[0] == 1.23
    assert aa_data.timestamps[0] == 1502963093.000000123
    assert aa_data.severities[0] == 1

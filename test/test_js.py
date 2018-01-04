from aa import js
from datetime import datetime
import numpy
import mock
import pytest
import os


EARLY_DATE = datetime(2001, 1, 1, 1, 1)
LATE_DATE = datetime(2010, 2, 3, 4, 5)
EMPTY_ARRAY = numpy.array((0,))


def load_from_file(filename):
    """
    Load the contents of a file in the data directory.
    Args:
        filename: name of file to load

    Returns:
        contents of file as a string

    """
    filepath = os.path.join(os.path.dirname(__file__), 'data', filename)
    with open(filepath) as f:
        return f.read()


@pytest.fixture
def json_fetcher():
    return js.JsonFetcher('localhost', 5000)


def test_JsonFetcher_constructs_url_correctly(json_fetcher):
    assert json_fetcher._url == 'http://localhost:5000/retrieval/data/getData.json'


def test_JsonFetcher_decodes_empty_json_correctly(dummy_pv, empty_data, json_fetcher):
    json_fetcher._fetch_data = mock.MagicMock(return_value='[]')
    aa_data = json_fetcher.get_values(dummy_pv, EARLY_DATE, LATE_DATE)
    assert aa_data == empty_data


def test_JsonFetcher_decodes_single_event_correctly(dummy_pv, json_fetcher):
    event_json = load_from_file('event.json')
    json_fetcher._fetch_data = mock.MagicMock(return_value=event_json)
    aa_data = json_fetcher.get_values(dummy_pv, EARLY_DATE, LATE_DATE)
    assert aa_data.pv == dummy_pv
    assert aa_data.values[0] == 1.23
    assert aa_data.timestamps[0] == 1502963093.000000123
    assert aa_data.severities[0] == 1


def test_JsonFetcher_decodes_waveform_events_correctly(dummy_pv, json_fetcher, data_2d_2_events):
    waveform_json = load_from_file('waveform.json')
    json_fetcher._fetch_data = mock.MagicMock(return_value=waveform_json)
    aa_data = json_fetcher.get_values(dummy_pv, EARLY_DATE, LATE_DATE)
    assert aa_data == data_2d_2_events

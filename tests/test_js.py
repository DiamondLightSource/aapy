from datetime import datetime
import json

import jsonschema
import mock
import numpy
import pytest
import utils

from aa import js


EARLY_DATE = datetime(2001, 1, 1, 1, 1)
LATE_DATE = datetime(2010, 2, 3, 4, 5)
EMPTY_ARRAY = numpy.array((0,))


@pytest.fixture
def json_fetcher():
    return js.JsonFetcher("localhost", 5000)


@pytest.mark.parametrize("filename", ["event", "string_event", "waveform"])
def test_json_matches_schema(filename):
    schema_string = utils.load_from_file("aa_schema.json")
    schema_obj = json.loads(schema_string)
    json_string = utils.load_from_file(filename + ".json")
    json_obj = json.loads(json_string)
    jsonschema.validate(json_obj, schema_obj)


def test_JsonFetcher_constructs_url_correctly(json_fetcher):
    assert json_fetcher._url == "http://localhost:5000/retrieval/data/getData.json"


def test_JsonFetcher_decodes_empty_json_correctly(dummy_pv, empty_data, json_fetcher):
    mock_response = utils.mock_response(json_str="[]")
    json_fetcher._fetch_data = mock_response
    aa_data = json_fetcher.get_values(dummy_pv, EARLY_DATE, LATE_DATE)
    assert aa_data == empty_data


def test_JsonFetcher_decodes_single_event_correctly(dummy_pv, json_fetcher):
    event_json = utils.load_from_file("event.json")
    mock_response = utils.mock_response(json_str=event_json)
    json_fetcher._fetch_data = mock.MagicMock(return_value=mock_response)
    aa_data = json_fetcher.get_values(dummy_pv, EARLY_DATE, LATE_DATE)
    assert aa_data.pv == dummy_pv
    assert aa_data.values[0] == 1.23
    assert aa_data.timestamps[0] == 1502963093.000000123
    assert aa_data.severities[0] == 1


def test_JsonFetcher_decodes_string_event_correctly(dummy_pv, json_fetcher):
    event_json = utils.load_from_file("string_event.json")
    mock_response = utils.mock_response(json_str=event_json)
    json_fetcher._fetch_data = mock.MagicMock(return_value=mock_response)
    aa_data = json_fetcher.get_values(dummy_pv, EARLY_DATE, LATE_DATE)
    print(aa_data)
    assert aa_data.pv == dummy_pv
    assert aa_data.values[0] == "2015-01-08 19:47:01 UTC"
    assert aa_data.timestamps[0] == 1507712433.235971000
    assert aa_data.severities[0] == 0


def test_JsonFetcher_decodes_waveform_events_correctly(
    dummy_pv, json_fetcher, data_2d_2_events
):
    waveform_json = utils.load_from_file("waveform.json")
    mock_response = utils.mock_response(json_str=waveform_json)
    json_fetcher._fetch_data = mock.MagicMock(return_value=mock_response)
    aa_data = json_fetcher.get_values(dummy_pv, EARLY_DATE, LATE_DATE)
    assert aa_data == data_2d_2_events

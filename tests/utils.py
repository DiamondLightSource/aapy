import json
import os

import mock


def mock_response(json_str=None, raw=None):
    resp = mock.MagicMock()
    if json_str is not None:
        loaded_json = json.loads(json_str)
        resp.json = mock.MagicMock(return_value=loaded_json)
    if raw is not None:
        resp.raw = mock.MagicMock()
        resp.raw.read = mock.MagicMock(return_value=raw)
    return resp


def get_data_filepath(filename):
    """Construct filepath for a file in the test/data directory
    Args:
        filename: name of file

    Returns:
        full path to file

    """
    return os.path.join(os.path.dirname(__file__), "data", filename)


def load_from_file(filename):
    """Load the contents of a file in the data directory.
    Args:
        filename: name of file to load

    Returns:
        contents of file as a string

    """
    filepath = get_data_filepath(filename)
    with open(filepath) as f:
        return f.read()

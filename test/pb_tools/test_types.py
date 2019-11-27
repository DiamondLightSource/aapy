from cothread.catools import dbr

from aa.pb_tools import types
from aa import epics_event_pb2 as ee

def test_ca_to_pb_type_gives_correct_output():
    first_test = dbr.ca_str()
    first_test.datatype = dbr.DBR_STRING

    result = types.ca_to_pb_type(first_test)

    assert result == ee.ScalarString
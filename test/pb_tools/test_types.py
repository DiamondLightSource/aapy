from pytest import mark
from cothread.catools import dbr

from aa.pb_tools import types
from aa import epics_event_pb2 as ee

@mark.parametrize("ca_type,dbr_type,pb_type",[
    (dbr.ca_str,   dbr.DBR_STRING, ee.ScalarString),
    (dbr.ca_int,   dbr.DBR_SHORT,  ee.ScalarShort),
    (dbr.ca_int,   dbr.DBR_LONG,   ee.ScalarInt),
    (dbr.ca_float, dbr.DBR_FLOAT,  ee.ScalarFloat),
    (dbr.ca_float, dbr.DBR_DOUBLE, ee.ScalarDouble),
])
def test_ca_to_pb_type_gives_correct_output(ca_type, dbr_type, pb_type):
    test_ca_obj = ca_type()
    test_ca_obj.datatype = dbr_type

    result = types.ca_to_pb_type(test_ca_obj)

    assert result == pb_type
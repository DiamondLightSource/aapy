from pytest import mark
from cothread.catools import dbr

from aa.pb_tools import types
from aa import epics_event_pb2 as ee


SCALAR_TYPE_MAP = [
    (dbr.ca_str,   dbr.DBR_STRING, ee.ScalarString),
    (dbr.ca_int,   dbr.DBR_SHORT,  ee.ScalarShort),
    (dbr.ca_int,   dbr.DBR_LONG,   ee.ScalarInt),
    (dbr.ca_float, dbr.DBR_FLOAT,  ee.ScalarFloat),
    (dbr.ca_float, dbr.DBR_DOUBLE, ee.ScalarDouble),
    (dbr.ca_int,   dbr.DBR_CHAR,   ee.ScalarByte),
]


VECTOR_TYPE_MAP = [
    (dbr.ca_array, dbr.DBR_STRING, ee.VectorString),
    (dbr.ca_array, dbr.DBR_SHORT,  ee.VectorShort),
    (dbr.ca_array, dbr.DBR_LONG,   ee.VectorInt),
    (dbr.ca_array, dbr.DBR_FLOAT,  ee.VectorFloat),
    (dbr.ca_array, dbr.DBR_DOUBLE, ee.VectorDouble),
    (dbr.ca_array, dbr.DBR_CHAR,   ee.VectorChar),
]


@mark.parametrize("ca_type,dbr_type,pb_type", SCALAR_TYPE_MAP)
def test_ca_to_pb_type_gives_correct_scalar_output(ca_type, dbr_type, pb_type):
    test_ca_obj = ca_type()
    test_ca_obj.datatype = dbr_type

    result = types.ca_to_pb_type(test_ca_obj)

    assert result == pb_type


@mark.parametrize("ca_type,dbr_type,pb_type", VECTOR_TYPE_MAP)
def test_ca_to_pb_type_gives_correct_vector_output(ca_type, dbr_type, pb_type):
    test_ca_obj = ca_type([0,1,2,3])
    test_ca_obj.datatype = dbr_type

    result = types.ca_to_pb_type(test_ca_obj)

    assert result == pb_type


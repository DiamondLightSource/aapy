from pytest import mark, raises
import mock
from cothread import dbr
from cothread.catools import ca_nothing

from aa.pb_tools import types
from aa import epics_event_pb2 as ee
from aa import pb


SCALAR_TYPE_MAP = [
    (dbr.ca_str,   dbr.DBR_STRING, pb.INVERSE_TYPE_MAPPINGS[ee.ScalarString]),
    (dbr.ca_int,   dbr.DBR_SHORT,  pb.INVERSE_TYPE_MAPPINGS[ee.ScalarShort]),
    (dbr.ca_int,   dbr.DBR_LONG,   pb.INVERSE_TYPE_MAPPINGS[ee.ScalarInt]),
    (dbr.ca_float, dbr.DBR_FLOAT,  pb.INVERSE_TYPE_MAPPINGS[ee.ScalarFloat]),
    (dbr.ca_float, dbr.DBR_DOUBLE, pb.INVERSE_TYPE_MAPPINGS[ee.ScalarDouble]),
    (dbr.ca_int,   dbr.DBR_CHAR,   pb.INVERSE_TYPE_MAPPINGS[ee.ScalarByte]),
]


VECTOR_TYPE_MAP = [
    (dbr.ca_array, dbr.DBR_STRING, pb.INVERSE_TYPE_MAPPINGS[ee.VectorString]),
    (dbr.ca_array, dbr.DBR_SHORT,  pb.INVERSE_TYPE_MAPPINGS[ee.VectorShort]),
    (dbr.ca_array, dbr.DBR_LONG,   pb.INVERSE_TYPE_MAPPINGS[ee.VectorInt]),
    (dbr.ca_array, dbr.DBR_FLOAT,  pb.INVERSE_TYPE_MAPPINGS[ee.VectorFloat]),
    (dbr.ca_array, dbr.DBR_DOUBLE, pb.INVERSE_TYPE_MAPPINGS[ee.VectorDouble]),
    (dbr.ca_array, dbr.DBR_CHAR,   pb.INVERSE_TYPE_MAPPINGS[ee.VectorChar]),
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


@mark.parametrize("ca_type,dbr_type,pb_type", SCALAR_TYPE_MAP)
@mock.patch("aa.pb_tools.types.caget")
def test_get_type_of_live_pv_gives_expected_for_scalars(mock_caget,
       ca_type, dbr_type, pb_type):

    mock_caget.return_value=ca_type()
    mock_caget.return_value.datatype = dbr_type
    assert types.get_pb_type_of_live_pv("FAKE-PV") == pb_type


@mark.parametrize("ca_type,dbr_type,pb_type", VECTOR_TYPE_MAP)
@mock.patch("aa.pb_tools.types.caget")
def test_get_type_of_live_pv_gives_expected_for_vectors(mock_caget,
       ca_type, dbr_type, pb_type):

    mock_caget.return_value=dbr.ca_array([0,1,2])
    mock_caget.return_value.datatype = dbr_type
    assert types.get_pb_type_of_live_pv("FAKE-PV") == pb_type


@mock.patch("aa.pb_tools.types.caget")
def test_get_type_of_live_pv_throws_ValueError_if_pv_not_found(mock_caget):
    mock_caget.return_value=ca_nothing("FAKE-PV")
    with raises(ValueError):
        types.get_pb_type_of_live_pv("FAKE-PV")

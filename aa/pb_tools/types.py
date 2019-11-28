"""
Mapping between Cothread DBR types and PB types, which I attempted to copy from
epicsarchiverap Java source:
src/main/edu/stanford/slac/archiverappliance/PB/data/DBR2PBTypeMapping.java

typemap.put(ArchDBRTypes.DBR_SCALAR_STRING, new DBR2PBTypeMapping(PBScalarString.class));
typemap.put(ArchDBRTypes.DBR_SCALAR_SHORT, new DBR2PBTypeMapping(PBScalarShort.class));
typemap.put(ArchDBRTypes.DBR_SCALAR_FLOAT, new DBR2PBTypeMapping(PBScalarFloat.class));
typemap.put(ArchDBRTypes.DBR_SCALAR_ENUM, new DBR2PBTypeMapping(PBScalarEnum.class));
typemap.put(ArchDBRTypes.DBR_SCALAR_BYTE, new DBR2PBTypeMapping(PBScalarByte.class));
typemap.put(ArchDBRTypes.DBR_SCALAR_INT, new DBR2PBTypeMapping(PBScalarInt.class));
typemap.put(ArchDBRTypes.DBR_SCALAR_DOUBLE, new DBR2PBTypeMapping(PBScalarDouble.class));
typemap.put(ArchDBRTypes.DBR_WAVEFORM_STRING, new DBR2PBTypeMapping(PBVectorString.class));
typemap.put(ArchDBRTypes.DBR_WAVEFORM_SHORT, new DBR2PBTypeMapping(PBVectorShort.class));
typemap.put(ArchDBRTypes.DBR_WAVEFORM_FLOAT, new DBR2PBTypeMapping(PBVectorFloat.class));
typemap.put(ArchDBRTypes.DBR_WAVEFORM_ENUM, new DBR2PBTypeMapping(PBVectorEnum.class));
typemap.put(ArchDBRTypes.DBR_WAVEFORM_BYTE, new DBR2PBTypeMapping(PBVectorByte.class));
typemap.put(ArchDBRTypes.DBR_WAVEFORM_INT, new DBR2PBTypeMapping(PBVectorInt.class));
typemap.put(ArchDBRTypes.DBR_WAVEFORM_DOUBLE, new DBR2PBTypeMapping(PBVectorDouble.class));
typemap.put(ArchDBRTypes.DBR_V4_GENERIC_BYTES, new DBR2PBTypeMapping(PBV4GenericBytes.class));
"""
from collections import namedtuple

from cothread.catools import caget
from cothread import dbr

from aa import epics_event_pb2 as ee


TypeMapping = namedtuple("TypeMapping",
                         "ca_type dbr_type pb_scalar_type pb_vector_type")


COTHREAD_TYPE_MAPPING = [
    TypeMapping(
        ca_type=dbr.ca_str,
        dbr_type=[dbr.DBR_STRING, dbr.DBR_CHAR_STR],
        pb_scalar_type=ee.ScalarString,
        pb_vector_type=ee.VectorString
    ),
    TypeMapping(
        ca_type=dbr.ca_int,
        dbr_type=[dbr.DBR_SHORT],
        pb_scalar_type=ee.ScalarShort,
        pb_vector_type=ee.VectorShort
    ),
    TypeMapping(
        ca_type=dbr.ca_float,
        dbr_type=[dbr.DBR_FLOAT],
        pb_scalar_type=ee.ScalarFloat,
        pb_vector_type=ee.VectorFloat
    ),
    TypeMapping(
        ca_type=dbr.ca_int,
        dbr_type=[dbr.DBR_ENUM],
        pb_scalar_type=ee.ScalarEnum,
        pb_vector_type=ee.VectorEnum
    ),
    TypeMapping(
        ca_type=dbr.ca_int,
        dbr_type=[dbr.DBR_CHAR],
        pb_scalar_type=ee.ScalarByte, # Not precise match
        pb_vector_type=ee.VectorChar
    ),
    TypeMapping(
        ca_type=dbr.ca_int,
        dbr_type=[dbr.DBR_LONG], # Not precise match ?
        pb_scalar_type=ee.ScalarInt,
        pb_vector_type=ee.VectorInt
    ),
    TypeMapping(
        ca_type=dbr.ca_float,
        dbr_type=[dbr.DBR_DOUBLE],
        pb_scalar_type=ee.ScalarDouble,
        pb_vector_type=ee.VectorDouble
    ),
]


def ca_to_pb_type(ca_value):
    """
    Deduce the protobuf type from a channel access return value from cothread.
    We search in COTHREAD_TYPE_MAPPING, and find the appropriate scalar or
    vector type.

    Args:
        ca_value: The return value from a caget.

    Returns:
        The type from list defined in epics_event_pb2

    Raises:
        ValueError if the lookup failed.
    """
    try:
        data_type = ca_value.datatype
    except AttributeError:
        raise ValueError("Did not get PV value")

    pb_type = None
    for mapping in COTHREAD_TYPE_MAPPING:
        if data_type in mapping.dbr_type:
            if isinstance(ca_value, dbr.ca_array):
                pb_type = mapping.pb_vector_type
            elif isinstance(ca_value, mapping.ca_type):
                pb_type = mapping.pb_scalar_type
            else:
                raise ValueError("Couldn't map CA type to PB type")
    return pb_type


def get_pb_type_of_live_pv(pv_name):
    """
    Do a caget on the named PV, and from the result look up the PB type
    that it corresponds to.

    Args:
        pv_name: Name of the PV to get

    Returns:
        he type from list defined in epics_event_pb2

    Raises:
        ValueError if the caget or lookup failed.
    """
    result = caget(pv_name, timeout=0.1)
    return ca_to_pb_type(result)

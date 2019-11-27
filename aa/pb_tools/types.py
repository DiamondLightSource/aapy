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

from cothread.catools import caget, dbr

from aa import epics_event_pb2 as ee

TypeMapping = namedtuple("TypeMapping",
                         "dbr_type pb_scalar_type pb_vector_type")


COTHREAD_TYPE_MAPPING = [
    TypeMapping(
        dbr_type=[dbr.DBR_STRING, dbr.DBR_CHAR_STR],
        pb_scalar_type=ee.ScalarString,
        pb_vector_type=ee.VectorString
    ),
    TypeMapping(
        dbr_type=[dbr.DBR_SHORT],
        pb_scalar_type=ee.ScalarShort,
        pb_vector_type=ee.VectorShort
    ),
    TypeMapping(
        dbr_type=[dbr.DBR_FLOAT],
        pb_scalar_type=ee.ScalarFloat,
        pb_vector_type=ee.VectorFloat
    ),
    TypeMapping(
        dbr_type=[dbr.DBR_ENUM],
        pb_scalar_type=ee.ScalarEnum,
        pb_vector_type=ee.VectorEnum
    ),
    TypeMapping(
        dbr_type=[dbr.DBR_CHAR],
        pb_scalar_type=ee.ScalarByte, # Not precise match
        pb_vector_type=ee.VectorChar
    ),
    TypeMapping(
        dbr_type=[dbr.DBR_LONG], # Not precise match ?
        pb_scalar_type=ee.ScalarInt,
        pb_vector_type=ee.VectorInt
    ),
    TypeMapping(
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
        ca_value:

    Returns:

    """
    data_type = ca_value.datatype
    pb_type = None
    for mapping in COTHREAD_TYPE_MAPPING:
        if data_type in mapping.dbr_type:
            if isinstance(ca_value, dbr.ca_array):
                pb_type = mapping.pb_vector_type
            else:
                pb_type = mapping.pb_scalar_type
    return pb_type


def get_type_of_live_pv(pv_name):
    result = caget(pv_name, timeout=0.1)
    return result.datatype

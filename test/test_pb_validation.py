import os

from aa.pb_tools import validation
from aa import epics_event_pb2 as ee
import utils as testutils


def test_one_chunk_from_raw():
    full_path = testutils.get_data_filepath('wrong_type.pb')
    with open(full_path, "rb") as raw_file:
        raw_data = raw_file.read()

    result = validation.one_chunk_from_raw(raw_data)

    comparison_header = ee.PayloadInfo(
        year=2017,
        type=6,
        pvname="BL14J-PS-SHTR-03:OPS",
        elementCount=1,
    )

    result_header, result_chunk = result

    # Get correct header info and number of lines
    assert result_header == comparison_header
    assert len(result_chunk) == 11


def test_raw_event_from_line():

    line = b"\x08\x96\xfb\xc6\x0c\x10\xd0\xf3\x8f\xb3\x01\x1d\x18\x00\x00" \
           b"\x00:\x12\x1b\x02\x1b\x03cnxlostepsecs\x12\x010:\x1f\x1b\x02" \
           b"\x11cnxregainedepsecs\x12\x1b\x021509557270:\x0f\x1b\x02\x07" \
           b"startup\x12\x04true"

    expected = ee.ScalarInt(
        secondsintoyear=26328470,
        nano=375650768,
        val=24
    )
    expected.fieldvalues.add(name="cnxlostepsecs", val="0")
    expected.fieldvalues.add(name="cnxregainedepsecs", val="1509557270")
    expected.fieldvalues.add(name="startup", val="true")

    result = validation.raw_event_from_line(line, 5)

    assert result == expected


def test_basic_data_checks():

    events = [
        None,
        ee.ScalarInt(secondsintoyear=123, nano=4),
        ee.ScalarInt(secondsintoyear=123, nano=4, val=1),
        ee.ScalarInt(secondsintoyear=122, nano=3, val=2)
    ]

    # Expect:
    # No event at 0
    # No value at 1
    # Timestamp duplicated at 2
    # Event out of order at 3

    expected_errors = [
        (0, validation.PbError.EVENT_NOT_DECODED),
        (1, validation.PbError.EVENT_MISSING_VALUE),
        (2, validation.PbError.EVENT_DUPLICATED),
        (3, validation.PbError.EVENT_OUT_OF_ORDER),
    ]

    payload_info = ee.PayloadInfo(
        year=2017,
        type=5,
        pvname="BL14J-PS-SHTR-03:OPS",
        elementCount=1,
    )

    result = validation.basic_data_checks(payload_info, events)

    assert result == expected_errors

    # With no header attached

    result = validation.basic_data_checks(None, events)

    assert result == [(None, validation.PbError.HEADER_NOT_DECODED)]


def test_writing_then_reading_file_gives_same_data():

    f = validation.PbFile()

    f.payload_info = ee.PayloadInfo(
        year=2017,
        type=5,
        pvname="BL14J-PS-SHTR-03:OPS",
        elementCount=1,
    )

    f.pb_events = [
        ee.ScalarInt(secondsintoyear=123, nano=4, val=123),
        ee.ScalarInt(secondsintoyear=123, nano=4, val=1),
        ee.ScalarInt(secondsintoyear=456, nano=3, val=56)
    ]

    filepath = testutils.get_data_filepath("tmp.pb")

    f.serialize_to_raw_lines()
    f.write_raw_lines_to_file(filepath)

    g = validation.PbFile(filepath)

    assert g.payload_info == f.payload_info
    assert g.raw_lines == f.raw_lines

    g.decode_raw_lines()
    assert g.pb_events == f.pb_events

    os.remove(filepath)
from aa import pb_validation
from aa import epics_event_pb2 as ee

import utils as testutils

def test_one_chunk_from_raw():
    full_path = testutils.get_data_filepath('wrong_type.pb')
    with open(full_path, "rb") as raw_file:
        raw_data = raw_file.read()

    result = pb_validation.one_chunk_from_raw(raw_data)

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

    result = pb_validation.raw_event_from_line(line, 5)

    assert result == expected

def test_basic_data_checks():

    events = [
        None,
        ee.ScalarInt(secondsintoyear=123, nano=4),
        ee.ScalarInt(secondsintoyear=122, nano=3, val=1),
        ee.ScalarInt(secondsintoyear=122, nano=3, val=2)
    ]

    # Expect:
    # No event at 0
    # No value at 1
    # Timestamp out of order at 2
    # Duplicated timestamp at 3

    expected_errors = [
        (0, pb_validation.PbError.EVENT_NOT_DECODED),
        (1, pb_validation.PbError.EVENT_MISSING_VALUE),
        (2, pb_validation.PbError.EVENT_OUT_OF_ORDER),
        (3, pb_validation.PbError.EVENT_DUPLICATED),
    ]

    header = ee.PayloadInfo(
        year=2017,
        type=6,
        pvname="BL14J-PS-SHTR-03:OPS",
        elementCount=1,
    )

    result = pb_validation.basic_data_checks(events, header)


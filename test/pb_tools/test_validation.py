from aa import epics_event_pb2 as ee
from aa.pb_tools import validation

def test_basic_data_checks():

    events = [
        None,
        ee.ScalarInt(secondsintoyear=123, nano=4),
        ee.ScalarInt(secondsintoyear=123, nano=4, val=1),
        ee.ScalarInt(secondsintoyear=122, nano=3, val=2),
        ee.ScalarInt(secondsintoyear=122, nano=3, val=2),
    ]

    # Expect:
    # No event at 0
    # No value at 1
    # Timestamp duplicated at 2
    # Event out of order at 3

    expected_errors = [
        (0, validation.PbError.EVENT_NOT_DECODED),
        (1, validation.PbError.EVENT_MISSING_VALUE),
        (2, validation.PbError.EVENTS_SHARE_TIMESTAMP),
        (3, validation.PbError.EVENT_OUT_OF_ORDER),
        (4, validation.PbError.EVENT_DUPLICATED),
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


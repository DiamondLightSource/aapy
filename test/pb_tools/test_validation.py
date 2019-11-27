from pytest import fixture
from aa import epics_event_pb2 as ee
from aa.pb_tools import validation


#######################################################
# Fixtures
#######################################################
@fixture()
def test_events():
    events = [
        None,
        ee.ScalarInt(secondsintoyear=123, nano=4),
        ee.ScalarInt(secondsintoyear=123, nano=4, val=1),
        ee.ScalarInt(secondsintoyear=122, nano=3, val=2),
        ee.ScalarInt(secondsintoyear=122, nano=3, val=2),
    ]
    return events

@fixture()
def payload_info():
    return ee.PayloadInfo(
        year=2017,
        type=5,
        pvname="BL14J-PS-SHTR-03:OPS",
        elementCount=1,
    )


#######################################################

def test_basic_data_checks(test_events, payload_info):

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
    result = validation.basic_data_checks(payload_info, test_events)
    assert result == expected_errors

    # With no header attached
    result = validation.basic_data_checks(None, test_events)
    assert result == [(None, validation.PbError.HEADER_NOT_DECODED)]


def test_basic_data_checks_with_lazy_returns_after_first(test_events,
                                                         payload_info):


    result = validation.basic_data_checks(payload_info, test_events, lazy=True)
    expected_errors = [
        (0, validation.PbError.EVENT_NOT_DECODED),
    ]
    assert result == expected_errors


def test_basic_data_checks_with_only_check_returns_only_requested(test_events,
                                                                  payload_info):
    """
    Test different variations of "only_check" mask to make sure we
    get the right subset of error sreported
    """

    mask = validation.PbError.ALL

    result = validation.basic_data_checks(payload_info, test_events,
                                          lazy=False, only_check=mask)
    expected_errors = [
        (0, validation.PbError.EVENT_NOT_DECODED),
        (1, validation.PbError.EVENT_MISSING_VALUE),
        (2, validation.PbError.EVENTS_SHARE_TIMESTAMP),
        (3, validation.PbError.EVENT_OUT_OF_ORDER),
        (4, validation.PbError.EVENT_DUPLICATED),
    ]
    assert result == expected_errors

    mask = validation.PbError.EVENT_OUT_OF_ORDER
    result = validation.basic_data_checks(payload_info, test_events,
                                          lazy=False, only_check=mask)
    expected_errors = [
        (3, validation.PbError.EVENT_OUT_OF_ORDER),
    ]
    assert result == expected_errors

    mask = validation.PbError.EVENT_MISSING_VALUE \
           | validation.PbError.EVENT_DUPLICATED
    result = validation.basic_data_checks(payload_info, test_events,
                                          lazy=False, only_check=mask)
    expected_errors = [
        (1, validation.PbError.EVENT_MISSING_VALUE),
        (4, validation.PbError.EVENT_DUPLICATED),
    ]
    assert result == expected_errors
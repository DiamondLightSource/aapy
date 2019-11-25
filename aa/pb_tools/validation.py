"""
Tools for validating and fixing protobuf files
"""
import logging
from enum import Enum

from aa import pb
from aa import epics_event_pb2 as ee

# A logger for this module
MODULE_LOGGER = logging.getLogger("{}".format(__name__))


class PbError(Enum):
    """Different error conditions that can occur in a PB file"""
    HEADER_NOT_DECODED = 0
    EVENT_NOT_DECODED = 1
    EVENT_MISSING_VALUE = 2
    EVENT_MISSING_TIMESTAMP = 3
    EVENT_OUT_OF_ORDER = 4
    EVENTS_SHARE_TIMESTAMP = 5
    EVENT_DUPLICATED = 6


PB_ERROR_STRINGS = {
    PbError.HEADER_NOT_DECODED: "Header not decoded",
    PbError.EVENT_NOT_DECODED: "Event not decoded",
    PbError.EVENT_MISSING_VALUE: "Event missing value",
    PbError.EVENT_MISSING_TIMESTAMP: "Event missing timestamp",
    PbError.EVENT_OUT_OF_ORDER: "Event out of order",
    PbError.EVENTS_SHARE_TIMESTAMP: "Multiple events sharing timestamp",
    PbError.EVENT_DUPLICATED: "Event duplicated",
}



def log_parsing_error(index, error_type):
    """Lookup a parsing error by type and issue a log message with the
    corresponding string"""
    error_string = PB_ERROR_STRINGS[error_type]
    MODULE_LOGGER.info(f"{error_string} at index {index}")


def basic_data_checks(payload_info: ee.PayloadInfo, pb_events: list,
                      lazy=False):
    """
    Run some basic checks on PB file events

    Args:
        payload_info: PayloadInfo for the events
        pb_events: List of PB event objects
        lazy: return as soon as first error is found (saves time
              parsing big files with lots of errors)

    Returns:
        List of tuples giving index and error type
    """

    if not isinstance(payload_info, ee.PayloadInfo):
        errors = [(None, PbError.HEADER_NOT_DECODED)]
        return errors

    year = payload_info.year
    list_of_events = pb_events

    index = 0
    prev_event = None
    errors = []
    for event in list_of_events:
        # Check event has been decoded; if not, indicates e.g. corruption
        if event is None:
            log_parsing_error(index, PbError.EVENT_NOT_DECODED)
            errors.append((index, PbError.EVENT_NOT_DECODED))
        else:
            # Check val field was populated
            # If not, indicates e.g. wrong type
            if not event.HasField("val"):
                log_parsing_error(index, PbError.EVENT_MISSING_VALUE)
                errors.append((index, PbError.EVENT_MISSING_VALUE))
            timestamp = pb.event_timestamp(year, event)

            # Following checks not valid on first event
            if prev_event is not None:
                prev_timestamp = pb.event_timestamp(year, prev_event)
                # Check timestamps monotonically increasing
                if timestamp < prev_timestamp:
                    log_parsing_error(index, PbError.EVENT_OUT_OF_ORDER)
                    errors.append((index, PbError.EVENT_OUT_OF_ORDER))
                elif timestamp == prev_timestamp:
                    # All fields of this event match the previous one
                    if event == prev_event:
                        log_parsing_error(index, PbError.EVENT_DUPLICATED)
                        errors.append((index, PbError.EVENT_DUPLICATED))
                    # Only timestamp duplicated
                    else:
                        log_parsing_error(index,
                                          PbError.EVENTS_SHARE_TIMESTAMP)
                        errors.append((index,
                                       PbError.EVENTS_SHARE_TIMESTAMP))

        prev_event = event
        if lazy and len(errors) > 0:
            break
        index += 1
    return errors

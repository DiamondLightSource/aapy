"""
Tools for validating and fixing protobuf files
"""
import logging
from enum import Flag, auto

from aa import pb
from aa import epics_event_pb2 as ee

# A logger for this module
MODULE_LOGGER = logging.getLogger("{}".format(__name__))

    
class PbError(Flag):
    """Different error conditions that can occur in a PB file"""
    HEADER_NOT_DECODED = auto()
    EVENT_NOT_DECODED = auto()
    EVENT_MISSING_VALUE = auto()
    EVENT_MISSING_TIMESTAMP = auto()
    EVENT_OUT_OF_ORDER = auto()
    EVENTS_SHARE_TIMESTAMP = auto()
    EVENT_DUPLICATED = auto()
    ALL = HEADER_NOT_DECODED | EVENT_NOT_DECODED | EVENT_MISSING_VALUE \
          | EVENT_MISSING_TIMESTAMP | EVENT_OUT_OF_ORDER \
          | EVENTS_SHARE_TIMESTAMP | EVENT_DUPLICATED


PB_ERROR_STRINGS = {
    PbError.HEADER_NOT_DECODED: "Header not decoded",
    PbError.EVENT_NOT_DECODED: "Event not decoded",
    PbError.EVENT_MISSING_VALUE: "Event missing value",
    PbError.EVENT_MISSING_TIMESTAMP: "Event missing timestamp",
    PbError.EVENT_OUT_OF_ORDER: "Event out of order",
    PbError.EVENTS_SHARE_TIMESTAMP: "Multiple events sharing timestamp",
    PbError.EVENT_DUPLICATED: "Event duplicated",
    PbError.ALL: "All errror types"
}

def check_single_event(event):
    """
    Run the checks which are possible on a single event without reference
    to others.

    Args:
        payload_info: The PayloadInfo for the PB file
        event: The PB event object itself

    Returns:
        List of error types found
    """
    errors = set()

    if event is None:
        errors.add(PbError.EVENT_NOT_DECODED)
    else:
        # Check val field was populated
        # If not, indicates e.g. wrong type
        if not event.HasField("val"):
            errors.add(PbError.EVENT_MISSING_VALUE)
        if not event.HasField("secondsintoyear"):
            errors.add(PbError.EVENT_MISSING_TIMESTAMP)

    return errors


def basic_data_checks(payload_info: ee.PayloadInfo, pb_events: list,
                      lazy=False, only_check=PbError.ALL):
    """
    Run some basic checks on PB file events

    Args:
        payload_info: PayloadInfo for the events
        pb_events: List of PB event objects
        lazy: return as soon as first error is found (saves time
              parsing big files with lots of errors)
        only_check: Only check for these types of error
                        You can "or" each of the PbError flags to combine.

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
            errors = record_error(index, PbError.EVENT_NOT_DECODED,
                                  only_check, errors)
        else:
            # Check val field was populated
            # If not, indicates e.g. wrong type
            if not event.HasField("val"):
                errors = record_error(index, PbError.EVENT_MISSING_VALUE,
                                      only_check, errors)

            timestamp = pb.event_timestamp(year, event)

            # Following checks not valid on first event
            if prev_event is not None:
                prev_timestamp = pb.event_timestamp(year, prev_event)
                # Check timestamps monotonically increasing
                if timestamp < prev_timestamp:
                    errors = record_error(index, PbError.EVENT_OUT_OF_ORDER,
                                          only_check, errors)
                elif timestamp == prev_timestamp:
                    # All fields of this event match the previous one
                    if event == prev_event:
                        errors = record_error(index, PbError.EVENT_DUPLICATED,
                                              only_check, errors)
                    # Only timestamp duplicated
                    else:
                        errors = record_error(
                            index, PbError.EVENTS_SHARE_TIMESTAMP,
                            only_check, errors,
                        )

        prev_event = event
        if lazy and len(errors) > 0:
            break
        index += 1
    return errors


def record_error(index: int, error_type: PbError, only_check: Flag,
                 errors: list):
    """
    Log a parsing error and append it to the given list.
    Note that irrespective of only_check, we always record HEADER_NOT_DECODED
    because it's a fundamental problem with the file that breaks everything.

    Args:
        index: The event index into file where it occurred
        error_type: Error type from PbError
        only_check: Flags for which error types to check
        errors: The existing list of errors to append to

    Returns:

    """
    if error_type & only_check or error_type == PbError.HEADER_NOT_DECODED:
        error_string = PB_ERROR_STRINGS[error_type]
        MODULE_LOGGER.info(f"{error_string} at index {index}")

        errors.append(
            (index, error_type)
        )

    return errors



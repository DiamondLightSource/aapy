import logging
from enum import Enum

from google.protobuf.message import DecodeError

import pb


# A logger for this module
log = logging.getLogger("{}".format(__name__))


def raw_event_from_line(line, event_type):
    """
    Get a protocol buffer event object for the raw data from the given line

    Args:
        line: (str) raw data from one line
        event_type: (int) a key for a type from TYPE_MAPPINGS

    Returns:
        A protocol buffer event object
    """
    unescaped = pb.unescape_bytes(line)
    event = pb.TYPE_MAPPINGS[event_type]()
    try:
        event.ParseFromString(unescaped)
    except DecodeError:
        # Decode failed
        event = None

    return event


def raw_events_from_chunk(raw_chunk, requested_type=None):
    """
    Get a list of protocol buffer event objects from the supplied raw data

    Args:
        raw_data: Raw data from a PB file
        requested_type: A key from TYPE_MAPPINGS to use for the data type
                        in decoding events; if None, we try to use the
                        one from the header

    Returns:
        List of protocol buffer event objects
    """
    chunk_info, lines = raw_chunk
    year = chunk_info.year
    raw_events = []
    if requested_type is not None:
        # Use the requested type if given
        decode_type = requested_type
    else:
        # Use the type from the header
        decode_type = chunk_info.type
    for line in lines:
        raw_events.append(raw_event_from_line(line, decode_type))
    return year, chunk_info, raw_events


def event_has_value(event):
    return event.HasField("val")


def one_chunk_from_raw(raw_data):
    chunks = pb.break_up_chunks(raw_data)
    if len(chunks) > 0:
        log.warning(
            "This data has multiple chunks, can only handle "
            "the first one at the moment."
        )
    return chunks.items()[0][1]


class PbError(Enum):
    HEADER_NOT_DECODED = 0
    EVENT_NOT_DECODED = 1
    EVENT_MISSING_VALUE = 2
    EVENT_MISSING_TIMESTAMP = 3
    EVENT_OUT_OF_ORDER = 4
    EVENT_DUPLICATED = 5


class PbFile:
    def __init__(self):
        self.raw_chunk = None
        self.raw_events = None
        self.header = None
        self.year = None
        self.archive_events = None

    def populate_from_file(self, full_path):
        # Read raw data from file
        with open(full_path, "rb") as raw_file:
            self.raw_data = raw_file.read()
        # Extract a chunk from the raw data
        self.raw_chunk = one_chunk_from_raw(self.raw_data)
        print(self.raw_chunk)
        # Interpret this to get year, header, and protobuf event objects
        self.year, self.header, self.raw_events = raw_events_from_chunk(
            self.raw_chunk
        )


def basic_data_checks(raw_events, header):

    year = header.year
    list_of_events = raw_events

    index = 0
    prev_timestamp = 0
    errors = []
    for event in list_of_events:
        #print(event)
        if event is None:
            log.warning("No event at index {}".format(index))
            errors.append((index, PbError.EVENT_NOT_DECODED))
        else:
            if not event.HasField("val"):
                log.warning("No value on event {}".format(index))
                errors.append((index, PbError.EVENT_MISSING_VALUE))
            timestamp = pb.event_timestamp(year, event)
            if timestamp <= prev_timestamp:
                log.warning("Timestamp out of order at {}".format(index))
                errors.append((index, PbError.EVENT_OUT_OF_ORDER))

        index += 1
    return errors

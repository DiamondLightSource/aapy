import logging
import typing
from enum import Enum

from google.protobuf.message import DecodeError

from . import pb
from . import epics_event_pb2 as ee


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
    if len(chunks) > 1:
        log.warning(
            "This data has multiple chunks, can only handle "
            "the first one at the moment."
        )
    return list(chunks.items())[0][1]

    
class PbError(Enum):
    HEADER_NOT_DECODED = 0
    EVENT_NOT_DECODED = 1
    EVENT_MISSING_VALUE = 2
    EVENT_MISSING_TIMESTAMP = 3
    EVENT_OUT_OF_ORDER = 4
    EVENT_DUPLICATED = 5


def encode_events_to_chunk(header: ee.PayloadInfo, events: list):
    unescaped_lines = []

    # Encode header
    unescaped_lines.append(
        header.SerializeToString()
    )

    for event in events:
        unescaped_lines.append(
            event.SerializeToString()
        )

    escaped_lines = []
    for line in unescaped_lines:
        escaped_lines.append(pb.escape_bytes(line) + b"\n")
    return escaped_lines


class PbFile:
    """
    Hold all the data associated with a PB file and its interpretation.
    Methods are mostly wrappers for static functions in this file that act
    on member variables.
    """

    def __init__(self, filename=None):
        self.empty()

        # Initialize if we are given a filename
        if filename:
            self.populate_from_file(filename)

    def empty(self):
        self.raw_chunk = None
        self.raw_events = None
        self.header = None
        self.year = None
        self.archive_events = None
        self.errors = None

    def raw_events_from_chunk(self, requested_type=None):
        """Interpret the raw chunk into raw events, optionally specifying a type to use"""
        self.year, self.header, self.raw_events = raw_events_from_chunk(
            self.raw_chunk, requested_type
        )

    def populate_from_file(self, full_path):
        # Read raw data from file
        with open(full_path, "rb") as raw_file:
            self.raw_data = raw_file.read()
        # Extract a chunk from the raw data
        self.raw_chunk = one_chunk_from_raw(self.raw_data)
        # Interpret this to get year, header, and protobuf event objects
        self.raw_events_from_chunk()

    def do_checks(self):
        """Run checks on data and populate list of errors"""
        self.errors = basic_data_checks(self.raw_events, self.header)

    def encode_chunk(self):
        pass

    def write_chunk_to_file(self, output_file_path):

        lines_to_write = encode_events_to_chunk(self.header, self.raw_events)

        with open(output_file_path, "wb") as output_file:
            output_file.writelines(lines_to_write)


def basic_data_checks(raw_events, header):


    if not type(header) is ee.PayloadInfo:
        errors = [(None, PbError.HEADER_NOT_DECODED)]
        return errors

    year = header.year
    list_of_events = raw_events

    index = 0
    prev_timestamp = 0
    errors = []
    for event in list_of_events:
        # Check event has been decoded; if not, indicates e.g. corruption
        if event is None:
            log.warning("No event at index {}".format(index))
            errors.append((index, PbError.EVENT_NOT_DECODED))
        else:

            # Check val field was populated
            # If not, indicates e.g. wrong type
            if not event.HasField("val"):
                log.warning("No value on event at index {}".format(index))
                errors.append((index, PbError.EVENT_MISSING_VALUE))
            timestamp = pb.event_timestamp(year, event)

            # Check timestamps monotonically increasing
            if timestamp < prev_timestamp:
                log.warning("Timestamp out of order at index {}".format(index))
                errors.append((index, PbError.EVENT_OUT_OF_ORDER))
            elif timestamp == prev_timestamp:
                log.warning("Duplicated timestamp at index {}".format(index))
                errors.append((index, PbError.EVENT_DUPLICATED))
            else:
                prev_timestamp = timestamp

        index += 1
    return errors

"""
Tools for validating and fixing protobuf files
"""
import logging
from enum import Enum

from google.protobuf.message import DecodeError

from aa import pb
from aa import epics_event_pb2 as ee


# A logger for this module
LOG = logging.getLogger("{}".format(__name__))


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


def pb_events_from_raw_lines(raw_chunk, requested_type=None):
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
    raw_events = []

    for line in raw_chunk:
        raw_events.append(raw_event_from_line(line, requested_type))
    return raw_events


def one_chunk_from_raw(raw_data):
    """
    Get one chunk only from raw data bytes
    Args:
        raw_data: Raw bytes

    Returns:

    """
    chunks = pb.break_up_chunks(raw_data)
    if len(chunks) > 1:
        LOG.warning(
            "This data has multiple chunks, can only handle "
            "the first one at the moment."
        )
    return list(chunks.items())[0][1]


class PbError(Enum):
    """Different error conditions that can occur in a PB file"""
    HEADER_NOT_DECODED = 0
    EVENT_NOT_DECODED = 1
    EVENT_MISSING_VALUE = 2
    EVENT_MISSING_TIMESTAMP = 3
    EVENT_OUT_OF_ORDER = 4
    EVENT_DUPLICATED = 5


PB_ERROR_STRINGS = {
    PbError.HEADER_NOT_DECODED: "Header not decoded",
    PbError.EVENT_NOT_DECODED: "Event not decoded",
    PbError.EVENT_MISSING_VALUE: "Event missing value",
    PbError.EVENT_MISSING_TIMESTAMP: "Event missing timestamp",
    PbError.EVENT_OUT_OF_ORDER: "Event out of order",
    PbError.EVENT_DUPLICATED: "Event duplicated",
}


def serialize_payload_info_to_raw(payload_info: ee.PayloadInfo):
    """
    Serialize a PayloadInfo into a raw line (without terminating newline
    Args:
        payload_info: a PayloadInfo object e.g. from a PbFile

    Returns:
        Raw line of serialized data
    """
    unescaped_bytes = payload_info.SerializeToString()
    escaped_bytes = pb.escape_bytes(unescaped_bytes)
    line = escaped_bytes
    return line


def serialize_events_to_raw_lines(pb_events):
    """
    Serialize a list of PB events into a list of raw lines
    Args:
        pb_events: List of PB event objects

    Returns:
        list of raw lines without terminators
    """

    # Serialize to unescaped bytes
    unescaped_lines = []
    for obj in pb_events:
        unescaped_lines.append(
            obj.SerializeToString()
        )

    # Escape each line
    escaped_lines = []
    for line in unescaped_lines:
        escaped_lines.append(pb.escape_bytes(line))
    return escaped_lines


def log_parsing_error(index, error_type):
    error_string = PB_ERROR_STRINGS[error_type]
    LOG.info(f"{error_string} at index {index}")


def basic_data_checks(payload_info: ee.PayloadInfo, pb_events: list):
    """
    Run some basic checks on PB events

    Args:
        payload_info: PayloadInfo for the events
        pb_events: List of PB event objects

    Returns:
        List of tuples giving index and error type
    """

    if not isinstance(payload_info, ee.PayloadInfo):
        errors = [(None, PbError.HEADER_NOT_DECODED)]
        return errors

    year = payload_info.year
    list_of_events = pb_events

    index = 0
    prev_timestamp = 0
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

            # Check timestamps monotonically increasing
            if timestamp < prev_timestamp:
                log_parsing_error(index, PbError.EVENT_OUT_OF_ORDER)
                errors.append((index, PbError.EVENT_OUT_OF_ORDER))
            elif timestamp == prev_timestamp:
                log_parsing_error(index, PbError.EVENT_DUPLICATED)
                errors.append((index, PbError.EVENT_DUPLICATED))
            else:
                prev_timestamp = timestamp

        index += 1
    return errors


class PbFile:
    """
    Hold all the data associated with a PB file and its interpretation.
    Methods are mostly wrappers for static functions in this file that act
    on member variables.
    """

    def __init__(self, filename=None):
        self.empty()

        # Read raw data if we are given a filename
        if filename:
            self.read_raw_lines_from_file(filename)

    def empty(self):
        """Initialize with an empty state"""
        self.raw_lines = None
        self.payload_info = None
        self.pb_events = None
        self.decoding_errors = None

    def decode_raw_lines(self, requested_type=None):
        """
        Decode raw lines into payload info and PB events.

        Args:
            requested_type: A key of pb.TYPE_MAPPINGS if a type other than
                            the one from the payload_info is to be used for
                            decoding the events. Otherwise the type from
                            payload_info is used.
        """
        if requested_type is not None:
            # Use the requested type if given
            decode_type = requested_type
        else:
            # Use the type from the header
            decode_type = self.payload_info.type

        self.pb_events = pb_events_from_raw_lines(
            self.raw_lines, decode_type
        )

    def read_raw_lines_from_file(self, full_path):
        """Read raw data from file"""
        with open(full_path, "rb") as raw_file:
            raw_data = raw_file.read()
        # Extract a chunk from the raw data
        self.payload_info, self.raw_lines = one_chunk_from_raw(raw_data)

    def check_data_for_errors(self):
        """Run checks on data and populate list of errors"""
        self.decoding_errors = basic_data_checks(
            self.payload_info,
            self.pb_events
        )

    def serialize_to_raw_lines(self):
        """Serialize events to raw lines"""
        self.raw_lines = serialize_events_to_raw_lines(
            self.pb_events
        )

    def write_raw_lines_to_file(self, output_file_path):
        """Write payload info and raw data to file"""
        raw_header = serialize_payload_info_to_raw(self.payload_info)
        bytes_to_write = raw_header + b"\n"

        for line in self.raw_lines:
            bytes_to_write += line + b"\n"

        with open(output_file_path, "wb") as output_file:
            output_file.write(bytes_to_write)

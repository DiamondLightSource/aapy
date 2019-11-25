"""A representation of a PB file allowing ease of loading, saving and
manipulating data."""
import logging

from google.protobuf.message import DecodeError

from aa import pb
from aa import epics_event_pb2 as ee
from aa.pb_tools import validation


module_logger = logging.getLogger(f"{__name__}")


class PbFile:
    """
    Hold all the data associated with a PB file and its interpretation.
    Methods are mostly wrappers for static functions in this file that act
    on member variables.
    """

    def __init__(self, filename=None):
        self.logger = logging.getLogger(f"{__name__}")
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

    def check_data_for_errors(self, lazy=False):
        """Run checks on data and populate list of errors"""
        self.decoding_errors = validation.basic_data_checks(
            self.payload_info,
            self.pb_events,
            lazy
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

    def all_events_equal_type(self):
        return all_events_equal_type(self.pb_events)


def all_events_equal_type(list_of_events: list):
    comparison_type = type(list_of_events[0])

    if len(list_of_events) < 1:
        module_logger.warning("all_events_equal_type got empty list")
        return True

    for event in list_of_events[1:]:
        if not isinstance(event, comparison_type):
            return False

    return True


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
        module_logger.warning(
            "This data has multiple chunks, can only handle "
            "the first one at the moment."
        )
    return list(chunks.items())[0][1]


def serialize_payload_info_to_raw(payload_info: ee.PayloadInfo):
    """
    Serialize a PayloadInfo into a raw line (without terminating newline)

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

    escaped_lines = []
    for obj in pb_events:
        escaped_lines.append(
            pb.escape_bytes(
                obj.SerializeToString()
            )
        )
    return escaped_lines

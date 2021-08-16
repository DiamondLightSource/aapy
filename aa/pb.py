"""Handle retrieval of data in the Archiver Appliance PB file format.

The file format is described on this page:
https://slacmshankar.github.io/epicsarchiver_docs/pb_pbraw.html

The data can be parsed in the same way whether retrieved using the
Rest API or whether reading files directly from disk. In either
case, it is important to treat the data as binary data - a stream of
bytes. The Google Protobuf library handles converting the stream of
bytes into the objects defined by the EPICSEvent.proto file.

The Archiver Appliance escapes certain characters as described on the
page above, which allows one to deduce the number of events in the
binary file using tools such as wc.

The unescape_bytes() method handles unescaping these characters before
handing the interpretation over to the Google Protobuf library.

Note: due to the way the protobuf objects are constructed, pylint can't
correctly deduce some properties, so I have manually disabled some warnings.

"""
from __future__ import annotations

import collections
import datetime
import glob
import logging as log
import os
import re
from typing import Any

import pytz
import requests

from . import data
from . import epics_event_pb2 as ee
from . import fetcher, utils

__all__ = [
    "unescape_bytes",
    "escape_bytes",
    "event_timestamp",
    "search_events",
    "break_up_chunks",
    "event_from_line",
    "parse_pb_data",
    "PbFetcher",
    "PbFileFetcher",
    "get_iso_timestamp_for_event",
]


# It is not clear to me why I can't extract this information
# from the compiled protobuf file.
TYPE_MAPPINGS = {
    0: ee.ScalarString,
    1: ee.ScalarShort,
    2: ee.ScalarFloat,
    3: ee.ScalarEnum,
    4: ee.ScalarByte,
    5: ee.ScalarInt,
    6: ee.ScalarDouble,
    7: ee.VectorString,
    8: ee.VectorShort,
    9: ee.VectorFloat,
    10: ee.VectorEnum,
    11: ee.VectorChar,
    12: ee.VectorInt,
    13: ee.VectorDouble,
    14: ee.V4GenericBytes,
}


INVERSE_TYPE_MAPPINGS = {cls: numeric for numeric, cls in TYPE_MAPPINGS.items()}


ESC_BYTE = b"\x1B"
NL_BYTE = b"\x0A"
CR_BYTE = b"\x0D"

# The character sequences required to unescape & escape AA pb file format.
# Note that we need to be careful about the ordering here. We must apply them
# in the opposite order when escaping and unescaping. In particular, the
# escape byte needs to be escaped *first* and unescaped *last* in order to
# prevent extra bytes appearing and causing problems. See #59.
PB_REPLACEMENTS_ESCAPING = collections.OrderedDict(
    [
        (ESC_BYTE + b"\x01", ESC_BYTE),
        (ESC_BYTE + b"\x02", NL_BYTE),
        (ESC_BYTE + b"\x03", CR_BYTE),
    ]
)
PB_REPLACEMENTS_UNESCAPING = collections.OrderedDict(
    [
        (ESC_BYTE + b"\x03", CR_BYTE),
        (ESC_BYTE + b"\x02", NL_BYTE),
        (ESC_BYTE + b"\x01", ESC_BYTE),
    ]
)


def unescape_bytes(byte_seq):
    """Replace specific sub-sequences in a bytes sequence.

    This escaping is defined as part of the Archiver Appliance raw file
    format: https://slacmshankar.github.io/epicsarchiver_docs/pb_pbraw.html

    Args:
        byte_seq: any byte sequence
    Returns:
        the byte sequence unescaped according to the AA file format rules
    """
    for key, value in PB_REPLACEMENTS_UNESCAPING.items():
        byte_seq = byte_seq.replace(key, value)
    return byte_seq


def escape_bytes(byte_seq):
    """Replace specific sub-sequences in a bytes sequence.

    This escaping is defined as part of the Archiver Appliance raw file
    format: https://slacmshankar.github.io/epicsarchiver_docs/pb_pbraw.html

    Args:
        byte_seq: any byte sequence
    Returns:
        the byte sequence escaped according to the AA file format rules
    """
    for key, value in PB_REPLACEMENTS_ESCAPING.items():
        byte_seq = byte_seq.replace(value, key)
    return byte_seq


def event_timestamp(year, event):
    year_start = utils.year_timestamp(year)
    # This will lose information (the last few decimal places) since
    # a double cannot store 18 significant figures.
    return year_start + event.secondsintoyear + 1e-9 * event.nano


def get_timestamp_from_line_function(chunk_info):
    def timestamp_from_line(line):
        event = TYPE_MAPPINGS[chunk_info.type]()
        event.ParseFromString(unescape_bytes(line))
        event_time = event_timestamp(
            chunk_info.year, event  # pylint: disable=no-member
        )
        return event_time

    return timestamp_from_line


def search_events(dt, chunk_info, lines):
    target_time = utils.datetime_to_epoch(dt)
    timestamp_from_line = get_timestamp_from_line_function(chunk_info)
    return utils.binary_search(lines, timestamp_from_line, target_time)


def break_up_chunks(raw_data):
    """
    Break up raw data into chunks by year

    Args:
        raw_data: Raw data from file

    Returns:
        collections.OrderedDict: keys are years; values are lists of chunks
    """
    chunks = [chunk.strip() for chunk in raw_data.split(b"\n\n")]
    log.info("{} chunks in pb file".format(len(chunks)))
    year_chunks = collections.OrderedDict()
    for chunk in chunks:
        lines = chunk.split(b"\n")
        chunk_info = ee.PayloadInfo()
        chunk_info.ParseFromString(unescape_bytes(lines[0]))
        chunk_year = chunk_info.year  # pylint: disable=no-member
        log.info("Year {}: {} events in chunk".format(chunk_year, len(lines) - 1))
        try:
            _, ls = year_chunks[chunk_year]
            ls.extend(lines[1:])
        except KeyError:
            year_chunks[chunk_year] = chunk_info, lines[1:]
    return year_chunks


def event_from_line(line, pv, year, event_type):
    """
    Get an ArchiveEvent from this line

    Args:
        line: A line of chunks of data
        pv: Name of the PV
        year: Year of interest
        event_type: Need to know the type of the event as key of TYPE_MAPPINGS

    Returns:
        ArchiveEvent
    """
    unescaped = unescape_bytes(line)
    event = TYPE_MAPPINGS[event_type]()
    event.ParseFromString(unescaped)
    return data.ArchiveEvent(
        pv, event.val, event_timestamp(year, event), event.severity
    )


def parse_pb_data(raw_data, pv, start, end, count=None):
    """
    Turn raw PB data into an ArchiveData object

    Args:
        raw_data: The raw data
        pv: name of PV
        start: datetime.datetime for start of window
        end: datetime.datetime for end of window
        count: return up to this many events

    Returns:
        An ArchiveData object
    """
    year_chunks = break_up_chunks(raw_data)
    events = []
    enum_options = {}
    # Iterate over years
    for year, (chunk_info, lines) in year_chunks.items():
        # Look for enum options in the chunk info.
        # Assume these are unchanging therefore stop looking after first success.
        if not enum_options:
            enum_options = parse_enum_options_from_PayloadInfo(chunk_info)
            if enum_options:
                log.debug(f"Found enum options: {enum_options}")

        # Find the index of the start event
        if start.year == year:  # search for the start
            s = search_events(start, chunk_info, lines)
        elif start.year > year:  # ignore this chunk
            s = len(lines) - 1
        else:  # start.year < year: all events from the start of the year
            s = 0
        # Find the index of the end event
        if end.year == year:  # search for the end
            e = search_events(end, chunk_info, lines)
        elif end.year < year:  # ignore this chunk
            e = 0
        else:  # end.year > year: all events to the end of the year
            e = None
        # Include the event preceding the time range. This won't work over
        # year boundaries for the time being.
        if s > 0:
            s -= 1
        log.info("Year {} start {} end {}".format(year, s, e))
        for line in lines[s:e]:
            events.append(event_from_line(line, pv, year, chunk_info.type))

    return data.data_from_events(pv, events, count, enum_options)


def parse_enum_options_from_PayloadInfo(
    payload_info: Any,
) -> collections.OrderedDict[int, str]:
    """Get enum options from a PayloadInfo object if available

    Convert the header fields to a dict and then call the parsing
    function from aa.data.

    Could not find a suitable type annotation for PayloadInfo arg.
    """
    headers_dict = {h.name: h.val for h in payload_info.headers}
    return data.parse_enum_options(headers_dict)


class PbFetcher(fetcher.AaFetcher):
    def __init__(self, hostname, port):
        super(PbFetcher, self).__init__(hostname, port, binary=True)
        self._url = "{}/retrieval/data/getData.raw".format(self._endpoint)

    def _get_values(self, pv, start, end, count, request_params):
        try:
            return super(PbFetcher, self)._get_values(
                pv, start, end, count, request_params
            )
        except requests.exceptions.HTTPError as e:
            # Not found typically means no data for the PV in this time range.
            if e.response.status_code == 404:
                return data.ArchiveData.empty(pv)
            else:
                raise e

    def _parse_raw_data(self, response, pv, start, end, count):
        raw_data = response.raw.read()
        return parse_pb_data(raw_data, pv, start, end, count)


class PbFileFetcher(fetcher.Fetcher):
    def __init__(self, roots):
        self._roots = list(roots)

    def _get_all_pb_files_of_pv(self, pv):
        """Returns a list of all .pb files of the given PV which are found under the
        root directory / directories."""
        pv_files = []
        # Split PV on either dash or colon.
        parts = re.split("[-:]", pv)
        suffix = parts.pop()
        # Go through each root path and collect all files for this PV.
        for root in self._roots:
            directory = os.path.join(root, os.path.sep.join(parts))
            for f in sorted(glob.glob(os.path.join(directory, f"{suffix}*pb"))):
                pv_files.append(f)
        if len(pv_files) == 0:
            log.warning("No pb file found for PV {}".format(pv))
        return pv_files

    @staticmethod
    def _create_datetime_for_pb_file(filepath):
        """Each .pb files of the archiver appliance ends with a date information
        corresponding to the stored data. This function returns a datetime.datetime
        object matching the date information of the given .pb file."""
        filename = os.path.basename(filepath)
        # The filename can contain just the year or stepwise more information up to
        # year, month, day, hour and minutes.
        dates = (
            re.search(r"\d{4}(_\d{2}){0,4}", filename)
            .group(0)
            .split("_")
        )
        if dates is None:
            log.warning(
                "File path does not contain date information in expected format."
            )
            return None
        dates = [int(date) for date in dates]
        # Make sure to give a least 3 arguments to datetime.datetime.
        while len(dates) < 3:
            dates.append(1)
        try:
            file_date = datetime.datetime(*dates, tzinfo=pytz.utc)
        except ValueError:
            log.warning(
                "Numbers from file path are no valid input for datetime object."
            )
            return None
        return file_date

    def _get_pb_files(self, pv, start, end=None):
        """Searches through all directories of given PV under all root directories and
        returns all .pb files containing data of the specified time window. The
        granularity setup of the archiver for LTS/MTS/STS can deviate from the standard
        one."""
        if end is None:
            end = utils.utc_now()
            log.info("No end time given, assuming now() in UTC.")
        if start > end:
            start, end = end, start
            log.warning("End date was before start date. Swapped them.")
        pv_files = self._get_all_pb_files_of_pv(pv)
        # Compare each file date against given time window.
        matching_paths = []
        for file_path in pv_files:
            file_date = self._create_datetime_for_pb_file(file_path)
            if file_date is None:
                continue
            if file_date >= start and file_date <= end:
                matching_paths.append(file_path)
            # Return newest file when only latest results are requested.
            elif file_path == pv_files[-1] and len(matching_paths) == 0:
                if start > file_date:
                    matching_paths.append(file_path)
        if len(matching_paths) == 0:
            log.warning(
                "No pb file found matching given time window for PV {}".format(pv)
            )
        return matching_paths

    @staticmethod
    def _read_pb_files(files, pv, start, end, count):
        raw_data = bytearray()
        for filepath in files:
            try:
                with open(filepath, "rb") as f:
                    # Ascii code for new line character. Makes a
                    # new 'chunk' for each file.
                    raw_data.append(10)
                    raw_data.extend(f.read())
            except IOError:  # File not found. No data.
                log.warning("No pb file {} found".format(filepath))
        return parse_pb_data(bytes(raw_data), pv, start, end, count)

    def _get_values(self, pv, start, end=None, count=None, request_params=None):
        pb_files = self._get_pb_files(pv, start, end)
        log.info("Parsing pb files {}".format(pb_files))
        return self._read_pb_files(pb_files, pv, start, end, count)


def get_iso_timestamp_for_event(year, event):
    """Returns an ISO-formatted timestamp string for the given event
    and year."""
    timestamp = event_timestamp(year, event)
    timezone = pytz.timezone("Europe/London")
    return datetime.datetime.fromtimestamp(timestamp, timezone).isoformat()

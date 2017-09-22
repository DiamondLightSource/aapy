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
"""
from . import data, fetcher, utils
from . import epics_event_pb2 as ee
from datetime import datetime
import pytz
import os
import re
import collections
import logging as log


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
        14: ee.V4GenericBytes
        }


ESC_BYTE = b'\x1B'
NL_BYTE = b'\x0A'
CR_BYTE = b'\x0D'

# The characters sequences required to unescape AA pb file format.
PB_REPLACEMENTS = {
    ESC_BYTE + b'\x01': ESC_BYTE,
    ESC_BYTE + b'\x02': NL_BYTE,
    ESC_BYTE + b'\x03': CR_BYTE
}


def unescape_bytes(byte_seq):
    """Replace specific sub-sequences in a bytes sequence.

    This escaping is defined as part of the Archiver Appliance raw file
    format: https://slacmshankar.github.io/epicsarchiver_docs/pb_pbraw.html

    Args:
        byte_seq: any byte sequence
    Returns:
        the byte sequence unescaped according to the AA file format rules
    """
    for r in PB_REPLACEMENTS:
        byte_seq = byte_seq.replace(r, PB_REPLACEMENTS[r])
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
        event_time = event_timestamp(chunk_info.year, event)
        return event_time
    return timestamp_from_line


def search_events(dt, chunk_info, lines):
    target_time = utils.datetime_to_epoch(dt)
    timestamp_from_line = get_timestamp_from_line_function(chunk_info)
    return utils.binary_search(lines, timestamp_from_line, target_time)


def break_up_chunks(raw_data):
    chunks = [chunk.strip() for chunk in raw_data.split(b'\n\n')]
    log.info('{} chunks in pb file'.format(len(chunks)))
    year_chunks = collections.OrderedDict()
    for chunk in chunks:
        lines = chunk.split(b'\n')
        chunk_info = ee.PayloadInfo()
        chunk_info.ParseFromString(unescape_bytes(lines[0]))
        log.info('Year {}: {} events in chunk'.format(chunk_info.year,
                                                      len(lines) - 1))
        try:
            ci, ls = year_chunks[chunk_info.year]
            ls.extend(lines[1:])
        except KeyError:
            year_chunks[chunk_info.year] = chunk_info, lines[1:]
    return year_chunks


def event_from_line(line, pv, year, event_type):
    unescaped = unescape_bytes(line)
    event = TYPE_MAPPINGS[event_type]()
    event.ParseFromString(unescaped)
    return data.ArchiveEvent(pv,
                             event.val,
                             event_timestamp(year, event),
                             event.severity)


def parse_pb_data(raw_data, pv, start, end, count=None):
    year_chunks = break_up_chunks(raw_data)
    events = []
    for year, (chunk_info, lines) in year_chunks.items():
        s = search_events(start, chunk_info, lines) if year == start.year else 0
        if s == -1:
            s = 0
        e = search_events(end, chunk_info, lines) if year == end.year else None
        for line in lines[s:e]:
            events.append(event_from_line(line, pv, year, chunk_info.type))
    return data.data_from_events(pv, events, count)


class PbFetcher(fetcher.AaFetcher):

    def __init__(self, hostname, port):
        super(PbFetcher, self).__init__(hostname, port)
        self._url = '{}/retrieval/data/getData.raw'.format(self._endpoint)

    def _parse_raw_data(self, raw_data, pv, start, end, count):
        return parse_pb_data(raw_data, pv, start, end, count)


class PbFileFetcher(fetcher.Fetcher):

    def __init__(self, root):
        self._root = root

    def _get_pb_file(self, pv, year):
        # Split PV on either dash or colon
        parts = re.split('[-:]', pv)
        suffix = parts.pop()
        directory = os.path.join(self._root, os.path.sep.join(parts))
        filename = '{}:{}.pb'.format(suffix, year)
        return os.path.join(directory, filename)

    @staticmethod
    def _read_pb_files(files, pv, start, end, count):
        raw_data = bytearray()
        for filepath in files:
            try:
                with open(filepath, 'rb') as f:
                    # Ascii code for new line character. Makes a
                    # new 'chunk' for each file.
                    raw_data.append(10)
                    raw_data.extend(f.read())
            except IOError: # File not found. No data.
                log.warning('No pb file {} found')
        return parse_pb_data(bytes(raw_data), pv, start, end, count)

    def get_values(self, pv, start, end=None, count=None):
        end = datetime.now(pytz.utc) if end is None else end
        pb_files = []
        for year in range(start.year, end.year + 1):
            pb_files.append(self._get_pb_file(pv, year))
        log.info('Parsing pb files {}'.format(pb_files))
        return self._read_pb_files(pb_files, pv, start, end, count)

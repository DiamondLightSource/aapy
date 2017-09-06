import aa
from aa import fetcher, utils
from aa import epics_event_pb2 as eepb
from datetime import datetime, timedelta
import curses.ascii as ascii
import numpy
import os
import re
import logging as log


# It is not clear to me why I can't extract this from the compiled protobuf file.
TYPE_MAPPINGS = {
        0: eepb.ScalarString,
        1: eepb.ScalarShort,
        2: eepb.ScalarFloat,
        3: eepb.ScalarEnum,
        4: eepb.ScalarByte,
        5: eepb.ScalarInt,
        6: eepb.ScalarDouble,
        7: eepb.VectorString,
        8: eepb.VectorShort,
        9: eepb.VectorFloat,
        10: eepb.VectorEnum,
        11: eepb.VectorChar,
        12: eepb.VectorInt,
        13: eepb.VectorDouble,
        14: eepb.V4GenericBytes
        }


REPLACEMENTS = {
        chr(ascii.ESC) + chr(1): chr(ascii.ESC),
        chr(ascii.ESC) + chr(2): chr(ascii.NL),
        chr(ascii.ESC) + chr(3): chr(ascii.CR)
        }


def unescape_line(line):
    for r in REPLACEMENTS:
        line = line.replace(r, REPLACEMENTS[r])
    return line


def year_timestamp(year):
    return (datetime(year, 1, 1) - datetime(1970, 1, 1)).total_seconds()


def event_timestamp(year, event):
    year_start = year_timestamp(year)
    return year_start + event.secondsintoyear + 1e-9 * event.nano


def get_timestamp_from_line_function(chunk_info):
    def timestamp_from_line(line):
        event = TYPE_MAPPINGS[chunk_info.type]()
        event.ParseFromString(unescape_line(line))
        event_time = event_timestamp(chunk_info.year, event)
        return event_time
    return timestamp_from_line


def binary_search(seq, f, target):
    """Find no such that f(seq[no]) >= target and f(seq[no+1]) > target.

    If f(seq[0]) > target, return -1
    If f(seq[-1]) < target, return len(seq)

    Assume f(seq[no]) < f(seq[no+1]).

    Args:
        seq: sequence of inputs on which to act
        f: function that returns a comparable when called on any input
        target: value

    Returns: index of seq meeting search requirements
    """
    if f(seq[0]) > target:
        return 0
    elif f(seq[-1]) < target:
        return len(seq)
    upper = len(seq)
    lower = -1
    while (upper - lower) > 1:
        current = (upper + lower) // 2
        next_input = seq[current]
        val = f(next_input)
        if val > target:
            upper = current
        elif val <= target:
            lower = current
    return lower


def search_events(dt, chunk_info, lines):
    target_time = utils.datetime_to_epoch(dt)
    timestamp_from_line = get_timestamp_from_line_function(chunk_info)
    return binary_search(lines, timestamp_from_line, target_time)


def parse_pb_data(raw_data, pv, start, end, count=None):
    chunks = [chunk.strip() for chunk in raw_data.split('\n\n')]
    log.info('{} chunks in pb file'.format(len(chunks)))
    events = []
    year_chunks = {}
    for chunk in chunks:
        lines = chunk.split('\n')
        log.info('{} lines in chunk'.format(len(lines)))
        chunk_info = eepb.PayloadInfo()
        chunk_info.ParseFromString(unescape_line(lines[0]))
        year_chunks[chunk_info.year] = chunk_info, lines[1:]

    chunk_info, lines = year_chunks[start.year]
    start_line = search_events(start, chunk_info, lines)
    chunk_info, lines = year_chunks[end.year]
    end_line = search_events(end, chunk_info, lines) + 1
    for year in range(start.year, end.year + 1):
        s = start_line if year == start.year else 0
        e = end_line if year == end.year else None
        info, lines = year_chunks[year]
        year_start = (datetime(info.year, 1, 1) - datetime(1970, 1, 1)).total_seconds()
        for line in lines[s:e]:
            unescaped = unescape_line(line)
            event = TYPE_MAPPINGS[info.type]()
            event.ParseFromString(unescaped)
            events.append((event.val,
                           year_start + event.secondsintoyear + 1e-9 * event.nano,
                           event.severity))

    event_count = min(count, len(events)) if count is not None else len(events)
    try:
        wf_length = len(events[0][0])
    except TypeError:
        wf_length = 1
    values = numpy.zeros((event_count, wf_length))
    timestamps = numpy.zeros((event_count,))
    severities = numpy.zeros((event_count,))
    for i, event in zip(range(event_count), events):
        values[i], timestamps[i], severities[i] = event

    if wf_length == 1:
        values = numpy.squeeze(values, axis=1)

    return aa.ArchiveData(pv, values, timestamps, severities)


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

    def _read_pb_files(self, files, pv, start, end, count):
        data = ''
        for filepath in files:
            with open(filepath) as f:
                data += '\n' + f.read()
        return parse_pb_data(data, pv, start, end, count)

    def get_values(self, pv, start, end=None, count=None):
        end = datetime.now() if end is None else end
        pb_files = []
        for year in range(start.year, end.year + 1):
            pb_files.append(self._get_pb_file(pv, year))
        log.info('Parsing pb files {}'.format(pb_files))
        return self._read_pb_files(pb_files, pv, start, end, count)

    def get_value_at(self, instant):
        raise NotImplementedError()

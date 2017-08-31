import aa
from aa import fetcher, utils
from aa import epics_event_pb2 as eepb
from datetime import datetime, timedelta
import curses.ascii as ascii
import numpy
import os


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


def parse_pb_data(raw_data, pv, count=None):
    chunks = [chunk.strip() for chunk in raw_data.split('\n\n')]
    events = []
    for chunk in chunks:
        lines = [unescape_line(line) for line in chunk.split('\n')]
        pi = eepb.PayloadInfo()
        pi.ParseFromString(lines[0])
        year_timestamp = (datetime(pi.year, 1, 1) - datetime(1970, 1, 1)).total_seconds()
        for line in lines[1:]:
            event = TYPE_MAPPINGS[pi.type]()
            event.ParseFromString(line)
            events.append((event.val,
                           year_timestamp + event.secondsintoyear + 1e-9 * event.nano,
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

    return aa.ArchiveData(pv, values, timestamps, severities)


class PbFetcher(fetcher.AaFetcher):

    def __init__(self, hostname, port):
        super(PbFetcher, self).__init__(hostname, port)
        self._url = '{}/retrieval/data/getData.raw'.format(self._endpoint)

    def _parse_raw_data(self, raw_data, pv, count):
        return parse_pb_data(raw_data, pv, count)


class PbFileFetcher(fetcher.Fetcher):

    def __init__(self, root):
        self._root = root

    def _get_pb_file(self, pv, year):
        prefix, suffix = pv.split(':')
        prefix_parts = prefix.split('-')
        directory = os.path.join(self._root, os.path.sep.join(prefix_parts))
        filename = '{}:{}.pb'.format(suffix, year)
        return os.path.join(directory, filename)

    def _read_pb_file(self, filename, pv, count):
        with open(filename) as f:
            return parse_pb_data(f.read(), pv, count)

    def get_values(self, pv, start, end=None, count=None):
        end = datetime.now() if end is None else end
        filepath = self._get_pb_file(pv, start.year)
        data = self._read_pb_file(filepath, pv, count)
        start_secs = utils.datetime_to_epoch(start)
        end_secs = utils.datetime_to_epoch(end)
        for i, ts in enumerate(data.timestamps):
            if ts > start_secs:
                start_index = i
                break
        for i, ts in enumerate(data.timestamps[start_index:]):
            if ts > end_secs:
                end_index = start_index + i
                break
        return aa.ArchiveData(
            pv,
            data.values[start_index:end_index],
            data.timestamps[start_index:end_index],
            data.severities[start_index:end_index]
            )

    def get_value_at(self, instant):
        raise NotImplementedError()

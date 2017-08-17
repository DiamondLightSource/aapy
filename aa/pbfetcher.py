import aa
from aa import fetcher
from aa import epics_event_pb2 as eepb
from datetime import datetime, timedelta
import curses.ascii as ascii
import numpy


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


ESCAPE_TO = [str(ascii.ESC), str(ascii.NL), str(ascii.CR)]


def unescape_line1(line):
    unescaping = False
    unescaped_line = ""
    for char in line:
        if ord(char) == ascii.ESC:
            unescaping = True
        else:
            if unescaping:
                unescaping = False
                unescaped_line = unescaped_line + ESCAPE_TO[ord(char)-1]
            else:
                unescaped_line = unescaped_line + char
    return unescaped_line


REPLACEMENTS = {
        chr(ascii.ESC) + chr(1): chr(ascii.ESC),
        chr(ascii.ESC) + chr(2): chr(ascii.NL),
        chr(ascii.ESC) + chr(3): chr(ascii.CR)
        }


def unescape_line2(line):
    for r in REPLACEMENTS:
        line = line.replace(r, REPLACEMENTS[r])
    return line


class PbFetcher(fetcher.AaFetcher):

    def __init__(self, hostname, port):
        super(PbFetcher, self).__init__(hostname, port)
        self._url = '{}/retrieval/data/getData.raw'.format(self._endpoint)

    def _parse_raw_data(self, raw_data, pv, count):
        chunks = [chunk.strip() for chunk in raw_data.split('\n\n')]
        events = []
        for chunk in chunks:
            lines = [unescape_line2(line) for line in chunk.split('\n')]
            pi = eepb.PayloadInfo()
            pi.ParseFromString(lines[0])
            year_timestamp = (datetime(pi.year, 1, 1) - datetime(1970, 1, 1)).total_seconds()
            for line in lines[1:]:
                event = TYPE_MAPPINGS[pi.type]()
                event.ParseFromString(line)
                events.append((event.val,
                    year_timestamp + event.secondsintoyear + 1e-9 * event.nano,
                    event.severity))

        array_size = min(count, len(events)) if count is not None else len(events)
        values = numpy.zeros((array_size,))
        timestamps = numpy.zeros((array_size,))
        severities = numpy.zeros((array_size,))
        for i, event in zip(range(array_size), (events)):
            values[i], timestamps[i], severities[i] = event

        return aa.AaData(pv, values, timestamps, severities)

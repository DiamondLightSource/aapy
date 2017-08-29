import collections
import numpy
from datetime import datetime


EPOCH = datetime(1970, 1, 1)
ArchiveData = collections.namedtuple('ArchiveData', ('pv', 'values', 'timestamps', 'severities'))


def concatenate(data_seq):
    pv = data_seq[0].pv
    for data in data_seq:
        assert data.pv == pv
    values = numpy.concatenate(data.values for data in data_seq)
    timestamps = numpy.concatenate(data.timestamps for data in data_seq)
    severities = numpy.concatenate(data.severities for data in data_seq)
    return ArchiveData(pv, values, timestamps, severities)


def datetime_to_epoch(dt):
    return int((dt - EPOCH).total_seconds())


def epoch_to_datetime(secs):
    return datetime.fromtimestamp(secs)
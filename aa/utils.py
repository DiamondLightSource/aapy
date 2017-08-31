import collections
import numpy
from datetime import datetime
import pytz


EPOCH = pytz.UTC.localize(datetime(1970, 1, 1))
ArchiveData = collections.namedtuple('ArchiveData', ('pv', 'values', 'timestamps', 'severities'))
DIFFERENT_PV_ERROR = 'All concatenated ArchiveData objects must have the same PV name'


def concatenate(data_seq):
    pv = data_seq[0].pv
    for i, data in enumerate(data_seq[1:]):
        assert data.pv == pv, DIFFERENT_PV_ERROR
        assert data.timestamps[0] > data_seq[i].timestamps[-1]
    values = numpy.concatenate([data.values for data in data_seq])
    timestamps = numpy.concatenate([data.timestamps for data in data_seq])
    severities = numpy.concatenate([data.severities for data in data_seq])
    return ArchiveData(pv, values, timestamps, severities)


def assert_archive_data_equal(data1, data2):
    assert data1.pv == data2.pv
    numpy.testing.assert_equal(data1.values, data2.values)
    numpy.testing.assert_equal(data1.timestamps, data2.timestamps)
    numpy.testing.assert_equal(data1.severities, data2.severities)
def datetime_to_epoch(dt):
    return int((dt - EPOCH).total_seconds())


def epoch_to_datetime(secs):
    return datetime.fromtimestamp(secs, tz=pytz.UTC)
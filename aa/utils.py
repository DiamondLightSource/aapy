import collections
import numpy
from datetime import datetime
import pytz
import logging as log


EPOCH = pytz.UTC.localize(datetime(1970, 1, 1))
ArchiveData = collections.namedtuple('ArchiveData', ('pv', 'values', 'timestamps', 'severities'))
ArchiveEvent = collections.namedtuple('ArchiveEvent', ('pv', 'value', 'timestamp', 'severity'))
DIFFERENT_PV_ERROR = 'All concatenated ArchiveData objects must have the same PV name'
TIMESTAMP_ERROR = ('Last timestamp in first ArchiveData object: {}\n'
                   'First timestamp in second ArchiveData object: {}')


def concatenate(data_seq):
    pv = data_seq[0].pv
    for i, data in enumerate(data_seq[1:]):
        assert data.pv == pv, DIFFERENT_PV_ERROR
        assert data.timestamps[0] > data_seq[i].timestamps[-1], TIMESTAMP_ERROR.format(
            epoch_to_datetime(data_seq[i].timestamps[-1]),
            epoch_to_datetime(data.timestamps[0]))
    values = numpy.concatenate([data.values for data in data_seq])
    timestamps = numpy.concatenate([data.timestamps for data in data_seq])
    severities = numpy.concatenate([data.severities for data in data_seq])
    return ArchiveData(pv, values, timestamps, severities)


def assert_archive_data_equal(data1, data2):
    assert data1.pv == data2.pv
    numpy.testing.assert_equal(data1.values, data2.values)
    numpy.testing.assert_equal(data1.timestamps, data2.timestamps)
    numpy.testing.assert_equal(data1.severities, data2.severities)


def get_event(archive_data, index):
    return ArchiveEvent(archive_data.pv,
                        archive_data.values[index],
                        archive_data.timestamps[index],
                        archive_data.severities[index])


"""
def event_str(data, index):
    msg = 'Archive event timestamp {:%Y-%m-%d %H:%M:%S.%f} value {:.3f} severity {:.0f}'
    return msg.format(epoch_to_datetime(data.timestamps[index]),
                     data.values[index],
                     data.severities[index])
"""

def event_str(event):
    msg = 'Archive event timestamp {:%Y-%m-%d %H:%M:%S.%f} value {:.3f} severity {:.0f}'
    return msg.format(epoch_to_datetime(event.timestamp),
                      event.value,
                      event.severity)


def event_equal(event1, event2):
    return (event1.pv == event2.pv and
            event1.value == event2.value and
            event1.timestamp == event2.timestamp and
            event1.severity == event2.severity)


"""
def event_equal(data1, index1, data2, index2):
    identical = data1.values[index1] == data2.values[index2]
    identical = identical and data1.timestamps[index1] == data2.timestamps[index2]
    identical = identical and data1.severities[index1] == data2.severities[index2]
    return identical
"""


def assert_event_similar(data1, index1, data2, index2):
    numpy.testing.assert_allclose(data1.values[index1], data2.values[index2])
    numpy.testing.assert_equal(data1.timestamps[index1], data2.timestamps[index2])
    numpy.testing.assert_equal(data1.severities[index1], data2.severities[index2])


def assert_archive_data_close(data1, data2):
    EPS = 1e-6
    assert data1.pv == data2.pv
    for d1, t1, d2, t2 in zip(data1.values, data1.timestamps, data2.values, data2.timestamps):
        if abs(d1 - d2) > EPS:
            print('different:\n{}\n{}'.format(d1, d2))
            print('{}\n{}\n'.format(epoch_to_datetime(t1), epoch_to_datetime(t2)))
    numpy.testing.assert_allclose(data1.values, data2.values, atol=1e-5)
    numpy.testing.assert_allclose(data1.timestamps, data2.timestamps)
    numpy.testing.assert_allclose(data1.severities, data2.severities)


def summarise(data):
    log.info('Archive data for pv %s', data.pv)
    log.info('First event time %s', epoch_to_datetime(data.timestamps[0]))
    log.info('Last event time %s', epoch_to_datetime(data.timestamps[-1]))
    log.info('%d events', len(data.timestamps))
    log.info('Max value %.3f', data.values.max())
    log.info('Min value %.3f', data.values.min())


def datetime_to_epoch(dt):
    return int((dt - EPOCH).total_seconds())


def epoch_to_datetime(secs):
    return datetime.fromtimestamp(secs, tz=pytz.UTC)
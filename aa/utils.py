from __future__ import print_function
import numpy
from datetime import datetime
import pytz
import logging as log


EPOCH = pytz.UTC.localize(datetime(1970, 1, 1))


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


def year_timestamp(year):
    return (datetime(year, 1, 1) - datetime(1970, 1, 1)).total_seconds()


def print_raw_bytes(byte_seq):
    for b in byte_seq:
        print('\\x{:02x}'.format(ord(b)), end='')
    print('')


def binary_search(seq, f, target):
    """Find no such that f(seq[no]) >= target and f(seq[no+1]) > target.

    If f(seq[0]) > target, return -1
    If f(seq[-1]) < target, return len(seq)

    Assume f(seq[no]) < f(seq[no+1]).

    Args:
        seq: sequence of inputs on which to act
        f: function that returns a comparable when called on any input
        target: value

    Returns: index of item in seq meeting search requirements
    """
    if len(seq) == 0 or f(seq[0]) > target:
        return -1
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

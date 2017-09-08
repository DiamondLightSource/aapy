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
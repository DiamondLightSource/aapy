import numpy
import logging as log
import aa
from aa import utils
from aa.fetcher import Fetcher
try:
    from xmlrpc.client import ServerProxy
except ImportError:  # Python 2 compatibility
    from xmlrpclib import ServerProxy


class CaFetcher(Fetcher):

    def __init__(self, url):
        self._url = url
        self._proxy = ServerProxy(url)

    def get_values(self, pv, start, end=None, count=None):
        count = 10000 if count is None else count
        start_secs = utils.datetime_to_epoch(start)
        end_secs = utils.datetime_to_epoch(end)
        data = self._proxy.archiver.values(1, [pv], start_secs, 0, end_secs, 0, count, 0)
        event_count = len(data[0]['values'])
        wf_length = len(data[0]['values'][0]['value'])
        values = numpy.zeros((event_count, wf_length))
        timestamps = numpy.zeros((event_count,))
        sevs = numpy.zeros((event_count,))
        count = 0
        previous_val = {}
        for val in data[0]['values']:
            if val == previous_val:
                log.debug('Continuing on duplicate event %s', val)
                continue
            previous_val = val

            timestamp = val['secs'] + 1e-9 * val['nano']
            if timestamp < start_secs:
                continue
            if val['sevr'] > 3:
                log.debug('Discarding error event %s %s',
                          utils.epoch_to_datetime(timestamp), val)
                continue
            timestamps[count] = timestamp
            values[count] = val['value']
            sevs[count] = val['sevr']
            count += 1

        return aa.ArchiveData(pv, numpy.squeeze(values[:count]),
                              timestamps[:count], sevs[:count])


    def get_value_at(self, pv, instant):
        return self._get_values(pv, instant, instant, 1)

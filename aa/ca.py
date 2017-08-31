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

    def _process_raw_data(self, data, pv):
        event_count = len(data)
        wf_length = len(data[0]['value'])
        values = numpy.zeros((event_count, wf_length))
        timestamps = numpy.zeros((event_count,))
        sevs = numpy.zeros((event_count,))
        for count, val in enumerate(data):
            timestamp = val['secs'] + 1e-9 * val['nano']
            timestamps[count] = timestamp
            values[count] = val['value']
            sevs[count] = val['sevr']
        return aa.ArchiveData(pv, numpy.squeeze(values), timestamps, sevs)

    def get_values(self, pv, start, end=None, count=None):
        # Make count a large number if not specified to ensure we get all
        # data.
        count = 2**31 if count is None else count
        requested = 10000 if count > 10000 else count
        start_secs = utils.datetime_to_epoch(start)
        end_secs = utils.datetime_to_epoch(end)
        response = self._proxy.archiver.values(1, [pv], start_secs, 0,
                                               end_secs, 0, requested, 0)
        done = len(response[0]['values']) < requested
        data = self._process_raw_data(response[0]['values'], pv)
        while done is not True and len(data.values) < count:
            log.warn('{} samples requested; {} fetched so far.'.format(count,
                                                                       len(data.values)))
            # The first two samples will be the last from the previous request
            # and the one before that.
            requested = count - len(data.values) + 2
            log.warn('Making additional request for {} samples.'.format(requested))
            start_secs = int(data.timestamps[-1])
            start_nanos = int(1e9 * (data.timestamps[-1] % 1))
            response = self._proxy.archiver.values(1, [pv],
                                                   start_secs, start_nanos,
                                                   end_secs, 0,
                                                   requested, 0)
            done = len(response) < requested
            # Discard the two samples we already have.
            new_data = self._process_raw_data(response[0]['values'][2:], pv)
            data = utils.concatenate((data, new_data))
        return data

    def get_value_at(self, pv, instant):
        return self.get_values(pv, instant, instant, 1)

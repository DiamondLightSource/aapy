import numpy
import logging as log
import aa
from aa import utils
from aa.fetcher import Fetcher
try:
    from xmlrpc.client import ServerProxy
except ImportError:  # Python 2 compatibility
    from xmlrpclib import ServerProxy


class CaClient(object):

    def __init__(self, url):
        self._proxy = ServerProxy(url)

    def get(self, pv, start, end, count):
        start_secs = utils.datetime_to_epoch(start)
        end_secs = utils.datetime_to_epoch(end)
        response = self._proxy.archiver.values(1, [pv], start_secs, 0,
                                               end_secs, 0, count, 0)
        return response[0]['values']


class CaFetcher(Fetcher):

    def __init__(self, url):
        self._client = CaClient(url)

    def _process_raw_data(self, events, pv):
        event_count = len(events)
        wf_length = len(events[0]['value'])
        values = numpy.zeros((event_count, wf_length))
        timestamps = numpy.zeros((event_count,))
        sevs = numpy.zeros((event_count,))
        for count, val in enumerate(events):
            timestamp = val['secs'] + 1e-9 * val['nano']
            timestamps[count] = timestamp
            values[count] = val['value']
            sevs[count] = val['sevr']
        if values.shape[1] == 1:
            values = numpy.squeeze(values, axis=1)
        return aa.ArchiveData(pv, values, timestamps, sevs)

    def get_values(self, pv, start, end=None, count=None):
        # Make count a large number if not specified to ensure we get all
        # data.
        count = 2**31 if count is None else count
        requested = 10000 if count > 10000 else count
        events = self._client.get(pv, start, end, requested)
        # Fewer samples than requested means that that was all there were,
        # and so we are done.
        done = len(events) < requested
        data = self._process_raw_data(events, pv)
        while done is not True and len(data.values) < count:
            log.warn('{} samples requested; {} fetched so far.'.format(count,
                                                                       len(data.values)))
            # The first two samples will be the last from the previous request
            # and the one before that.
            requested = count - len(data.values) + 2
            log.warn('Making additional request for {} samples.'.format(requested))
            start = utils.epoch_to_datetime(data.timestamps[-1])
            events = self._client.get(pv, start, end, requested)
            done = len(events) < requested
            # Discard the two samples we already have.
            new_data = self._process_raw_data(events[2:], pv)
            data = utils.concatenate((data, new_data))
        return data

    def get_value_at(self, pv, instant):
        return self.get_values(pv, instant, instant, 1)

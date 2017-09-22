import logging as log
from aa import data, utils
from aa.fetcher import Fetcher
try:
    from xmlrpc.client import ServerProxy
except ImportError:  # Python 2 compatibility
    from xmlrpclib import ServerProxy


class CaClient(object):

    def __init__(self, url):
        self._proxy = ServerProxy(url)

    @staticmethod
    def _create_archive_event(pv, ca_event):
        value = ca_event['value']
        timestamp = ca_event['secs'] + 1e-9 * ca_event['nano']
        severity = ca_event['sevr']
        return data.ArchiveEvent(pv, value, timestamp, severity)

    def get(self, pv, start, end, count):
        start_secs = utils.datetime_to_epoch(start)
        end_secs = utils.datetime_to_epoch(end)
        response = self._proxy.archiver.values(1, [pv], start_secs, 0,
                                               end_secs, 0, count, 0)
        return [CaClient._create_archive_event(pv, val)
                for val in response[0]['values']]


class CaFetcher(Fetcher):

    def __init__(self, url):
        self._client = CaClient(url)

    def _get_values(self, pv, start, end=None, count=None):
        # Make count a large number if not specified to ensure we get all
        # data.
        count = 2**31 if count is None else count
        requested = min(count, 10000)
        events = self._client.get(pv, start, end, requested)
        log.info('Request PV {} for {} samples.'.format(pv, requested))
        log.info('Request start {} end {}'.format(start, end))
        # Fewer samples than requested means that that was all there were,
        # and so we are done.
        done = len(events) < requested
        all_data = data.data_from_events(pv, events)
        while done is not True and len(all_data) < count:
            log.warn('{} samples fetched so far.'.format(len(all_data)))
            # The first two samples will be the last from the previous request
            # and the one before that.
            requested = min(count - len(all_data) + 2, 10000)
            start = utils.epoch_to_datetime(all_data.timestamps[-1])
            log.info('Making additional request for {} samples.'.format(requested))
            log.info('Request start {} end {}'.format(start, end))
            events = self._client.get(pv, start, end, requested)
            done = len(events) < requested
            skip = 0
            for event in events:
                if event.timestamp <= all_data.timestamps[-1]:
                    skip += 1
                else:
                    break
            new_data = data.data_from_events(pv, events[skip:])
            all_data.append(new_data)
        return all_data

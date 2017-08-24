import numpy
import aa
from aa.fetcher import Fetcher
from datetime import datetime
try:
    from xmlrpc.client import ServerProxy
except ImportError:  # Python 2 compatibility
    from xmlrpclib import ServerProxy


class CaFetcher(Fetcher):

    EPOCH = datetime(1970, 1, 1)

    def __init__(self, url):
        self._url = url
        self._proxy = ServerProxy(url)

    def _datetime_to_epoch(self, dt):
        return int((dt - CaFetcher.EPOCH).total_seconds())

    def get_values(self, pv, start, end=None, count=None):
        count = 10000 if count is None else count
        start_secs = self._datetime_to_epoch(start)
        end_secs = self._datetime_to_epoch(end)
        data = self._proxy.archiver.values(1, [PV], start_secs, 0, end_secs, 0, count, 0)
        array_size = len(data[0]['values'])
        vals = numpy.zeros((array_size,))
        timestamps = numpy.zeros((array_size,))
        sevs = numpy.zeros((array_size,))
        for i, val in enumerate(data[0]['values']):
            timestamp = val['secs'] + 1e-9 * val['nano']
            timestamps[i] = timestamp
            vals[i] = val['value'][0]
            sevs[i] = val['sevr']

        return aa.ArchiveData(pv, vals, timestamps, sevs)

    def get_value_at(self, pv, instant):
        return self._get_values(pv, start, start, 1)


if __name__ == '__main__':
    PV = 'SR-DI-DCCT-01:SIGNAL'
    PRIMARY_ARCHIVER = 'http://archiver.pri.diamond.ac.uk/archive/cgi/ArchiveDataServer.cgi'
    fetcher = CaFetcher(PRIMARY_ARCHIVER)

    start = datetime(2016, 1, 2)
    end = datetime(2017, 1, 1)

    data = fetcher.get_values(PV, start, end)

    print(len(data.values))
    print(datetime.fromtimestamp(data.timestamps[0]))
    print(datetime.fromtimestamp(data.timestamps[-1]))


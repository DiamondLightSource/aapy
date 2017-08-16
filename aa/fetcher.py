try:
    from urllib.parse import quote
    from urllib.request import urlopen
except ImportError:  # Python 2 compatibility.
    from urllib import quote
    from urllib2 import urlopen

from datetime import tzinfo, timedelta, datetime


class AaFetcher(object):

    def __init__(self, hostname, port):
        self._host = hostname
        self._port = port
        self._endpoint = 'http://{}:{}'.format(self._host, self._port)
        self._url = None

    def _format_date(self, datetime):
        return datetime.strftime('%Y-%m-%dT%H:%M:%SZ')

    def _construct_url(self, pv, start, end):
        encoded_pv = quote(pv)
        encoded_start = quote(self._format_date(start))
        encoded_end = quote(self._format_date(end))
        suffix = '?pv={}&from={}&to={}'.format(encoded_pv, 
                encoded_start, encoded_end)
        return '{}{}'.format(self._url, suffix)

    def _fetch_data(self, pv, start, end):
        url = self._construct_url(pv, start, end)
        urlinfo = urlopen(url)
        return urlinfo.read()

    def get_values(self, pv, start, end=None, count=None):
        if end is None:
            end = datetime.now()
        return self._get_values(pv, start, end, count)

    def get_value_at(self, pv, start):
        raise NotImplementedError('This feature is planned but not implemented.')

    def _get_values(self, pv, start, end, count):
        raise NotImplementedError()

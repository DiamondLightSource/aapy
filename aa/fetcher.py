import urllib2
import urllib

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
        encoded_pv = urllib.quote(pv)
        encoded_start = urllib.quote(self._format_date(start))
        encoded_end = urllib.quote(self._format_date(end))
        suffix = '?pv={}&from={}&to={}'.format(encoded_pv, 
                encoded_start, encoded_end)
        return '{}{}'.format(self._url, suffix)

    def _fetch_data(self, pv, start, end):
        url = self._construct_url(pv, start, end)
        urlinfo = urllib2.urlopen(url)
        return urlinfo.read()

    def get_value_at(self, pv, start):
        return self.get_values(pv, start, start, 1)

    def get_values(self, pv, start, end=None, count=None):
        if end is None:
            end = datetime.now()
        return self._get_values(pv, start, end, count)

    def _get_values(self, pv, start, end, count):
        raise NotImplementedError()


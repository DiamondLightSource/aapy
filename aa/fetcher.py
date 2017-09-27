from utils import quote, urlopen
import pytz
from datetime import datetime


class Fetcher(object):

    def _get_values(self, pv, start, end, count):
        raise NotImplementedError()

    def get_values(self, pv, start, end=None, count=None):
        if end is None:
            end = pytz.utc.localize(datetime.now())
        return self._get_values(pv, start, end, count)

    def get_event_at(self, pv, instant):
        try:
            return self.get_values(pv, instant, instant, 1).get_event(0)
        except IndexError:
            error_msg = 'No data found for pv {} at timestamp {}'
            raise ValueError(error_msg.format(pv, instant))


class AaFetcher(Fetcher):

    def __init__(self, hostname, port):
        self._host = hostname
        self._port = port
        self._endpoint = 'http://{}:{}'.format(self._host, self._port)
        self._url = None

    def _format_datetime(self, dt):
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    def _construct_url(self, pv, start, end):
        encoded_pv = quote(pv)
        encoded_start = quote(self._format_datetime(start))
        encoded_end = quote(self._format_datetime(end))
        suffix = '?pv={}&from={}&to={}'.format(encoded_pv,
                encoded_start, encoded_end)
        return '{}{}'.format(self._url, suffix)

    def _fetch_data(self, pv, start, end):
        url = self._construct_url(pv, start, end)
        urlinfo = urlopen(url)
        return urlinfo.read()

    def _get_values(self, pv, start, end, count):
        raw_data = self._fetch_data(pv, start, end)
        return self._parse_raw_data(raw_data, pv, start, end, count)

    def _parse_raw_data(self, raw_data, pv, start, end, count):
        raise NotImplementedError()

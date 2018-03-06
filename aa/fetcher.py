import pytz
from datetime import datetime

from .utils import urlget


class Fetcher(object):
    """Abstract base class for fetching data from an archiver."""

    def _get_values(self, pv, start, end, count):
        raise NotImplementedError()

    def get_values(self, pv, start, end=None, count=None):
        """Retrieve archive data.

        Args:
            pv: PV to request data for
            start: datetime at start of requested period
            end: datetime at end of requested period. If None, request all
                events to the current time
            count: maximum number of events to return. In None, return all
                events

        Returns:
            ArchiveData object representing all events

        """
        if end is None:
            end = pytz.utc.localize(datetime.now())
        return self._get_values(pv, start, end, count)

    def get_event_at(self, pv, instant):
        """Retrieve the event preceding the specified datetime.

        Args:
            pv: PV to request event for
            instant: datetime of the requested event

        Returns:
            ArchiveEvent representing the event preceding the requested
            datetime

        """
        try:
            return self.get_values(pv, instant, instant, 1).get_event(0)
        except IndexError:
            error_msg = 'No data found for pv {} at timestamp {}'
            raise ValueError(error_msg.format(pv, instant))


class AaFetcher(Fetcher):
    """Abstract base class for fetching data from the Archiver Appliance."""

    def __init__(self, hostname, port):
        self._host = hostname
        self._port = port
        self._endpoint = 'http://{}:{}'.format(self._host, self._port)
        self._url = None

    @staticmethod
    def _format_datetime(dt):
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    def _construct_url(self, pv, start, end):
        suffix = '?pv={}&from={}&to={}'.format(
            pv,
            self._format_datetime(start),
            self._format_datetime(end)
        )
        return '{}{}'.format(self._url, suffix)

    def _fetch_data(self, pv, start, end):
        url = self._construct_url(pv, start, end)
        return urlget(url)

    def _get_values(self, pv, start, end, count):
        raw_data = self._fetch_data(pv, start, end)
        return self._parse_raw_data(raw_data, pv, start, end, count)

    def _parse_raw_data(self, raw_data, pv, start, end, count):
        """Convert raw data received from the Archiver Appliance.

        This must be implemented by any subclasses.

        Args:
            raw_data: raw data received
            pv: PV name requested
            start: datetime of start of request
            end: datetime of end of request
            count: maximum number of events

        Returns:
            ArchiveData object containing all events

        """
        raise NotImplementedError()

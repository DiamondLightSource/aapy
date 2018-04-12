import logging
import pytz
import requests
from datetime import datetime


class Fetcher(object):
    """Abstract base class for fetching data from an archiver."""

    def _get_values(self, pv, start, end, count):
        raise NotImplementedError()

    def get_values(self, pv, start, end=None, count=None):
        """Retrieve archive data.

        start and end are datetime objects. If they are not timezone aware,
        assume that they are in UTC.

        Args:
            pv: PV to request data for
            start: datetime at start of requested period
            end: datetime at end of requested period. If None, request all
                events to the current time
            count: maximum number of events to return. If None, return all
                events

        Returns:
            ArchiveData object representing all events

        """
        if start.tzinfo is None:
            logging.warning('Assuming start datetime {} is UTC'.format(start))
            start = start.replace(tzinfo=pytz.UTC)
        if end is not None:
            if end.tzinfo is None:
                logging.warning('Assuming end datetime {} is UTC'.format(end))
                end = end.replace(tzinfo=pytz.UTC)
        else:
            end = pytz.utc.localize(datetime.now())
        return self._get_values(pv, start, end, count)

    def get_event_at(self, pv, instant):
        """Retrieve the event preceding the specified datetime.

        If the datetime is not timezone aware, assume that it is in UTC.

        Args:
            pv: PV to request event for
            instant: datetime of the requested event

        Returns:
            ArchiveEvent representing the event preceding the requested
            datetime

        """
        if instant.tzinfo is None:
            logging.warning('Assuming datetime {} is UTC'.format(instant))
            instant = instant.replace(tzinfo=pytz.UTC)
        try:
            return self.get_values(pv, instant, instant, 1).get_event(0)
        except IndexError:
            error_msg = 'No data found for pv {} at timestamp {}'
            raise ValueError(error_msg.format(pv, instant))


class AaFetcher(Fetcher):
    """Abstract base class for fetching data from the Archiver Appliance."""

    def __init__(self, hostname, port, binary=False):
        self._host = hostname
        self._port = port
        self._endpoint = 'http://{}:{}'.format(self._host, self._port)
        self._url = None
        self._binary = binary

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
        return requests.get(url, stream=self._binary)

    def _get_values(self, pv, start, end, count):
        response = self._fetch_data(pv, start, end)
        response.raise_for_status()
        return self._parse_raw_data(response, pv, start, end, count)

    def _parse_raw_data(self, response, pv, start, end, count):
        """Convert raw data received from the Archiver Appliance.

        This must be implemented by any subclasses.

        Args:
            response: requests response object
            pv: PV name requested
            start: datetime of start of request
            end: datetime of end of request
            count: maximum number of events

        Returns:
            ArchiveData object containing all events

        """
        raise NotImplementedError()

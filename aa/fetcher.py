"""Base classes for use in fetching data from archivers."""
from datetime import datetime

import pytz
import requests

from . import utils

__all__ = [
    "Fetcher",
    "AaFetcher",
]


class Fetcher(object):
    """Abstract base class for fetching data from an archiver."""

    def _get_values(self, pv, start, end, count, request_params):
        raise NotImplementedError()

    def get_values(self, pv, start, end=None, count=None, request_params=None):
        """Retrieve archive data.

        start and end are datetime objects. If they are not timezone aware,
        assume that they are in the local timezone.

        Args:
            pv: PV to request data for
            start: datetime at start of requested period
            end: datetime at end of requested period. If None, request all
                events to the current time
            count: maximum number of events to return. If None, return all
                events
            request_params: Settings dictionary used for archiver request

        Returns:
            ArchiveData object representing all events

        """
        if start.tzinfo is None:
            start = utils.add_local_timezone(start)
        if end is not None:
            if end.tzinfo is None:
                end = utils.add_local_timezone(end)
        else:
            end = utils.add_local_timezone(datetime.now())
        return self._get_values(pv, start, end, count, request_params)

    def get_event_at(self, pv, instant, request_params=None):
        """Retrieve the event preceding the specified datetime.

        If instant is not timezone aware, assume that it is in the local
        timezone.

        Args:
            pv: PV to request event for
            instant: datetime of the requested event
            request_params: Settings dictionary used for archiver request

        Returns:
            ArchiveEvent representing the event preceding the requested
            datetime

        """
        if instant.tzinfo is None:
            instant = utils.add_local_timezone(instant)
        try:
            return self.get_values(pv, instant, instant, 1, request_params).get_event(0)
        except IndexError:
            error_msg = "No data found for pv {} at timestamp {}"
            raise ValueError(error_msg.format(pv, instant))


class AaFetcher(Fetcher):
    """Abstract base class for fetching data from the Archiver Appliance."""

    def __init__(self, hostname, port, binary=False):
        self._host = hostname
        self._port = port
        self._endpoint = "http://{}:{}".format(self._host, self._port)
        self._url = None
        self._binary = binary

    @staticmethod
    def _format_datetime(dt):
        """Format datetime into string for use in AA URL.

        Convert to UTC for simplicity in rendering.

        Args:
            dt: datetime to format

        Returns:
            formatted datetime string

        """
        assert dt.tzinfo is not None
        return dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _construct_url(self, pv, start, end, request_params):
        if request_params is None:
            request_params = {}

        # Gets values for all fields. Enables us to give string labels for enums.
        request_params["fetchLatestMetadata"] = "true"

        suffix = "?pv={}&from={}&to={}".format(
            pv, self._format_datetime(start), self._format_datetime(end)
        )

        for key, value in request_params.items():
            suffix += "&{}={}".format(key, value)

        return "{}{}".format(self._url, suffix)

    def _fetch_data(self, pv, start, end, request_params):
        url = self._construct_url(pv, start, end, request_params)
        return requests.get(url, stream=self._binary)

    def _get_values(self, pv, start, end, count, request_params):
        response = self._fetch_data(pv, start, end, request_params)
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

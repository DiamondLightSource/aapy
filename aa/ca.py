"""Simple client to the Channel Archiver using xmlrpc."""
import logging as log
from xmlrpc.client import ServerProxy

import numpy

from . import data, utils
from .fetcher import Fetcher

__all__ = [
    "CaClient",
    "CaFetcher",
]


class CaClient(object):
    """Class to handle XMLRPC interaction with a channel archiver."""

    def __init__(self, url):
        """
        Args:
            url: url for the channel archiver
        """
        self._proxy = ServerProxy(url)

    @staticmethod
    def _create_archive_event(pv, ca_event):
        """Create ArchiveEvent from the objects received over XMLRPC.

        Args:
            pv: PV name to add to the event
            ca_event: object received over XMLRPC

        Returns:
            ArchiveEvent object

        """
        value = ca_event["value"]
        timestamp = ca_event["secs"] + 1e-9 * ca_event["nano"]
        severity = ca_event["sevr"]
        return data.ArchiveEvent(pv, value, timestamp, severity)

    def get(self, pv, start, end, count):
        """Request events over XMLRPC.

        Args:
            pv: PV name to request events for
            start: datetime of start of requested period
            end: datetime of end of requested period
            count: maximum number of events to retrieve

        Returns:
            List of ArchiveEvent objects

        """
        start_secs = utils.datetime_to_epoch(start)
        end_secs = utils.datetime_to_epoch(end)
        response = self._proxy.archiver.values(
            1, [pv], start_secs, 0, end_secs, 0, count, 0
        )
        return [
            CaClient._create_archive_event(pv, val) for val in response[0]["values"]
        ]


class CaFetcher(Fetcher):
    """Class to retrieve data from a channel archiver."""

    def __init__(self, url):
        """
        Args:
            url: url for the channel archiver
        """
        self._client = CaClient(url)

    def _get_values(self, pv, start, end=None, count=None, request_params=None):
        # Make count a large number if not specified to ensure we get all
        # data.
        count = 2 ** 31 if count is None else count
        empty_array = numpy.zeros((0,))
        all_data = data.ArchiveData(pv, empty_array, empty_array, empty_array)
        last_timestamp = -1
        done = False
        while done is not True and len(all_data) < count:
            requested = min(count - len(all_data), 10000)
            if all_data.timestamps.size:
                last_timestamp = all_data.timestamps[-1]
                start = utils.epoch_to_datetime(last_timestamp)
            log.info("Request PV {} for {} samples.".format(pv, requested))
            log.info("Request start {} end {}".format(start, end))
            events = self._client.get(pv, start, end, requested)
            done = len(events) < requested
            # Drop any events that are earlier than ones already fetched.
            events = [e for e in events if e.timestamp > last_timestamp]
            new_data = data.data_from_events(pv, events)
            all_data = all_data.concatenate(new_data, zero_pad=True)
        return all_data

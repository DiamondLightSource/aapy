"""
Python client to the EPICS Archiver Appliance.
"""
from . import js
from .data import ArchiveData, ArchiveEvent
import logging


LOG_FORMAT = '%(levelname)s:  %(message)s'
LOG_LEVEL = logging.INFO
logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)


URL = 'cs03r-cs-serv-54.cs.diamond.ac.uk'
PORT = 8080


def get_value_at(pv, date):
    """Retrieve the one event preceding the specified datetime.

    Args:
        pv: PV to query
        date: datetime before which to find the event

    Returns:
        ArchiveEvent object representing one event

    """
    fetcher = js.JsonFetcher(URL, PORT)
    return fetcher.get_event_at(pv, date)


def get_values(pv, start_date, end_date=None, count=None):
    """Retrieve all values between the start and end datetimes.
    If no end datetime is specified, return all events to the current time.
    If count is specified, limit to the first count events.

    The event preceding the first datetime will also be included.

    Args:
        pv: PV to query
        start_date: start of period for which to retrieve events
        end_date: end of period for which to retrieve events
        count: restrict number of events to count

    Returns:
        ArchiveData object representing all events in specified period

    """
    fetcher = js.JsonFetcher(URL, PORT)
    return fetcher.get_values(pv, start_date, end_date, count)

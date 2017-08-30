from . import js
from .utils import ArchiveData
import logging


LOG_FORMAT = '%(levelname)s:  %(message)s'
LOG_LEVEL = logging.INFO
logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)


URL = 'cs03r-cs-serv-54.cs.diamond.ac.uk'
PORT = 8080


def get_value_at(pv, date):
    fetcher = js.JsonFetcher(URL, PORT)
    return fetcher.get_value_at(pv, date)


def get_values(pv, start_date, end_date=None, count=None):
    fetcher = js.JsonFetcher(URL, PORT)
    return fetcher.get_values(pv, start_date, end_date, count)

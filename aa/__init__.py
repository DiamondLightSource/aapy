from . import js
import collections


URL = 'cs03r-cs-serv-54.cs.diamond.ac.uk'
PORT = 8080


ArchiveData = collections.namedtuple('ArchiveData', ('pv', 'values', 'timestamps', 'severities'))


def get_value_at(pv, date):
    fetcher = js.JsonFetcher(URL, PORT)
    return fetcher.get_value_at(pv, date)


def get_values(pv, start_date, end_date=None, count=None):
    fetcher = js.JsonFetcher(URL, PORT)
    return fetcher.get_values(pv, start_date, end_date, count)

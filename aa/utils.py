from __future__ import print_function
from datetime import datetime
import logging
import pytz
import requests
from requests.exceptions import ConnectionError, HTTPError

# string23 is used for type-checking for strings in both Python 2 and Python 3.
try:
    string23 = basestring
except NameError:
    string23 = str


def utc_datetime(*args):
    return pytz.UTC.localize(datetime(*args))


def utc_now():
    return pytz.UTC.localize(datetime.now())


EPOCH = utc_datetime(1970, 1, 1)


def datetime_to_epoch(dt):
    return int((dt - EPOCH).total_seconds())


def epoch_to_datetime(secs):
    return datetime.fromtimestamp(secs, tz=pytz.UTC)


def year_timestamp(year):
    return (datetime(year, 1, 1) - datetime(1970, 1, 1)).total_seconds()


def urlget(url):
    """Simple shim to call requests.get()."""
    logging.debug('Fetching URL {}'.format(url))
    return requests.get(url)


def urlpost(url, payload, headers):
    """Simple shim to call requests.post()."""
    logging.debug('Posting to URL {}'.format(url))
    return requests.post(url, payload, headers=headers)


def print_raw_bytes(byte_seq):
    for b in byte_seq:
        print('\\x{:02x}'.format(ord(b)), end='')
    print('')


def binary_search(seq, f, target):
    """Find no such that f(seq[no-1]) <= target and f(seq[no]) > target.

    If target < f(seq[0]), return 0
    If target > f(seq[-1]), return len(seq)

    Assume f(seq[no]) < f(seq[no+1]).

    The integer result is useful for indexing the array.

    Args:
        seq: sequence of inputs on which to act
        f: function that returns a comparable when called on any input
        target: value

    Returns: index of item in seq meeting search requirements
    """
    if len(seq) == 0 or f(seq[0]) > target:
        return 0
    elif f(seq[-1]) < target:
        return len(seq)
    upper = len(seq)
    lower = 0
    while (upper - lower) > 1:
        current = (upper + lower) // 2
        next_val = f(seq[current])
        if next_val > target:
            upper = current
        elif next_val <= target:
            lower = current
    return upper

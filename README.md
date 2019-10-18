[![Build Status](https://travis-ci.org/dls-controls/aapy.svg?branch=travis)](https://travis-ci.org/dls-controls/aapy) [![Coverage Status](https://coveralls.io/repos/github/dls-controls/aapy/badge.svg?branch=master)](https://coveralls.io/github/dls-controls/aapy?branch=master)

Python code to retrieve data from the Archiver Appliance.

## Usage

### Note on timezones

When you pass a datetime to aapy it doesn't know by default what timezone
that datetime is supposed to be in. It will assume that it is the local
timezone, but will print a warning. If you pass it a timezone-aware
datetime no warning will be printed. You can use `utc_datetime()` as
a shortcut:

    >>> utc_datetime(2019, 10, 7, 17) # 5pm UTC on 7th October 2019

### Fetching data

To retrieve data, create the appropriate fetcher

    >>> from aa.js import JsonFetcher
    >>> jf = JsonFetcher('archappl.diamond.ac.uk', 80)

You can request a single event, returning an ArchiveEvent object:

    >>> from datetime import datetime
    >>> event = jf.get_event_at('SR-DI-DCCT-01:SIGNAL', datetime.now())
    WARNING:root:Assuming timezone for 2019-10-07 16:42:13.301672 is Europe/London
    Archive event for PV SR-DI-DCCT-01:SIGNAL: timestamp 2019-10-07
    15:42:04.876639 UTC value [301.33007915] severity 0
    >>> event.value
    array([300.77982715])
    >>> event.utc_datetime
    datetime.datetime(2019, 10, 7, 16, 2, 54, 928836, tzinfo=<UTC>)


You can also request a range of events, returning an ArchiveData object:

    >>> data = jf.get_values('SR-DI-DCCT-01:SIGNAL', utc_datetime(2018, 1, 7), utc_datetime(2018, 1, 8))
    >>> data.values
    array([[2.51189843e-03],
       [1.56371643e-03],
       [5.54392030e-04],
       ...,
       [2.77373366e+02],
       [2.77329542e+02],
       [2.77287664e+02]])
    >>> data.utc_datetimes
    array([datetime.datetime(2018, 1, 6, 23, 59, 59, 3897, tzinfo=<UTC>),
       datetime.datetime(2018, 1, 7, 0, 0, 2, 3975, tzinfo=<UTC>),
       datetime.datetime(2018, 1, 7, 0, 0, 5, 4066, tzinfo=<UTC>), ...,
       datetime.datetime(2018, 1, 7, 23, 59, 53, 3885, tzinfo=<UTC>),
       datetime.datetime(2018, 1, 7, 23, 59, 56, 3825, tzinfo=<UTC>),
       datetime.datetime(2018, 1, 7, 23, 59, 59, 3726, tzinfo=<UTC>)],
      dtype=object)
    >>> len(data)
    28764


## Development

To install development requirements:

    pip install -r requirements.txt

To run the tests:

    py.test test

To run the tests with coverage:

    py.test --cov=aa test

To run the tests with pylint checks:

    py.test --pylint dls_aa test

[![Build Status](https://travis-ci.org/dls-controls/aapy.svg?branch=travis)](https://travis-ci.org/dls-controls/aapy) [![Coverage Status](https://coveralls.io/repos/github/dls-controls/aapy/badge.svg?branch=master)](https://coveralls.io/github/dls-controls/aapy?branch=master)

Python code to retrieve data from the Archiver Appliance.

## Development

To install development requirements:

    pip install -r requirements.txt

To run the tests:

    py.test test

To run the tests with coverage:

    py.test --cov=aa test

To run the tests with pylint checks:

    py.test --pylint dls_aa test

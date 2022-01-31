aapy release notes
==================

0.12 (31st Jan 2022)
--------------------

- Switch to Github Actions for CI
- #72 Add missing filename in warning message
- #74 and #75 Add REST calls to rename PV and get
  policy list

0.11 (21st Jul 2021)
--------------------

- #69 Add REST calls to get appliance and cluster metrics

0.10 (16th Jul 2021)
--------------------

- #53 Add pv argument to get_all_pvs that allows a glob pattern.
- #68 Tweaks to README
- #67 Add support for Enum string lables.
- Added some type hints

0.9 (29th Jun 2021)
-------------------

- Python 2 is no longer supported.
- #64 Apply DLS standard package template
- #63 Check that JSON response contains data


0.8 (21st Feb 2020)
-------------------

- Fix #59 - Update order in which byte sequences are unescaped when parsing 
  PB files. This could cause an occasional failure to interpret events.


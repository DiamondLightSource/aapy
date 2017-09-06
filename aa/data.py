from . import utils
import numpy


DIFFERENT_PV_ERROR = 'All concatenated ArchiveData objects must have the same PV name'
TIMESTAMP_ERROR = ('Last timestamp in first ArchiveData object: {}\n'
                   'First timestamp in second ArchiveData object: {}')


class ArchiveEvent(object):

    DESC = 'Archive event timestamp {:%Y-%m-%d %H:%M:%S.%f} value {:.3f} severity {:.0f}'

    def __init__(self, pv, value, timestamp, severity):
        self._pv = pv
        self._value = value
        self._timestamp = timestamp
        self._severity = severity

    @property
    def pv(self):
        return self._pv

    @property
    def value(self):
        return self._value

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def severity(self):
        return self._severity

    def __str__(self):
        return ArchiveEvent.DESC.format(
            utils.epoch_to_datetime(self.timestamp),
            self.value,
            self.severity)

    def __eq__(self, other):
        return (isinstance(other, ArchiveEvent) and
                self.pv == other.pv and
                self.value == other.value and
                self.timestamp == other.timestamp and
                self.severity == other.severity)


class ArchiveData(object):

    def __init__(self, pv, values, timestamps, severities):
        assert len(values) == len(timestamps) == len(severities)
        self._pv = pv
        self._values = values
        self._timestamps = timestamps
        self._severities = severities

    @property
    def pv(self):
        return self._pv

    @property
    def values(self):
        return self._values

    @property
    def timestamps(self):
        return self._timestamps

    @property
    def severities(self):
        return self._severities

    def get_event(self, index):
        return ArchiveEvent(self.pv, self.values[index],
                            self.timestamps[index], self.severities[index])

    def append(self, other):
        assert other.pv == self.pv, DIFFERENT_PV_ERROR
        assert self.timestamps[-1] < other.timestamps[0], TIMESTAMP_ERROR.format(
            utils.epoch_to_datetime(self.timestamps[-1]),
            utils.epoch_to_datetime(other.timestamps[0])
        )
        self._values = numpy.concatenate([self.values, other.values])
        self._timestamps = numpy.concatenate([self.timestamps, other.timestamps])
        self._severities = numpy.concatenate([self.severities, other.severities])

    def __eq__(self, other):
        return (isinstance(other, ArchiveData) and
                self.pv == other.pv and
                self.values == other.values and
                self.timestamps == other.timestamps and
                self.severities == other.severities)

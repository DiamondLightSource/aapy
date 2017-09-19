from . import utils
import numpy


DIFFERENT_PV_ERROR = 'All concatenated ArchiveData objects must have the same PV name'
TIMESTAMP_ERROR = ('Last timestamp in first ArchiveData object: {}\n'
                   'First timestamp in second ArchiveData object: {}')


class ArchiveEvent(object):

    DESC = 'Archive event timestamp {:%Y-%m-%d %H:%M:%S.%f} value {} severity {:.0f}'

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
        equal = (isinstance(other, ArchiveEvent))
        equal = equal and self.pv == other.pv
        equal = equal and numpy.allclose(self.value, other.value)
        equal = equal and self.timestamp == other.timestamp
        equal = equal and self.severity == other.severity
        return equal


class ArchiveData(object):

    DESC = ('Archive data: {} events'
            ' first timestamp {:%Y-%m-%d %H:%M:%S.%f}'
            ' last timestamp {:%Y-%m-%d %H:%M:%S.%f}')

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

    def __str__(self):
        if len(self.values) == 0:
            return 'Empty archive data'
        else:
            return ArchiveData.DESC.format(len(self.values),
                utils.epoch_to_datetime(self.timestamps[0]),
                utils.epoch_to_datetime(self.timestamps[-1]))

    def __eq__(self, other):
        equal = (isinstance(other, ArchiveData))
        equal = equal and self.pv == other.pv
        equal = equal and numpy.allclose(self.values, other.values)
        equal = equal and numpy.allclose(self.timestamps, other.timestamps)
        equal = equal and numpy.array_equal(self.severities, other.severities)
        return equal

    def __iter__(self):
        for value, timestamp, severity in zip(self.values,
                                              self.timestamps,
                                              self.severities):
            yield ArchiveEvent(self.pv, value, timestamp, severity)

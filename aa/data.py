"""Objects representing data returned from the Archiver Appliance."""
import logging
import numpy
import pytz

from . import utils


DIFFERENT_PV_ERROR = 'All concatenated ArchiveData objects must have the same PV name'
TIMESTAMP_WARNING = 'Timestamps not monotonically increasing: {} -> {}'


class ArchiveEvent(object):

    DESC = ('Archive event for PV {}: '
            'timestamp {:%Y-%m-%d %H:%M:%S.%f %Z} value {} severity {:.0f}')

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

    def datetime(self, tz):
        """Returns a timezone-aware datetime for the events.

        Args:
            tz: the timezone for the datetime object

        Returns:
            datetime object

        """
        return utils.epoch_to_datetime(self._timestamp).astimezone(tz)

    @property
    def utc_datetime(self):
        """Returns a UTC datetime for the events.

        Returns:
            datetime object

        """
        return self.datetime(pytz.utc)

    @property
    def severity(self):
        return self._severity

    def __str__(self):
        return ArchiveEvent.DESC.format(
            self.pv,
            self.utc_datetime,
            self.value,
            self.severity
        )

    __repr__ = __str__

    def __eq__(self, other):
        equal = (isinstance(other, ArchiveEvent))
        equal = equal and self.pv == other.pv
        equal = equal and numpy.allclose(self.value, other.value)
        equal = equal and self.timestamp == other.timestamp
        equal = equal and self.severity == other.severity
        return equal


class ArchiveData(object):

    DESC = ('Archive data for PV {}: {} events'
            ' first timestamp {:%Y-%m-%d %H:%M:%S.%f %Z}'
            ' last timestamp {:%Y-%m-%d %H:%M:%S.%f %Z}')

    def __init__(self, pv, values, timestamps, severities):
        values = numpy.array(values)
        timestamps = numpy.array(timestamps)
        severities = numpy.array(severities)
        assert len(values) == len(timestamps) == len(severities)
        if values.ndim == 1:
            values = values.reshape((-1, 1))
        self._check_timestamps(timestamps)
        self._pv = pv
        self._values = values
        self._timestamps = timestamps
        self._severities = severities

    @staticmethod
    def _check_timestamps(ts_array):
        back_steps = numpy.diff(ts_array) < 0
        if numpy.any(back_steps):
            for nonzero_index in numpy.nditer(numpy.nonzero(back_steps)):
                logging.warning(
                    TIMESTAMP_WARNING.format(
                        utils.epoch_to_datetime(ts_array[nonzero_index]),
                        utils.epoch_to_datetime(ts_array[nonzero_index + 1])
                    )
                )

    @staticmethod
    def empty(pv):
        empty_array = numpy.zeros((0,))
        return ArchiveData(pv, empty_array, empty_array, empty_array)

    @property
    def pv(self):
        return self._pv

    @property
    def values(self):
        return self._values

    @property
    def timestamps(self):
        return self._timestamps

    def datetimes(self, tz):
        """Returns a numpy array of timezone-aware datetimes for the events.

        Args:
            tz: the timezone for the datetime objects

        Returns:
            numpy array of datetime objects

        """
        return numpy.array([
            utils.epoch_to_datetime(ts).astimezone(tz) for ts in self._timestamps
        ])

    @property
    def utc_datetimes(self):
        """Returns a numpy array of UTC datetimes for the events.

        Returns:
            numpy array of datetime objects

        """
        return self.datetimes(pytz.utc)

    @property
    def severities(self):
        return self._severities

    def get_event(self, index):
        return ArchiveEvent(self.pv, self.values[index],
                            self.timestamps[index], self.severities[index])

    def concatenate(self, other, zero_pad=False):
        """Combine two ArchiveData objects.

        Create a new object so that ArchiveData objects can be treated as
        immutable.

        Args:
            other: ArchiveData object with later timestamps
            zero_pad: if the values arrays differ in their second dimension,
                      expand the smaller to the size of the larger, padding
                      with zeros.

        Returns:
            new ArchiveData object combining self and other
        """
        assert other.pv == self.pv, DIFFERENT_PV_ERROR
        timestamps = numpy.concatenate([self.timestamps, other.timestamps])
        self._check_timestamps(timestamps)
        if zero_pad:
            first_length, first_size = self.values.shape
            second_length, second_size = other.values.shape

            target_shape = (first_length + second_length,
                            max(first_size, second_size))
            new_values = numpy.zeros(target_shape)
            new_values[:first_length, :first_size] = self.values
            new_values[first_length:, :second_size] = other.values
        else:
            new_values = numpy.concatenate([self.values, other.values])
        severities = numpy.concatenate([self.severities, other.severities])
        return ArchiveData(self.pv, new_values, timestamps, severities)

    def __str__(self):
        if not self.values.size:
            return "Empty archive data for PV '{}'".format(self.pv)
        else:
            return ArchiveData.DESC.format(
                self.pv,
                len(self.values),
                utils.epoch_to_datetime(self._timestamps[0]),
                utils.epoch_to_datetime(self._timestamps[-1])
            )

    __repr__ = __str__

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

    def __len__(self):
        return len(self.values)

    def __getitem__(self, i):
        return ArchiveEvent(self.pv,
                            self.values[i],
                            self.timestamps[i],
                            self.severities[i])


def data_from_events(pv, events, count=None):
    """Convert multiple ArchiveEvents into an ArchiveData object

    Args:
        pv: pv name for all archive events
        events: sequence of ArchiveEvent objects
        count: maximum number of events to include.  If None, return all events

    Returns:
        ArchiveData object
    """
    event_count = min(count, len(events)) if count is not None else len(events)

    # Use the first event to determine the type of array to use.
    try:
        first_event = events[0]
        if isinstance(first_event.value, utils.string23):
            wf_length = 1
            dt = numpy.dtype('U100')
        else:
            wf_length = len(first_event.value)
            dt = numpy.dtype(type(first_event.value[0]))
    except TypeError:  # Event value is not a waveform
        wf_length = 1
        dt = numpy.dtype(type(first_event.value))
    except IndexError:  # No events
        wf_length = 1
        dt = numpy.float64

    values = numpy.zeros((event_count, wf_length), dtype=dt)
    timestamps = numpy.zeros((event_count,))
    severities = numpy.zeros((event_count,))
    for i, event in enumerate(events[:event_count]):
        values[i] = event.value
        timestamps[i] = event.timestamp
        severities[i] = event.severity

    return ArchiveData(pv, values, timestamps, severities)

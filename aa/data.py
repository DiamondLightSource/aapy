"""Objects representing data returned from the Archiver Appliance."""
from __future__ import annotations

import datetime as datetime_module
import logging
import re
from collections import OrderedDict
from typing import Dict, List, Optional

import numpy
import pytz

from . import utils

__all__ = [
    "ArchiveEvent",
    "ArchiveData",
    "data_from_events",
    "parse_enum_options",
]


DIFFERENT_PV_ERROR = "All concatenated ArchiveData objects must have the same PV name"
TIMESTAMP_WARNING = "Timestamps not monotonically increasing: {} -> {}"
REGEX_ENUM = r"^ENUM_([0-9]+)$"
DTYPE_ENUM_STR = "U100"


class ArchiveEvent(object):

    """One Event, retrieved from the AA, representing a change in value of a PV"""

    DESC = (
        "Archive event for PV {}: "
        "timestamp {:%Y-%m-%d %H:%M:%S.%f %Z} value {} severity {}"
    )

    def __init__(
        self,
        pv: str,
        value: numpy.ndarray,
        timestamp: float,
        severity: float,
        enum_options: OrderedDict = OrderedDict(),
    ):
        self._pv: str = pv
        self._value: numpy.ndarray = value
        self._timestamp: float = timestamp
        self._severity: float = severity
        self._enum_options: OrderedDict = enum_options

    @property
    def pv(self) -> str:
        return self._pv

    @property
    def value(self) -> numpy.ndarray:
        return self._value

    @property
    def timestamp(self) -> float:
        return self._timestamp

    @property
    def enum_options(self) -> OrderedDict[int, str]:
        """OrderedDict containing string label for each option if this PV is an enum.

        Key is int value of PV, value is the corresponding string label.
        """
        return self._enum_options

    @property
    def has_enum_options(self) -> bool:
        """True if this PV has enum string labels available."""
        return len(self._enum_options) > 0

    @property
    def enum_string(self) -> Optional[numpy.ndarray]:
        """If PV is an enum, contains the string label of the value, if avilable"""
        return (
            lookup_enum_string(self.value, self.enum_options)
            if self.has_enum_options
            else None
        )

    def datetime(self, tz: datetime_module.tzinfo) -> datetime_module.datetime:
        """Returns a timezone-aware datetime for the events.

        Args:
            tz: the timezone for the datetime object

        Returns:
            datetime object

        """
        return utils.epoch_to_datetime(self._timestamp).astimezone(tz)

    @property
    def utc_datetime(self) -> datetime_module.datetime:
        """Returns a UTC datetime for the events.

        Returns:
            datetime object

        """
        return self.datetime(pytz.utc)

    @property
    def severity(self) -> float:
        """EPICS alarm sevirity"""
        return self._severity

    def __str__(self):
        display_val = (
            f"{self.enum_string} ({repr(self.value)})"
            if self.enum_string
            else f"{repr(self.value)}"
        )
        return ArchiveEvent.DESC.format(
            self.pv, self.utc_datetime, display_val, repr(self.severity)
        )

    __repr__ = __str__

    def __eq__(self, other):
        equal = isinstance(other, ArchiveEvent)
        equal = equal and self.pv == other.pv
        equal = equal and numpy.allclose(self.value, other.value)
        equal = equal and self.timestamp == other.timestamp
        equal = equal and self.severity == other.severity
        equal = equal and numpy.array_equal(self.enum_options, other.enum_options)

        return equal


class ArchiveData(object):

    """A collection of ArchiveEvents retireved from the AA.

    Stores the data for the result of a retrieval request. Events can be accessed:

    - By `indexing: data[0]`
    - By `get_event(index)`
    - By iterating: `for event in data:`
    - By directly accessing the arrays of values and timestamps
    """

    DESC = (
        "Archive data for PV {}: {} events"
        " first timestamp {:%Y-%m-%d %H:%M:%S.%f %Z}"
        " last timestamp {:%Y-%m-%d %H:%M:%S.%f %Z}"
    )

    def __init__(
        self,
        pv: str,
        values: numpy.ndarray,
        timestamps: numpy.ndarray,
        severities: numpy.ndarray,
        enum_options: OrderedDict[int, str] = OrderedDict(),
    ):
        assert len(values) == len(timestamps) == len(severities)
        if values.ndim == 1:
            values = values.reshape((-1, 1))
        self._check_timestamps(timestamps)
        self._pv: str = pv
        self._values: numpy.ndarray = values
        self._timestamps: numpy.ndarray = timestamps
        self._severities: numpy.ndarray = severities
        self._enum_options: OrderedDict = enum_options

    @staticmethod
    def _check_timestamps(ts_array: numpy.ndarray) -> None:
        back_steps = numpy.diff(ts_array) < 0
        if numpy.any(back_steps):
            for nonzero_index in numpy.nditer(numpy.nonzero(back_steps)):
                logging.warning(
                    TIMESTAMP_WARNING.format(
                        utils.epoch_to_datetime(ts_array[nonzero_index]),
                        utils.epoch_to_datetime(ts_array[nonzero_index + 1]),
                    )
                )

    @staticmethod
    def empty(pv: str) -> ArchiveData:
        """Returns an empty ArchiveData"""
        empty_array = numpy.zeros((0,))
        return ArchiveData(pv, empty_array, empty_array, empty_array)

    @property
    def pv(self) -> str:
        return self._pv

    @property
    def values(self) -> numpy.ndarray:
        return self._values

    @property
    def timestamps(self) -> numpy.ndarray:
        return self._timestamps

    @property
    def enum_options(self) -> OrderedDict[int, str]:
        return self._enum_options

    @property
    def has_enum_options(self) -> bool:
        return len(self._enum_options) > 0

    @property
    def enum_strings(self) -> Optional[numpy.ndarray]:
        return (
            lookup_enum_string(self.values, self.enum_options)
            if self.has_enum_options
            else None
        )

    def datetimes(self, tz: datetime_module.tzinfo) -> numpy.ndarray:
        """Returns a numpy array of timezone-aware datetimes for the events.

        Args:
            tz: the timezone for the datetime objects

        Returns:
            numpy array of datetime objects

        """
        return numpy.array(
            [utils.epoch_to_datetime(ts).astimezone(tz) for ts in self._timestamps]
        )

    @property
    def utc_datetimes(self) -> numpy.ndarray:
        """Returns a numpy array of UTC datetimes for the events.

        Returns:
            numpy array of datetime objects

        """
        return self.datetimes(pytz.utc)

    @property
    def severities(self) -> numpy.ndarray:
        return self._severities

    def get_event(self, index: int) -> ArchiveEvent:
        """Returns an ArchiveEvent for the event at the given index"""
        return ArchiveEvent(
            self.pv,
            self.values[index],
            self.timestamps[index],
            self.severities[index],
            self.enum_options,
        )

    def concatenate(self, other: ArchiveData, zero_pad: bool = False) -> ArchiveData:
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

            target_shape = (first_length + second_length, max(first_size, second_size))
            new_values = numpy.zeros(target_shape)
            new_values[:first_length, :first_size] = self.values
            new_values[first_length:, :second_size] = other.values
        else:
            new_values = numpy.concatenate([self.values, other.values])
        severities = numpy.concatenate([self.severities, other.severities])
        if self.enum_options != other.enum_options:
            logging.warning("Enum options are not the same. Using mine.")
        return ArchiveData(
            self.pv, new_values, timestamps, severities, self.enum_options
        )

    def __str__(self):
        if not self.values.size:
            return "Empty archive data for PV '{}'".format(self.pv)
        else:
            return ArchiveData.DESC.format(
                self.pv,
                len(self.values),
                utils.epoch_to_datetime(self._timestamps[0]),
                utils.epoch_to_datetime(self._timestamps[-1]),
            )

    __repr__ = __str__

    def __eq__(self, other):
        equal = isinstance(other, ArchiveData)
        equal = equal and self.pv == other.pv
        equal = equal and numpy.allclose(self.values, other.values)
        equal = equal and numpy.allclose(self.timestamps, other.timestamps)
        equal = equal and numpy.array_equal(self.severities, other.severities)
        equal = equal and self.enum_options == other.enum_options
        return equal

    def __iter__(self):
        for value, timestamp, severity in zip(
            self.values, self.timestamps, self.severities
        ):
            yield ArchiveEvent(self.pv, value, timestamp, severity, self.enum_options)

    def __len__(self):
        return len(self.values)

    def __getitem__(self, i):
        return ArchiveEvent(
            self.pv,
            self.values[i],
            self.timestamps[i],
            self.severities[i],
            self.enum_options,
        )


def data_from_events(
    pv: str,
    events: List[ArchiveEvent],
    count: int = None,
    enum_options: OrderedDict = OrderedDict(),
) -> ArchiveData:
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
    dt: numpy.dtype
    try:
        first_event = events[0]
        if isinstance(first_event.value, str):
            wf_length = 1
            dt = numpy.dtype("U100")
        else:
            wf_length = len(first_event.value)
            dt = numpy.dtype(type(first_event.value[0]))
    except TypeError:  # Event value is not a waveform
        wf_length = 1
        dt = numpy.dtype(type(first_event.value))
    except IndexError:  # No events
        wf_length = 1
        dt = numpy.dtype(numpy.float64)

    values = numpy.zeros((event_count, wf_length), dtype=dt)
    timestamps = numpy.zeros((event_count,))
    severities = numpy.zeros((event_count,))
    for i, event in enumerate(events[:event_count]):
        values[i] = event.value
        timestamps[i] = event.timestamp
        severities[i] = event.severity

    return ArchiveData(pv, values, timestamps, severities, enum_options)


def parse_enum_options(meta_dict: Dict[str, str]) -> OrderedDict[int, str]:
    """Parse enum options, if available, from a metadata dict.

    Returns
        a dict with keys of enum value (str) and values of enum string
    """
    output_dict = OrderedDict()

    for key, value in meta_dict.items():
        result = re.search(REGEX_ENUM, key)
        if result is not None:
            enum_key = int(result.group(1))
            output_dict[enum_key] = value

    return OrderedDict([(key, output_dict[key]) for key in sorted(output_dict.keys())])


def _lookup_enum_string(int_value: int, enum_options: OrderedDict[int, str]) -> str:
    """Look up the enum string for an int value; defaults to empty string."""
    return enum_options.get(int_value, "")


lookup_enum_string = numpy.vectorize(_lookup_enum_string, otypes=[DTYPE_ENUM_STR])

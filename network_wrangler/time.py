"""Module for time and timespan objects."""

from __future__ import annotations

from datetime import datetime, time
from typing import TYPE_CHECKING, Any

from .errors import TimeFormatError, TimespanFormatError
from .utils.time import duration_dt, str_to_time

if TYPE_CHECKING:
    from .models._base.types import TimeType


class Time:
    """Represents a time object.

    This class provides methods to initialize and manipulate time objects.

    Attributes:
        datetime (datetime): The underlying datetime object representing the time.
        time_str (str): The time string representation in HH:MM:SS format.
        time_sec (int): The time in seconds since midnight.

        _raw_time_in (TimeType): The raw input value used to initialize the Time object.

    """

    def __init__(self, value: TimeType):
        """Initializes a Time object.

        Args:
            value (TimeType): A time object, string in HH:MM[:SS] format, or seconds since
                midnight.

        Raises:
            TimeFormatError: If the value is not a valid time format.

        """
        if isinstance(value, datetime):
            self.datetime: datetime = value
        elif isinstance(value, time):
            self.datetime = datetime.combine(datetime.today(), value)
        elif isinstance(value, str):
            self.datetime = str_to_time(value)
        elif isinstance(value, int):
            self.datetime = datetime.datetime.fromtimestamp(value).time()
        else:
            msg = "time must be a string, int, or time object"
            raise TimeFormatError(msg)

        self._raw_time_in = value

    def __getitem__(self, item: Any) -> str:
        """Get the time string representation.

        Args:
            item (Any): Not used.

        Returns:
            str: The time string representation in HH:MM:SS format.
        """
        return self.time_str

    @property
    def time_str(self):
        """Get the time string representation.

        Returns:
            str: The time string representation in HH:MM:SS format.
        """
        return self.datetime.strftime("%H:%M:%S")

    @property
    def time_sec(self):
        """Get the time in seconds since midnight.

        Returns:
            int: The time in seconds since midnight.
        """
        return self.datetime.hour * 3600 + self.datetime.minute * 60 + self.datetime.second

    def __str__(self) -> str:
        """Get the string representation of the Time object.

        Returns:
            str: The time string representation in HH:MM:SS format.
        """
        return self.time_str

    def __hash__(self) -> int:
        """Get the hash value of the Time object.

        Returns:
            int: The hash value of the Time object.
        """
        return hash(str(self))


class Timespan:
    """Timespan object.

    This class provides methods to initialize and manipulate time objects.

    If the end_time is less than the start_time, the duration will assume that it crosses
        over midnight.

    Attributes:
        start_time (datetime.time): The start time of the timespan.
        end_time (datetime.time): The end time of the timespan.
        timespan_str_list (str): A list of start time and end time in HH:MM:SS format.
        start_time_sec (int): The start time in seconds since midnight.
        end_time_sec (int): The end time in seconds since midnight.
        duration (datetime.timedelta): The duration of the timespan.
        duration_sec (int): The duration of the timespan in seconds.

        _raw_timespan_in (Any): The raw input value used to initialize the Timespan object.

    """

    def __init__(self, value: list[TimeType]):
        """Constructor for the Timespan object.

        If the value is a list of two time strings, datetime objects, Time, or seconds from
        midnight, the start_time and end_time attributes will be set accordingly.

        Args:
            value (time): a list of two time strings, datetime objects, Time, or seconds from
              midnight.
        """
        if len(value) != 2:  # noqa: PLR2004
            msg = "timespan must be a list of 2 time strings, datetime objs, Time, or sec from midnight."
            raise TimespanFormatError(msg)

        self.start_time, self.end_time = (Time(t) for t in value)
        self._raw_timespan_in = value

    @property
    def timespan_str_list(self):
        """Get the timespan string representation."""
        return [self.start_time.time_str, self.end_time.time_str]

    @property
    def start_time_sec(self):
        """Start time in seconds since midnight."""
        return self.start_time.time_sec

    @property
    def end_time_sec(self):
        """End time in seconds since midnight."""
        return self.end_time.time_sec

    @property
    def duration(self):
        """Duration of timespan as a timedelta object."""
        return duration_dt(self.start_time, self.end_time)

    @property
    def duration_sec(self):
        """Duration of timespan in seconds.

        If end_time is less than start_time, the duration will assume that it crosses over
        midnight.
        """
        if self.end_time_sec < self.start_time_sec:
            return (24 * 3600) - self.start_time_sec + self.end_time_sec
        return self.end_time_sec - self.start_time_sec

    def __str__(self) -> str:
        """String representation of the Timespan object."""
        return str(self.timespan_str)

    def __hash__(self) -> int:
        """Hash value of the Timespan object."""
        return hash(str(self))

    def overlaps(self, other: Timespan) -> bool:
        """Check if two timespans overlap.

        If the start time is greater than the end time, the timespan is assumed to cross over
        midnight.

        Args:
            other (Timespan): The other timespan to compare.

        Returns:
            bool: True if the two timespans overlap, False otherwise.
        """
        real_end_time = self.end_time.datetime
        if self.end_time.datetime > self.start_time.datetime:
            real_end_time = self.end_time.datetime + datetime.timedelta(days=1)

        real_other_end_time = other.end_time.datetime
        if other.end_time.datetime > other.start_time.datetime:
            real_other_end_time = other.end_time.datetime + datetime.timedelta(days=1)
        return (
            self.start_time.datetime <= real_other_end_time
            and real_end_time >= other.start_time.datetime
        )

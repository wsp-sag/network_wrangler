

from datetime import datetime,time
from typing import Annotated, List, Any, Union

from pydantic import Field
from datetime import time
from typing import Annotated, List, Union
from pydantic import Field


class TimeFormatError(Exception):
    pass


class TimespanFormatError(Exception):
    pass


# fixme
TimeString = Annotated[
    str, Field(description="A time string in the format HH:MM or HH:MM:SS")
]
TimespanString = Annotated[
    List[TimeString], 
    Field(min_length=2, max_length=2,pattern="^([01]\d|2[0-3]):([0-5]\d)(:[0-5]\d)?$")
]

TimeType = Union[time, str, int]


def dt_overlaps(timespans: List[List[datetime]]) -> bool:
    """Check if any of the timespans overlap."""
    for i in range(len(timespans)):
        for j in range(i + 1, len(timespans)):
            if (timespans[i][0] < timespans[j][1]) and (
                timespans[j][0] < timespans[i][1]
            ):
                return True
    return False


def dt_overlap_duration(
    timedelta1: datetime.timedelta, timedelta2: datetime.timedelta
) -> datetime.timedelta:
    """Check if two timespans overlap and return the amount of overlap."""
    overlap_start = max(timedelta1.start_time, timedelta2.start_time)
    overlap_end = min(timedelta1.end_time, timedelta2.end_time)
    overlap_duration = max(overlap_end - overlap_start, datetime.timedelta(0))
    return overlap_duration


def dt_contains(timespan1: list[time], timespan2: list[time]) -> bool:
    """
    Check if one timespan contains another.

    Args:
        timespan1 (list[time]): The first timespan represented as a list containing the start time and end time.
        timespan2 (list[time]): The second timespan represented as a list containing the start time and end time.

    Returns:
        bool: True if the first timespan contains the second timespan, False otherwise.
    """
    start_time_dt, end_time_dt = timespan1
    start_time_dt2, end_time_dt2 = timespan2
    return (start_time_dt <= start_time_dt2) and (end_time_dt >= end_time_dt2)


def dt_overlap(timespans: List[List[datetime]]) -> bool:
    """Check if any of the timespans overlap.

    Args:
        timespans (List[List[datetime]]): A list of timespans, where each timespan is represented as a list of two datetime objects.

    Returns:
        bool: True if any of the timespans overlap, False otherwise.
    """
    for i in range(len(timespans)):
        for j in range(i + 1, len(timespans)):
            if dt_overlaps(
                timespans[i][0], timespans[i][1], timespans[j][0], timespans[j][1]
            ):
                return True
    return False


def _str_to_time(time: str) -> time:
    if len(time.split(":")) == 2:
        return datetime.datetime.strptime(time, "%H:%M").time()
    elif len(time.split(":")) == 3:
        return datetime.datetime.strptime(time, "%H:%M:%S").time()
    else:
        raise TimeFormatError("time strings must be in the format HH:MM or HH:MM:SS")


def _duration_dt(start_time_dt: time, end_time_dt: time) -> datetime.timedelta:
    """Returns a datetime.timedelta object representing the duration of the timespan.

    If end_time is less than start_time, the duration will assume that it crosses over
    midnight.
    """
    if end_time_dt < start_time_dt:
        return datetime.timedelta(
            hours=24 - start_time_dt.hour + end_time_dt.hour,
            minutes=end_time_dt.minute - start_time_dt.minute,
            seconds=end_time_dt.second - start_time_dt.second,
        )
    else:
        return end_time_dt - start_time_dt


class Time:
    """Represents a time object.

    This class provides methods to initialize and manipulate time objects.

    Attributes:
        datetime (datetime.time): The underlying datetime.time object representing the time.
        time_str (str): The time string representation in HH:MM:SS format.
        time_sec (int): The time in seconds since midnight.

        _raw_time_in (TimeType): The raw input value used to initialize the Time object.

    """

    def __init__(self, value: TimeType):
        """Initializes a Time object.

        Args:
            value (TimeType): A time object, string in HH:MM[:SS] format, or seconds since midnight.

        Raises:
            TimeFormatError: If the value is not a valid time format.

        """
        if value is time:
            self.datetime = value
        elif value is str:
            self.datetime = _str_to_time(value)
        elif value is int:
            self.datetime = datetime.datetime.fromtimestamp(value).time()
        else:
            raise TimeFormatError("time must be a string, int, or time object")

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
        return (
            self.datetime.hour * 3600 + self.datetime.minute * 60 + self.datetime.second
        )

    def __eq__(self, other: Any) -> bool:
        """Check if two Time objects are equal.

        Args:
            other (Any): The other object to compare.

        Returns:
            bool: True if the two Time objects are equal, False otherwise.
        """
        return Time(other).datetime == self.datetime

    def __gt__(self, other: Any) -> bool:
        """Check if the current Time object is greater than the other Time object.

        Args:
            other (Any): The other object to compare.

        Returns:
            bool: True if the current Time object is greater than the other Time object, False otherwise.
        """
        return self.datetime > Time(other).datetime

    def __ge__(self, other: Any) -> bool:
        """Check if the current Time object is greater than or equal to the other Time object.

        Args:
            other (Any): The other object to compare.

        Returns:
            bool: True if the current Time object is greater than or equal to the other Time object, False otherwise.
        """
        return self.datetime >= Time(other).datetime

    def __lt__(self, other: Any) -> bool:
        """Check if the current Time object is less than the other Time object.

        Args:
            other (Any): The other object to compare.

        Returns:
            bool: True if the current Time object is less than the other Time object, False otherwise.
        """
        return self.datetime < Time(other).datetime

    def __le__(self, other: Any) -> bool:
        """Check if the current Time object is less than or equal to the other Time object.

        Args:
            other (Any): The other object to compare.

        Returns:
            bool: True if the current Time object is less than or equal to the other Time object, False otherwise.
        """
        return self.datetime <= Time(other).datetime

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
    """imespan object.

    This class provides methods to initialize and manipulate time objects.

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
        """_summary_

        Args:
            value (time): a list of two time strings, datetime objects, Time, or seconds from
              midnight.
        """
        if len(value) != 2:
            raise TimespanFormatError(
                "timespan must be a list of 2 time strings, datetime objs, Time, or sec from midnight."
            )

        self.start_time, self.end_time = [Time(t) for t in value]
        self._raw_timespan_in = value

    @property
    def timespan_str_list(self):
        return [self.start_time.time_str, self.end_time.time_str]

    def __get__(self) -> str:
        return self.timespan_str_list

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
        return _duration_dt(self.start_time, self.end_time)

    @property
    def duration_sec(self):
        """Duration of timespan in seconds.

        If end_time is less than start_time, the duration will assume that it crosses over
        midnight.
        """
        if self.end_time_sec < self.start_time_sec:
            return (24 * 3600) - self.start_time_sec + self.end_time_sec
        else:
            return self.end_time_sec - self.start_time_sec

    def __eq__(self, other: Any) -> bool:
        return self.duration == Timespan(other).duration

    def __gt__(self, other: Any) -> bool:
        return self.duration > Timespan(other).duration

    def __ge__(self, other: Any) -> bool:
        return self.duration >= Timespan(other).duration

    def __lt__(self, other: Any) -> bool:
        return self.duration < Timespan(other).duration

    def __le__(self, other: Any) -> bool:
        return self.duration <= Timespan(other).duration

    def __str__(self) -> str:
        return str(self.timespan_str)

    def __hash__(self) -> int:
        return hash(str(self))

"""Functions related to parsing and comparing time objects and series.

Internal function terminology for timespan scopes:

- `matching`: a scope that could be applied for a given timespan combination.
    This includes the default timespan as well as scopes wholely contained within.
- `overlapping`: a timespan that fully or partially overlaps a given timespan.
    This includes the default timespan, all `matching` timespans and all timespans where
    at least one minute overlap.
- `conflicting`: a timespan that is overlapping but not matching. By definition default
     scope values are not conflicting.
- `independent` a timespan that is not overlapping.
"""

from __future__ import annotations
from datetime import datetime, timedelta, date
from typing import TYPE_CHECKING, Optional

import pandas as pd
from pydantic import validate_call

from ..logger import WranglerLogger

if TYPE_CHECKING:
    from ..models._base.types import TimespanString, TimeString


def str_to_time(time_str: TimeString, base_date: Optional[datetime.date] = None) -> datetime:
    """Convert TimeString (HH:MM<:SS>) to datetime object.

    If HH > 24, will add a day to the base_date.

    Args:
        time_str: TimeString in HH:MM:SS or HH:MM format.
        base_date: optional date to base the datetime on. Defaults to None.
            If not provided, will use today.
    """
    # Set the base date to today if not provided
    if base_date is None:
        base_date = date.today()

    # Split the time string to extract hours, minutes, and seconds
    parts = time_str.split(":")
    hours = int(parts[0])
    minutes = int(parts[1])
    seconds = int(parts[2]) if len(parts) == 3 else 0

    # Calculate total number of days to add to base_date based on hours
    days_to_add = hours // 24
    hours = hours % 24

    # Create a time object with the adjusted hours, minutes, and seconds
    adjusted_time = datetime.strptime(f"{hours:02}:{minutes:02}:{seconds:02}", "%H:%M:%S").time()

    # Combine the base date with the adjusted time and add the extra days if needed
    combined_datetime = datetime.combine(base_date, adjusted_time) + timedelta(days=days_to_add)

    return combined_datetime


def str_to_time_list(timespan: list[TimeString]) -> list[list[datetime]]:
    """Convert list of TimeStrings (HH:MM<:SS>) to list of datetime.time objects."""
    return list(map(str_to_time, timespan))


def timespan_str_list_to_dt(timespans: list[TimespanString]) -> list[list[datetime]]:
    """Convert list of TimespanStrings to list of datetime.time objects."""
    [str_to_time_list(ts) for ts in timespans]


def dt_to_seconds_from_midnight(dt: datetime) -> int:
    """Convert a datetime object to the number of seconds since midnight."""
    return (dt - dt.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()


def str_to_seconds_from_midnight(time_str: TimeString) -> int:
    """Convert a TimeString (HH:MM<:SS>) to the number of seconds since midnight."""
    dt = str_to_time(time_str)
    return dt_to_seconds_from_midnight(dt)


def seconds_from_midnight_to_str(seconds: int) -> TimeString:
    """Convert the number of seconds since midnight to a TimeString (HH:MM)."""
    return str(timedelta(seconds=seconds))


def filter_df_to_overlapping_timespans(
    orig_df: pd.DataFrame,
    query_timespan: list[TimeString],
) -> pd.DataFrame:
    """Filters dataframe for entries that have any overlap with the given query timespan.

    Args:
        orig_df: dataframe to query timespans for with `start_time` and `end_time` fields.
        query_timespan: TimespanString of format ['HH:MM','HH:MM'] to query orig_df for overlapping
            records.
    """
    q_start, q_end = str_to_time_list(query_timespan)
    # WranglerLogger.debug(f"q_start_time: {q_start}")
    # WranglerLogger.debug(f"q_end_time:{q_end}")
    # make sure start_time and end_time are datetime objects
    df_start_time = pd.to_datetime(orig_df["start_time"])
    df_end_time = pd.to_datetime(orig_df["end_time"])

    # WranglerLogger.debug(f"df_start_time: \n{df_start_time}")
    # WranglerLogger.debug(f"df_end_time: \n{df_end_time}")
    overlap_df = orig_df.loc[(df_start_time < q_end) & (df_end_time > q_start)]

    # WranglerLogger.debug(f"time overlap_df: \n{overlap_df}")
    return overlap_df


def filter_df_to_max_overlapping_timespans(
    orig_df: pd.DataFrame,
    query_timespan: list[TimeString],
    strict_match: bool = False,
    min_overlap_minutes: int = 0,
    keep_max_of_cols: list[str] = ["model_link_id"],
) -> pd.DataFrame:
    """Filters dataframe for entries that have maximum overlap with the given query timespan.

    Args:
        orig_df: dataframe to query timespans for with `start_time` and `end_time` fields.
        query_timespan: TimespanString of format ['HH:MM','HH:MM'] to query orig_df for overlapping
            records.
        strict_match: boolean indicating if the returned df should only contain
            records that fully contain the query timespan. If set to True, min_overlap_minutes
            does not apply. Defaults to False.
        min_overlap_minutes: minimum number of minutes the timespans need to overlap to keep.
            Defaults to 0.
        keep_max_of_cols: list of fields to return the maximum value of overlap for.  If None,
            will return all overlapping time periods. Defaults to `['model_link_id']`
    """
    q_start, q_end = str_to_time_list(query_timespan)

    overlap_start = orig_df["start_time"].combine(q_start, max)
    overlap_end = orig_df["end_time"].combine(q_end, min)
    orig_df["overlap_duration"] = (overlap_end - overlap_start).dt.total_seconds() / 60

    if strict_match:
        overlap_df = orig_df.loc[(orig_df.start_time <= q_start) & (orig_df.end_time >= q_end)]
    else:
        overlap_df = orig_df.loc[orig_df.overlap_duration > min_overlap_minutes]
    WranglerLogger.debug(f"overlap_df: \n{overlap_df}")
    if keep_max_of_cols:
        # keep only the maximum overlap
        idx = overlap_df.groupby(keep_max_of_cols)["overlap_duration"].idxmax()
        overlap_df = overlap_df.loc[idx]
    return overlap_df


def convert_timespan_to_start_end_dt(timespan_s: pd.Series) -> pd.DataFrame:
    """Covert a timespan string ['12:00','14:00] to start_time and end_time datetime cols in df."""
    start_time = timespan_s.apply(lambda x: str_to_time(x[0]))
    end_time = timespan_s.apply(lambda x: str_to_time(x[1]))
    return pd.DataFrame({"start_time": start_time, "end_time": end_time})


@validate_call
def dt_overlap_duration(timedelta1: timedelta, timedelta2: timedelta) -> timedelta:
    """Check if two timespans overlap and return the amount of overlap."""
    overlap_start = max(timedelta1.start_time, timedelta2.start_time)
    overlap_end = min(timedelta1.end_time, timedelta2.end_time)
    overlap_duration = max(overlap_end - overlap_start, timedelta(0))
    return overlap_duration


@validate_call
def dt_contains(timespan1: list[datetime], timespan2: list[datetime]) -> bool:
    """Check if one timespan inclusively contains another.

    Args:
        timespan1 (list[time]): The first timespan represented as a list containing the start
            time and end time.
        timespan2 (list[time]): The second timespan represented as a list containing the start
            time and end time.

    Returns:
        bool: True if the first timespan contains the second timespan, False otherwise.
    """
    start_time_dt, end_time_dt = timespan1
    start_time_dt2, end_time_dt2 = timespan2
    return (start_time_dt <= start_time_dt2) and (end_time_dt >= end_time_dt2)


@validate_call
def dt_overlaps(timespan1: list[datetime], timespan2: list[datetime]) -> bool:
    """Check if two timespans overlap.

    `overlapping`: a timespan that fully or partially overlaps a given timespan.
    This includes and all timespans where at least one minute overlap.
    """
    if (timespan1[0] < timespan2[1]) and (timespan2[0] < timespan1[1]):
        return True
    return False


def timespans_overlap(timespan1: list[TimespanString], timespan2: list[TimespanString]) -> bool:
    """Check if two timespan strings overlap.

    `overlapping`: a timespan that fully or partially overlaps a given timespan.
    This includes and all timespans where at least one minute overlap.
    """
    timespan1 = str_to_time_list(timespan1)
    timespan2 = str_to_time_list(timespan2)
    return dt_overlaps(timespan1, timespan2)


@validate_call
def filter_dt_list_to_overlaps(timespans: list[list[datetime]]) -> list[list[datetime]]:
    """Filter a list of timespans to only include those that overlap.

    `overlapping`: a timespan that fully or partially overlaps a given timespan.
    This includes and all timespans where at least one minute overlap.
    """
    overlaps = []
    for i in range(len(timespans)):
        for j in range(i + 1, len(timespans)):
            if dt_overlaps(timespans[i], timespans[j]):
                overlaps += [timespans[i], timespans[j]]

    # remove dupes
    overlaps = list(map(list, set(map(tuple, overlaps))))
    return overlaps


@validate_call
def dt_list_overlaps(timespans: list[list[datetime]]) -> bool:
    """Check if any of the timespans overlap.

    `overlapping`: a timespan that fully or partially overlaps a given timespan.
    This includes and all timespans where at least one minute overlap.
    """
    if filter_dt_list_to_overlaps(timespans):
        return True
    return False


def duration_dt(start_time_dt: datetime, end_time_dt: datetime) -> timedelta:
    """Returns a datetime.timedelta object representing the duration of the timespan.

    If end_time is less than start_time, the duration will assume that it crosses over
    midnight.
    """
    if end_time_dt < start_time_dt:
        return timedelta(
            hours=24 - start_time_dt.hour + end_time_dt.hour,
            minutes=end_time_dt.minute - start_time_dt.minute,
            seconds=end_time_dt.second - start_time_dt.second,
        )
    else:
        return end_time_dt - start_time_dt


def format_time(seconds):
    """Formats seconds into a human-friendly string for log files."""
    if seconds < 60:
        return f"{int(seconds)} seconds"
    elif seconds < 3600:
        return f"{int(seconds // 60)} minutes"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours} hours and {minutes} minutes"

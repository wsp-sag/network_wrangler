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
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import pandas as pd
from pydantic import validate_call

from ..logger import WranglerLogger

if TYPE_CHECKING:
    from ..models._base.types import TimespanString, TimeString


def str_to_time(time_str: TimeString) -> datetime:
    """Convert TimeString (HH:MM<:SS>) to datetime.time object."""
    n_days = 0
    # Convert to the next day
    hours, min_sec = time_str.split(":", 1)
    if int(hours) >= 24:
        n_days, hour_of_day = divmod(int(hours), 24)
        time_str = f"{hour_of_day}:{min_sec}"  # noqa E231

    if len(time_str.split(":")) == 2:
        base_time = datetime.strptime(time_str, "%H:%M")
    elif len(time_str.split(":")) == 3:
        base_time = datetime.strptime(time_str, "%H:%M:%S")
    else:
        from ..time import TimeFormatError

        raise TimeFormatError("time strings must be in the format HH:MM or HH:MM:SS")

    total_time = base_time
    if n_days > 0:
        total_time = base_time + timedelta(days=n_days)
    return total_time


def str_to_time_list(timespan: list[TimeString]) -> list[list[datetime]]:
    """Convert list of TimeStrings (HH:MM<:SS>) to list of datetime.time objects."""
    return list(map(str_to_time, timespan))


def timespan_str_list_to_dt(timespans: list[TimespanString]) -> list[list[datetime]]:
    """Convert list of TimespanStrings to list of datetime.time objects."""
    [str_to_time_list(ts) for ts in timespans]


def filter_df_to_overlapping_timespans(
    orig_df: pd.DataFrame,
    query_timespan: list[TimeString],
    strict_match: bool = False,
    min_overlap_minutes: int = 0,
    keep_max_of_cols: list[str] = ["model_link_id"],
) -> pd.DataFrame:
    """Filters dataframe for entries that have maximum overlap with the given query timespan.

    Args:
        orig_df: dataframe to query timespans for with `start_time` and `end_time`.
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

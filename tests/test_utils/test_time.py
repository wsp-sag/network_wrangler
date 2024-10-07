import datetime

import pandas as pd
import pytest

from network_wrangler.logger import WranglerLogger
from network_wrangler.utils.time import (
    filter_df_to_overlapping_timespans,
    str_to_time,
    timespans_overlap,
)

str_to_time_cases = [
    # Test case format: (time_str, base_date, expected_datetime)
    ("14:30:15", datetime.date(2024, 8, 12), datetime.datetime(2024, 8, 12, 14, 30, 15)),
    ("27:45:00", datetime.date(2024, 8, 12), datetime.datetime(2024, 8, 13, 3, 45, 0)),
    ("51:30:00", datetime.date(2024, 8, 12), datetime.datetime(2024, 8, 14, 3, 30, 0)),
    ("14:30", datetime.date(2024, 8, 12), datetime.datetime(2024, 8, 12, 14, 30, 0)),
    (
        "10:15",
        None,
        datetime.datetime.combine(
            datetime.date.today(), datetime.datetime.strptime("10:15", "%H:%M").time()
        ),
    ),
]


@pytest.mark.parametrize("case", str_to_time_cases)
def test_str_to_time(case):
    time_str, base_date, expected_datetime = case
    result = str_to_time(time_str, base_date)
    assert result == expected_datetime


timespans_overlap_cases = [
    # Test case format: (timespan_str, timespan_str, expected_result)
    (["14:30:15", "15:30:15"], ["14:30:15", "15:30:15"], True),  # same time is overlapping
    (["14:30:15", "16:30:15"], ["14:30:15", "15:30:15"], True),  # same start time is overlapping
    (["14:00:15", "14:30:15"], ["4:30:15", "14:30:15"], True),  # same end time is overlapping
    (["12:00:00", "13:00:00"], ["13:00:00", "14:00:00"], False),  # sequential not overlapping
    (["12:00:00", "14:00:00"], ["1:00:00", "2:00:00"], False),  # different day same time
    (["8:00:00", "10:00:00"], ["9:45:00", "11:00:00"], True),  # partial overlap, before
    (["7:00:00", "8:00:00"], ["4:00:00", "7:11:00"], True),  # partial overlap, after
]


@pytest.mark.parametrize("case", timespans_overlap_cases)
def test_timespans_overlaps(case):
    timespan1, timespan2, expected_result = case
    result = timespans_overlap(timespan1, timespan2)
    assert result == expected_result


df_overlap_cases = [
    # Test case format: (query, expected_result)
    ([["14:30:15", "15:30:15"]], [1, 2]),
    ([["12:00:00", "13:00:00"]], [4, 5]),
    ([["8:00:00", "10:00:00"], ["7:00:00", "8:00:00"]], [6, 7, 8, 9]),
    ([["4:00:00", "7:11:00"]], [7, 9]),
    ([["13:00:00", "14:00:00"]], [5, 10]),
    ([["1:00:00", "2:00:00"]], []),
    ([["1:30:15", "19:30:15"]], [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
]

overlap_df = pd.DataFrame(
    [
        [1, "14:30:15", "15:30:15"],
        [2, "14:30:17", "16:30:15"],
        [3, "14:00:15", "14:30:15"],
        [4, "12:00:00", "13:00:00"],
        [5, "12:00:00", "14:00:00"],
        [6, "8:00:00", "10:00:00"],
        [7, "7:00:00", "8:00:00"],
        [8, "9:45:00", "11:00:00"],
        [9, "4:00:00", "7:11:00"],
        [10, "13:00:00", "14:00:00"],
    ],
    columns=["id", "start_time", "end_time"],
).astype({"start_time": "datetime64[s]", "end_time": "datetime64[s]"})


@pytest.mark.parametrize("case", df_overlap_cases)
def test_filter_df_to_overlapping_timespans(case):
    query, expected_result = case
    WranglerLogger.debug(f"overlap_df: \n{overlap_df}")
    filtered_df = filter_df_to_overlapping_timespans(overlap_df, query)
    result = filtered_df["id"].tolist()
    assert result == expected_result

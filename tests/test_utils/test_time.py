import datetime
import pytest

from network_wrangler.utils.time import str_to_time


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

"""Filters and queries of a gtfs stop_times table."""

from __future__ import annotations

import pandas as pd
from pandera.typing import DataFrame

from ...logger import WranglerLogger
from ...models.gtfs.tables import (
    WranglerShapesTable,
    WranglerStopsTable,
    WranglerStopTimesTable,
    WranglerTripsTable,
)
from ...utils.models import validate_call_pyd
from .feed import (
    PickupDropoffAvailability,
    merge_shapes_to_stop_times,
    stop_count_by_trip,
)


def stop_times_for_trip_id(
    stop_times: DataFrame[WranglerStopTimesTable], trip_id: str
) -> DataFrame[WranglerStopTimesTable]:
    """Returns a stop_time records for a given trip_id."""
    stop_times = stop_times.loc[stop_times.trip_id == trip_id]
    return stop_times.sort_values(by=["stop_sequence"])


def stop_times_for_trip_ids(
    stop_times: DataFrame[WranglerStopTimesTable], trip_ids: list[str]
) -> DataFrame[WranglerStopTimesTable]:
    """Returns a stop_time records for a given list of trip_ids."""
    stop_times = stop_times.loc[stop_times.trip_id.isin(trip_ids)]
    return stop_times.sort_values(by=["stop_sequence"])


def stop_times_for_route_ids(
    stop_times: DataFrame[WranglerStopTimesTable],
    trips: DataFrame[WranglerTripsTable],
    route_ids: list[str],
) -> DataFrame[WranglerStopTimesTable]:
    """Returns a stop_time records for a list of route_ids."""
    trip_ids = trips.loc[trips.route_id.isin(route_ids)].trip_id.unique()
    return stop_times_for_trip_ids(stop_times, trip_ids)


def stop_times_for_min_stops(
    stop_times: DataFrame[WranglerStopTimesTable], min_stops: int
) -> DataFrame[WranglerStopTimesTable]:
    """Filter stop_times dataframe to only the records which have >= min_stops for the trip.

    Args:
        stop_times: stoptimestable to filter
        min_stops: minimum stops to require to keep trip in stoptimes
    """
    stop_ct_by_trip_df = stop_count_by_trip(stop_times)

    # Filter to obtain DataFrame of trips with stop counts >= min_stops
    min_stop_ct_trip_df = stop_ct_by_trip_df[stop_ct_by_trip_df.stop_count >= min_stops]
    if len(min_stop_ct_trip_df) == 0:
        msg = f"No trips meet threshold of minimum stops: {min_stops}"
        raise ValueError(msg)
    WranglerLogger.debug(
        f"Found {len(min_stop_ct_trip_df)} trips with a minimum of {min_stops} stops."
    )

    # Filter the original stop_times DataFrame to only include trips with >= min_stops
    filtered_stop_times = stop_times.merge(
        min_stop_ct_trip_df["trip_id"], on="trip_id", how="inner"
    )
    WranglerLogger.debug(
        f"Filter stop times to {len(filtered_stop_times)}/{len(stop_times)}\
            w/a minimum of {min_stops} stops."
    )

    return filtered_stop_times


def stop_times_for_stops(
    stop_times: DataFrame[WranglerStopTimesTable], stops: DataFrame[WranglerStopsTable]
) -> DataFrame[WranglerStopTimesTable]:
    """Filter stop_times dataframe to only have stop_times associated with stops records."""
    _sel_stops = stops.stop_id.unique().tolist()
    filtered_stop_times = stop_times[stop_times.stop_id.isin(_sel_stops)]
    WranglerLogger.debug(
        f"Filtered stop_times to {len(filtered_stop_times)}/{len(stop_times)} \
                         records that referenced one of {len(stops)} stops."
    )
    return filtered_stop_times


@validate_call_pyd
def stop_times_for_pickup_dropoff_trip_id(
    stop_times: DataFrame[WranglerStopTimesTable],
    trip_id: str,
    pickup_dropoff: PickupDropoffAvailability = "either",
) -> DataFrame[WranglerStopTimesTable]:
    """Filters stop_times for a given trip_id based on pickup type.

    GTFS values for pickup_type and drop_off_type"
        0 or empty - Regularly scheduled pickup/dropoff.
        1 - No pickup/dropoff available.
        2 - Must phone agency to arrange pickup/dropoff.
        3 - Must coordinate with driver to arrange pickup/dropoff.

    Args:
        stop_times: A WranglerStopTimesTable to query.
        trip_id: trip_id to get stop pattern for
        pickup_dropoff: str indicating logic for selecting stops based on pickup and dropoff
            availability at stop. Defaults to "either".
            "any": all stoptime records
            "either": either pickup_type or dropoff_type != 1
            "both": both pickup_type and dropoff_type != 1
            "pickup_only": dropoff = 1; pickup != 1
            "dropoff_only":  pickup = 1; dropoff != 1
    """
    trip_stop_pattern = stop_times_for_trip_id(stop_times, trip_id)

    if pickup_dropoff == "any":
        return trip_stop_pattern

    pickup_type_selection = {
        "either": (trip_stop_pattern.pickup_type != 1) | (trip_stop_pattern.drop_off_type != 1),
        "both": (trip_stop_pattern.pickup_type != 1) & (trip_stop_pattern.drop_off_type != 1),
        "pickup_only": (trip_stop_pattern.pickup_type != 1)
        & (trip_stop_pattern.drop_off_type == 1),
        "dropoff_only": (trip_stop_pattern.drop_off_type != 1)
        & (trip_stop_pattern.pickup_type == 1),
    }

    selection = pickup_type_selection[pickup_dropoff]
    trip_stops = trip_stop_pattern[selection]

    return trip_stops


def stop_times_for_longest_segments(
    stop_times: DataFrame[WranglerStopTimesTable],
) -> pd.DataFrame:
    """Find the longest segment of each trip_id that is in the stop_times.

    Segment ends defined based on interruptions in `stop_sequence`.
    """
    stop_times = stop_times.sort_values(by=["trip_id", "stop_sequence"])

    stop_times["prev_stop_sequence"] = stop_times.groupby("trip_id")["stop_sequence"].shift(1)
    stop_times["gap"] = (stop_times["stop_sequence"] - stop_times["prev_stop_sequence"]).ne(
        1
    ) | stop_times["prev_stop_sequence"].isna()

    stop_times["segment_id"] = stop_times["gap"].cumsum()
    # WranglerLogger.debug(f"stop_times with segment_id:\n{stop_times}")

    # Calculate the length of each segment
    segment_lengths = (
        stop_times.groupby(["trip_id", "segment_id"]).size().reset_index(name="segment_length")
    )

    # Identify the longest segment for each trip
    idx = segment_lengths.groupby("trip_id")["segment_length"].idxmax()
    longest_segments = segment_lengths.loc[idx]

    # Merge longest segment info back to stop_times
    stop_times = stop_times.merge(
        longest_segments[["trip_id", "segment_id"]],
        on=["trip_id", "segment_id"],
        how="inner",
    )

    # Drop temporary columns used for calculations
    stop_times.drop(columns=["prev_stop_sequence", "gap", "segment_id"], inplace=True)
    # WranglerLogger.debug(f"stop_timesw/longest segments:\n{stop_times}")
    return stop_times


def stop_times_for_trip_node_segment(
    stop_times: DataFrame[WranglerStopTimesTable],
    trip_id: str,
    node_id_start: int,
    node_id_end: int,
    include_start: bool = True,
    include_end: bool = True,
) -> DataFrame[WranglerStopTimesTable]:
    """Returns stop_times for a given trip_id between two nodes or with those nodes included.

    Args:
        stop_times: WranglerStopTimesTable
        trip_id: trip id to select
        node_id_start: int of the starting node
        node_id_end: int of the ending node
        include_start: bool indicating if the start node should be included in the segment.
            Defaults to True.
        include_end: bool indicating if the end node should be included in the segment.
            Defaults to True.
    """
    stop_times = stop_times_for_trip_id(stop_times, trip_id)
    start_idx = stop_times[stop_times["stop_id"] == node_id_start].index[0]
    end_idx = stop_times[stop_times["stop_id"] == node_id_end].index[0]
    if not include_start:
        start_idx += 1
    if include_end:
        end_idx += 1
    return stop_times.loc[start_idx:end_idx]


def stop_times_for_shapes(
    stop_times: DataFrame[WranglerStopTimesTable],
    shapes: DataFrame[WranglerShapesTable],
    trips: DataFrame[WranglerTripsTable],
) -> DataFrame[WranglerStopTimesTable]:
    """Filter stop_times dataframe to records associated with shapes dataframe.

    Where multiple segments of stop_times are found to match shapes, retain only the longest.

    Args:
        stop_times: stop_times dataframe to filter
        shapes: shapes dataframe to stop_times to.
        trips: trips to link stop_times to shapess

    Returns:
        filtered stop_times dataframe

    EX:
    * should be retained
    > stop_times

    trip_id   stop_sequence   stop_id
    *t1          1                  1
    *t1          2                  2
    *t1          3                  3
    t1           4                  5
    *t2          1                  1
    *t2          2                  3
    t2           3                  7

    > shapes

    shape_id   shape_pt_sequence   shape_model_node_id
    s1          1                  1
    s1          2                  2
    s1          3                  3
    s1          4                  4
    s2          1                  1
    s2          2                  2
    s2          3                  3

    > trips

    trip_id   shape_id
    t1          s1
    t2          s2
    """
    """
    > stop_times_w_shapes

    trip_id   stop_sequence   stop_id    shape_id   shape_pt_sequence
    *t1          1                  1        s1          1
    *t1          2                  2        s1          2
    *t1          3                  3        s1          3
    t1           4                  5        NA          NA
    *t2          1                  1        s2          1
    *t2          2                  3        s2          2
    t2           3                  7        NA          NA

    """
    stop_times_w_shapes = merge_shapes_to_stop_times(stop_times, shapes, trips)
    # WranglerLogger.debug(f"stop_times_w_shapes :\n{stop_times_w_shapes}")
    """
    > stop_times_w_shapes

    trip_id   stop_sequence   stop_id   shape_id   shape_pt_sequence
    *t1          1               1        s1          1
    *t1          2               2        s1          2
    *t1          3               3        s1          3
    *t2          1               1        s2          1
    *t2          2               3        s2          2

    """
    filtered_stop_times = stop_times_w_shapes[stop_times_w_shapes["shape_pt_sequence"].notna()]
    # WranglerLogger.debug(f"filtered_stop_times:\n{filtered_stop_times}")

    # Filter out any stop_times the shape_pt_sequence is not ascending
    valid_stop_times = filtered_stop_times.groupby("trip_id").filter(
        lambda x: x["shape_pt_sequence"].is_monotonic_increasing
    )
    # WranglerLogger.debug(f"valid_stop_times:\n{valid_stop_times}")

    valid_stop_times = valid_stop_times.drop(columns=["shape_id", "shape_pt_sequence"])

    longest_valid_stop_times = stop_times_for_longest_segments(valid_stop_times)
    longest_valid_stop_times = longest_valid_stop_times.reset_index(drop=True)

    return longest_valid_stop_times

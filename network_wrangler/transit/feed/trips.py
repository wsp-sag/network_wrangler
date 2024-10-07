"""Filters and queries of a gtfs trips table and trip_ids."""

from pandera.typing import DataFrame

from ...logger import WranglerLogger
from ...models.gtfs.tables import WranglerStopTimesTable, WranglerTripsTable


def trips_for_shape_id(
    trips: DataFrame[WranglerTripsTable], shape_id: str
) -> DataFrame[WranglerTripsTable]:
    """Returns a trips records for a given shape_id."""
    return trips.loc[trips.shape_id == shape_id]


def trip_ids_for_shape_id(trips: DataFrame[WranglerTripsTable], shape_id: str) -> list[str]:
    """Returns a list of trip_ids for a given shape_id."""
    return trips_for_shape_id(trips, shape_id)["trip_id"].unique().tolist()


def trips_for_stop_times(
    trips: DataFrame[WranglerTripsTable], stop_times: DataFrame[WranglerStopTimesTable]
) -> DataFrame[WranglerTripsTable]:
    """Filter trips dataframe to records associated with stop_time records."""
    _sel_trips = stop_times.trip_id.unique().tolist()
    filtered_trips = trips[trips.trip_id.isin(_sel_trips)]
    WranglerLogger.debug(
        f"Filtered trips to {len(filtered_trips)}/{len(trips)} \
                         records that referenced one of {len(stop_times)} stop_times."
    )
    return filtered_trips

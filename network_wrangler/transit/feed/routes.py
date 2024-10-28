"""Filters and queries of a gtfs routes table and route_ids."""

from __future__ import annotations

from pandera.typing import DataFrame

from ...logger import WranglerLogger
from ...models.gtfs.tables import RoutesTable, WranglerTripsTable


def route_ids_for_trip_ids(trips: DataFrame[WranglerTripsTable], trip_ids: list[str]) -> list[str]:
    """Returns route ids for given list of trip_ids."""
    return trips[trips["trip_id"].isin(trip_ids)].route_id.unique().tolist()


def routes_for_trips(
    routes: DataFrame[RoutesTable], trips: DataFrame[WranglerTripsTable]
) -> DataFrame[RoutesTable]:
    """Filter routes dataframe to records associated with trip records."""
    _sel_routes = trips.route_id.unique().tolist()
    filtered_routes = routes[routes.route_id.isin(_sel_routes)]
    WranglerLogger.debug(
        f"Filtered routes to {len(filtered_routes)}/{len(routes)} \
                         records that referenced one of {len(trips)} trips."
    )
    return filtered_routes


def routes_for_trip_ids(
    routes: DataFrame[RoutesTable], trips: DataFrame[WranglerTripsTable], trip_ids: list[str]
) -> DataFrame[RoutesTable]:
    """Returns route records for given list of trip_ids."""
    route_ids = route_ids_for_trip_ids(trips, trip_ids)
    return routes.loc[routes.route_id.isin(route_ids)]

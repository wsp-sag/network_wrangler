"""Data Model for Pure GTFS Feed (not wrangler-flavored)."""

from typing import ClassVar

from ...models._base.db import DBModelMixin
from .tables import (
    AgenciesTable,
    FrequenciesTable,
    RoutesTable,
    ShapesTable,
    StopsTable,
    StopTimesTable,
    TripsTable,
)


class GtfsValidationError(Exception):
    """Exception raised for errors in the GTFS feed."""


class GtfsModel(DBModelMixin):
    """Wrapper class around GTFS feed.

    Most functionality derives from mixin class DBModelMixin which provides:
    - validation of tables to schemas when setting a table attribute (e.g. self.trips = trips_df)
    - validation of fks when setting a table attribute (e.g. self.trips = trips_df)
    - hashing and deep copy functionality
    - convenience methods for accessing tables

    Attributes:
        table_names: list of table names in GTFS feed.
        tables: list tables as dataframes.
        stop_times: stop_times dataframe with roadway node_ids
        stops: stops dataframe
        shapes: shapes dataframe
        trips: trips dataframe
        frequencies: frequencies dataframe
        routes: route dataframe
        net: TransitNetwork object
    """

    # the ordering here matters because the stops need to be added before stop_times if
    # stop times needs to be converted
    _table_models: ClassVar[dict] = {
        "agencies": AgenciesTable,
        "frequencies": FrequenciesTable,
        "routes": RoutesTable,
        "shapes": ShapesTable,
        "stops": StopsTable,
        "trips": TripsTable,
        "stop_times": StopTimesTable,
    }

    table_names: ClassVar[list[str]] = [
        "frequencies",
        "routes",
        "shapes",
        "stops",
        "trips",
        "stop_times",
    ]

    optional_table_names: ClassVar[list[str]] = ["agencies"]

    def __init__(self, **kwargs):
        """Initialize GTFS model."""
        self.initialize_tables(**kwargs)

        # Set extra provided attributes.
        extra_attr = {k: v for k, v in kwargs.items() if k not in self.table_names}
        for k, v in extra_attr:
            self.__setattr__(k, v)

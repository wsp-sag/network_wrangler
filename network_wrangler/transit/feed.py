import copy
import hashlib
import shutil
import tempfile
import weakref

from typing import Union
from pathlib import Path

import pandas as pd
from pandera.errors import SchemaErrors
from pandera.decorators import check_input, check_output
from networkx import DiGraph

from ..models.gtfs.tables import (
    AgenciesTable,
    StopsTable,
    RoutesTable,
    TripsTable,
    StopTimesTable,
    ShapesTable,
    FrequenciesTable
)
from ..models._base import DBModel
from ..models._base import RecordModel

from ..utils import fk_in_pk, update_df_by_col_value
from ..logger import WranglerLogger


# Raised when there is an issue reading the GTFS feed.
class FeedReadError(Exception):
    pass


# Raised when there is an issue with the validation of the GTFS data.
class FeedValidationError(Exception):
    pass


HASH_TABLES = ["frequencies", "routes", "shapes", "stop_times", "stops", "trips"]
TABLE_SCHEMAS = {
    "frequencies": FrequenciesTable,
    "routes": RoutesTable,
    "trips": TripsTable,
    "stops": StopsTable,
    "shapes": ShapesTable,
    "stop_times": StopTimesTable,
}


class Feed(RecordModel, DBModel):
    """
    Wrapper class around GTFS feed.

    Attributes:
        table_names: list of table names in GTFS feed.
        tables: list tables as dataframes.
        table_schemas: Dictionary mapping feed tables to a Pandera DataFrameSchema.
        valid: True if all specified tables match schemas and foreign keys valid.
        schemas_valid: True if all specified tables match schemas
        foreign_keys_valid: True if all foreign keys exist in primary key table.
        stop_times: stop_times dataframe with roadway node_ids
        stops: stops dataframe
        shapes: shapes dataframe
        trips: trips dataframe
        frequencies: frequencies dataframe
        routes: route dataframe
        net: TransitNetwork object
        feed_hash: hash representing the contents of the talbes in HASH_TQBLES
        stops_node_id: convenience reference to field in stops table that references roadway
            model_node_id
        shapes_node_id: convenience reference to field in shapes table that references roadway
            model_node_id
    """

    def __init__(self, feed_dfs, table_schemas=TABLE_SCHEMAS):
        """Constructor for GTFS Feed.

        Initializes a GTFS Feed object with the given feed dataframes and schemas.

        Args:
            feed_dfs (duct): A dictionary mapping the table name to a dataframe for
                each GTFS table. Example:

                ```python
                {
                    "frequencies": frequencies_df,
                    "trips": trips_df,
                    "stops": stops_df,
                    ...
                ```

            table_schemas (dict): Dictionary mapping feed tables to a Pandera DataFrameSchema.
                Defaults to TABLE_SCHEMAS.
        """
        WranglerLogger.debug("Creating transit feed.")
        _missing_ts = [t for t in HASH_TABLES if t not in feed_dfs]
        if _missing_ts:
            WranglerLogger.warning(
                f"Missing {len(_missing_ts)} required in tables feed_dfs:\
                                    {_missing_ts} "
            )

        self._net = None
        self.table_schemas = table_schemas
        self.table_names = []

        for table, df in feed_dfs.items():
            setattr(self, table, self.validate_df_as_table(table, df))
            self.table_names.append(table)

        assert self.foreign_keys_valid
        WranglerLogger.debug("Created valid transit feed.")

    def __deepcopy__(self, memo):
        """Custom implementation of __deepcopy__ method.

        This method is called by copy.deepcopy() to create a deep copy of the object.


        Args:
            memo (dict): Dictionary to track objects already copied during deepcopy.

        Returns:
            Feed: A deep copy of the Feed object.
        """
        # Create a new, empty instance of the Feed class
        new_feed = self.__class__.__new__(self.__class__)

        # Copy all attributes to the new instance
        for attr_name, attr_value in self.__dict__.items():
            # Use copy.deepcopy to create deep copies of mutable objects
            if isinstance(attr_value, pd.DataFrame):
                setattr(new_feed, attr_name, copy.deepcopy(attr_value, memo))
            else:
                setattr(new_feed, attr_name, attr_value)

        WranglerLogger.warning(
            "Creating a deep copy of Transit Feed.\
            This will NOT update the reference from TransitNetwork.feed."
        )

        # Return the newly created deep copy of the Feed object
        return new_feed

    def deepcopy(self):
        return copy.deepcopy(self)

    @property
    def tables(self) -> list[pd.DataFrame]:
        """
        Returns list of tables.
        """
        return [self.get(table) for table in self.table_names]

    @property
    def schemas_valid(self) -> bool:
        _schema_validations = {
            _table: self.validate_df_as_table(_table, self.get(_table))
            for _table in self.table_names
        }
        _invalid_schemas = [t for t, v in _schema_validations.items() if not v]
        if _invalid_schemas:
            WranglerLogger.warning(
                f"!!! Following transit feed schemas invalid: {','.join(_invalid_schemas)}"
            )
        schemas_valid = not bool(_invalid_schemas)
        return schemas_valid

    @property
    def valid(self) -> bool:
        """
        Returns True iff all specified tables match schemas and foreign keys valid.
        """
        return self.foreign_keys_valid & self.schemas_valid

    def get(self, table: str) -> pd.DataFrame:
        """Get table by name.

        args:
            table: table name. e.g. frequencies, stops, etc.
        """
        df = getattr(self, table)
        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"Couldn't find valid table: {table}")

        return df

    @property
    def stop_times(self):
        """If roadway node_ids aren't all there, will merge in roadway node_ids from stops.txt."""
        if not self.net:
            return self._stop_times
        if self.stops_node_id in self._stop_times.columns:
            if self._stop_times[self.stops_node_id].isna().any():
                WranglerLogger.debug(f"Removing stoptimes nodes col.")
                self._stop_times = self._stop_times.drop(columns=[self.stops_node_id])

        if self.stops_node_id not in self._stop_times.columns:
            self._stop_times = self._stop_times.merge(
                self.net.feed.stops[["stop_id", self.stops_node_id]],
                on="stop_id",
                how="left",
            )
        self._stop_times = StopTimesTable.validate(self._stop_times, lazy=True)
        return self._stop_times

    @stop_times.setter
    def stop_times(self, df):
        try:
            df = StopTimesTable.validate(df, lazy=True)
        except SchemaErrors as err:
            raise FeedValidationError(f"Invalid stops.txt:\n {err.failure_cases}")
        self._stop_times = df

    @property
    def stops(self):
        return self._stops

    @stops.setter
    def stops(self, value):
        try:
            df = StopsTable.validate(value, lazy=True)
        except SchemaErrors as err:
            WranglerLogger.error(f"Invalid stops.txt:\n {err.failure_cases}")
            raise FeedValidationError("Invalid stops.txt")
        self._stops = df

    @property
    def shapes(self):
        return self._shapes

    @shapes.setter
    def shapes(self, value):
        try:
            df = ShapesTable.validate(value, lazy=True)
        except SchemaErrors as err:
            raise FeedValidationError(f"Invalid shapes.txt:\n {err.failure_cases}")
        self._shapes = df

    @property
    def trips(self):
        return self._trips

    @trips.setter
    def trips(self, value):
        try:
            df = TripsTable.validate(value, lazy=True)
        except SchemaErrors as err:
            raise FeedValidationError(f"Invalid trips.txt:\n {err.failure_cases}")
        self._trips = df

    @property
    def frequencies(self):
        return self._frequencies

    @frequencies.setter
    def frequencies(self, value):
        try:
            df = FrequenciesTable.validate(value, lazy=True)
        except SchemaErrors as err:
            raise FeedValidationError(f"Invalid frequencies.txt:\n {err.failure_cases}")
        self._frequencies = df

    @property
    def routes(self):
        return self._routes

    @routes.setter
    def routes(self, value):
        try:
            df = RoutesTable.validate(value, lazy=True)
        except SchemaErrors as err:
            raise FeedValidationError(f"Invalid routes.txt:\n {err.failure_cases}")
        self._routes = df

    def set_by_id(
        self,
        table_name: str,
        set_df: pd.DataFrame,
        id_property: str = "trip_id",
        properties: list[str] = None,
    ):
        """
        Set property values in a specific table for a list of IDs.

        Args:
            table_name (str): Name of the table to modify.
            set_df (pd.DataFrame): DataFrame with columns 'trip_id' and 'value' containing
                trip IDs and values to set for the specified property.
            id_property: Property to use as ID to set by. Defaults to "trip_id.
            properties: List of properties to set which are in set_df. If not specified, will set
                all properties.
        """
        table_df = self.get(table_name)
        updated_df = update_df_by_col_value(
            table_df, set_df, id_property, properties=properties
        )
        self.__dict__[table_name] = updated_df

    def validate_df_as_table(self, table: str, df: pd.DataFrame) -> bool:
        """Validate a dataframe as a table: relevant schemas and foreign keys.

        Args:
            table (str): table name. e.g. frequencies, stops, etc.
            df (pd.DataFrame): dataframe to be validated as that table
        """
        if table in self.table_schemas:
            # set to lazy so that all errors in each table found before returning
            try:
                df = self.table_schemas[table].validate(df, lazy=True)
            except SchemaErrors as err:
                WranglerLogger.error(f"Invalid {table}:\n {err.failure_cases}")
                raise FeedValidationError(f"Invalid {table}")

        else:
            WranglerLogger.debug(
                f"{table} requested but no schema available to validate."
            )
        return df

    @property
    def net(self):
        if self._net == None:
            WranglerLogger.warning("Feed.net called, but is not set.")
        return self._net

    @net.setter
    def net(self, net: "TransitNetwork"):
        self._net = net
        self._net._feed = self

    @property
    def stops_node_id(self):
        return self.net.TRANSIT_FOREIGN_KEYS_TO_ROADWAY["stops"]["nodes"][0]

    @property
    def shapes_node_id(self):
        return self.net.TRANSIT_FOREIGN_KEYS_TO_ROADWAY["shapes"]["nodes"][0]

    @property
    def foreign_keys_valid(self) -> bool:
        """Boolean indiciating if all foreign keys exist in primary key table."""
        return self.validate_fks()

    def validate_table_fks(
        self, table: str, df: pd.DataFrame = None, _raise_error: bool = True
    ) -> tuple[bool, list]:
        """Validates the foreign keys of a specific table.

        Args:
            table (str): Table name (i.e. routes, stops, etc)
            df (pd.DataFrame, optional): Dataframe to use as that table. If left to default of
                None, will retrieve the table using table name using `self.feed.get(<table>)`.

        Returns:
            tuple[bool, list]: A tuple where the first value is a boolean representing if foreign
                keys are valid.  The second value is a list of foreign keys that are missing from
                the referenced table.
        """
        _fks = self.INTRA_FEED_FOREIGN_KEYS.get(table, {})
        table_valid = True
        table_fk_missing = []
        if not _fks:
            return True, []
        if df is None:
            df = self.get(table)
        for _fk_field, (_pk_table, _pk_field) in _fks.items():
            if _fk_field not in df.columns:
                continue

            _pk_s = self.get(_pk_table)[_pk_field]
            _fk_s = df[_fk_field]
            _valid, _missing = fk_in_pk(_pk_s, _fk_s)
            table_valid = table_valid and _valid
            if _missing:
                table_fk_missing.append(
                    f"{_pk_table}.{_pk_field} missing values from {table}.{_fk_field}\
                    :{_missing}"
                )
        if _raise_error and table_fk_missing:
            WranglerLogger.error(table_fk_missing)
            raise FeedValidationError("{table} missing Foreign Keys.")
        return table_valid, table_fk_missing

    def validate_fks(self) -> bool:
        """
        Validates the foreign keys of all the tables in the feed.

        Returns:
            bool: If true, all tables in feed have valid foreign keys.
        """
        all_valid = True
        missing = []
        for df in self.tables:
            _valid, _missing = self.validate_table_fks(df, _raise_error=False)
            all_valid = all_valid and _valid
            if _missing:
                missing += _missing

        if missing:
            WranglerLogger.error(missing)
            raise FeedValidationError("Missing Foreign Keys.")
        return all_valid

    @property
    def feed_hash(self) -> str:
        """A hash representing the contents of the talbes in HASH_TABLES."""
        _table_hashes = [self.get(t).df_hash() for t in HASH_TABLES]
        _value = str.encode("-".join(_table_hashes))

        _hash = hashlib.sha256(_value).hexdigest()
        return _hash

    def tables_with_property(self, property: str) -> list[str]:
        """
        Returns feed tables in the feed which contain the property.

        arg:
            property: name of property to search for tables with
        """

        return [t for t in self.table_names if property in self.get(t).columns]

    @check_output(TripsTable, inplace=True)
    def trips_with_shape_id(self, shape_id: str) -> pd.DataFrame:
        trips_df = self.get("trips")

        return trips_df.loc[trips_df.shape_id == shape_id]

    @check_output(StopTimesTable, inplace=True)
    def trip_stop_times(self, trip_id: str) -> pd.DataFrame:
        """Returns a stop_time records for a given trip_id.

        args:
            trip_id: trip_id to get stop pattern for
        """
        stop_times_df = self.stop_times

        return stop_times_df.loc[stop_times_df.trip_id == trip_id]

    def trip_shape_id(self, trip_id: str) -> str:
        """Returns a shape_id for a given trip_id.

        args:
            trip_id: trip_id to get stop pattern for
        """
        trips_df = self.get("trips")
        return trips_df.loc[trips_df.trip_id == trip_id, "shape_id"].values[0]

    def trip_shape(self, trip_id: str) -> pd.DataFrame:
        """Returns a shape records for a given trip_id.

        args:
            trip_id: trip_id to get stop pattern for
        """
        shape_id = self.trip_shape_id(trip_id)
        shapes_df = self.get("shapes")
        return shapes_df.loc[shapes_df.shape_id == shape_id]

    @check_output(StopsTable, inplace=True)
    def trip_stops(self, trip_id: str, pickup_type: str = "either") -> list[str]:
        """Returns stops.txt which are used for a given trip_id"""
        stop_ids = self.trip_stop_pattern(trip_id, pickup_type=pickup_type)
        return self.stops.loc[self.stops.isin(stop_ids)]

    def shape_node_pattern(self, shape_id: str) -> list[int]:
        """Returns node pattern of a shape.

        args:
            shape_id: string identifier of the shape.
        """
        shape_df = self.shapes.loc[self.shapes["shape_id"] == shape_id]
        shape_df = shape_df.sort_values(by=["shape_pt_sequence"])
        return shape_df[self.shapes_node_id].to_list()

    def shape_with_trip_stops(
        self, trip_id: str, pickup_type: str = "either"
    ) -> pd.DataFrame:
        """Returns shapes.txt for a given trip_id with the stop_id added based on pickup_type.

        args:
            trip_id: trip id to select
            pickup_type: str indicating logic for selecting stops based on piackup and dropoff
                availability at stop. Defaults to "either".
                "either": either pickup_type or dropoff_type > 0
                "both": both pickup_type or dropoff_type > 0
                "pickup_only": only pickup > 0
                "dropoff_only": only dropoff > 0

        """

        shapes = self.trip_shape(trip_id)
        trip_stop_times = self.trip_stop_times_for_pickup_type(
            trip_id, pickup_type=pickup_type
        )

        stop_times_cols = [
            "stop_id",
            "trip_id",
            "pickup_type",
            "drop_off_type",
            self.stops_node_id,
        ]
        shape_with_trip_stops = shapes.merge(
            trip_stop_times[stop_times_cols],
            how="left",
            right_on=self.stops_node_id,
            left_on=self.shapes_node_id,
        )
        shape_with_trip_stops = shape_with_trip_stops.sort_values(
            by=["shape_pt_sequence"]
        )
        return shape_with_trip_stops

    def stops_node_id_from_stop_id(
        self, stop_id: Union[list[str], str]
    ) -> Union[list[int], int]:
        """Returns node_ids from one or more stop_ids.

        stop_id: a stop_id string or a list of stop_id strings
        """
        if isinstance(stop_id, list):
            return [self.stops_node_id_from_stop_id(s) for s in stop_id]
        elif isinstance(stop_id, str):
            stops = self.get("stops")
            return stops.at[stops["stop_id"] == stop_id, self.stops_node_id]
        raise ValueError(
            f"Expecting list of strings or string for stop_id; got {type(stop_id)}"
        )

    @check_output(StopTimesSchema, inplace=True)
    def trip_stop_times_for_pickup_type(
        self, trip_id: str, pickup_type: str = "either"
    ) -> list[str]:
        """Returns stop_times for a given trip_id based on pickup type.

        GTFS values for pickup_type and drop_off_type"
            0 or empty - Regularly scheduled pickup/dropoff.
            1 - No pickup/dropoff available.
            2 - Must phone agency to arrange pickup/dropoff.
            3 - Must coordinate with driver to arrange pickup/dropoff.

        args:
            trip_id: trip_id to get stop pattern for
            pickup_type: str indicating logic for selecting stops based on piackup and dropoff
                availability at stop. Defaults to "either".
                "either": either pickup_type or dropoff_type != 1
                "both": both pickup_type and dropoff_type != 1
                "pickup_only": dropoff = 1; pickup != 1
                "dropoff_only":  pickup = 1; dropoff != 1


        """
        trip_stop_pattern = self.trip_stop_times(trip_id)

        pickup_type_selection = {
            "either": (trip_stop_pattern.pickup_type != 1)
            | (trip_stop_pattern.drop_off_type != 1),
            "both": (trip_stop_pattern.pickup_type != 1)
            & (trip_stop_pattern.drop_off_type != 1),
            "pickup_only": (trip_stop_pattern.pickup_type != 1)
            & (trip_stop_pattern.drop_off_type == 1),
            "dropoff_only": (trip_stop_pattern.drop_off_type != 1)
            & (trip_stop_pattern.pickup_type == 1),
        }

        selection = pickup_type_selection[pickup_type]
        trip_stops = trip_stop_pattern[selection]

        return trip_stops

    def node_is_stop(
        self, node_id: Union[int, list[int]], trip_id: str, pickup_type: str = "either"
    ) -> Union[bool, list[bool]]:
        """Returns a boolean indicating if a node (or a list of nodes) is (are) stops for a given trip_id.

        args:
            node_id: node ID for roadway
            trip_id: trip_id to get stop pattern for
            pickup_type: str indicating logic for selecting stops based on piackup and dropoff
                availability at stop. Defaults to "either".
                "either": either pickup_type or dropoff_type > 0
                "both": both pickup_type or dropoff_type > 0
                "pickup_only": only pickup > 0
                "dropoff_only": only dropoff > 0
        """
        trip_stop_nodes = self.trip_stops(trip_id, pickup_type=pickup_type)[
            self.stops_node_id
        ]
        if isinstance(node_id, list):
            return [n in trip_stop_nodes.values for n in node_id]
        return node_id in trip_stop_nodes.values

    def trip_stop_pattern(self, trip_id: str, pickup_type: str = "either") -> list[str]:
        """Returns a stop pattern for a given trip_id given by a list of stop_ids.

        args:
            trip_id: trip_id to get stop pattern for
            pickup_type: str indicating logic for selecting stops based on piackup and dropoff
                availability at stop. Defaults to "either".
                "either": either pickup_type or dropoff_type > 0
                "both": both pickup_type or dropoff_type > 0
                "pickup_only": only pickup > 0
                "dropoff_only": only dropoff > 0
        """
        trip_stops = self.trip_stop_times_for_pickup_type(
            trip_id, pickup_type=pickup_type
        )
        return trip_stops.stop_id.to_list()

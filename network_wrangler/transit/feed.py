import copy
import hashlib

from typing import Union
from pathlib import Path

import pandas as pd
import partridge as ptg

from pandera.decorators import check_input, check_output
from networkx import DiGraph
from partridge.config import default_config

from .schemas import (
    FrequenciesSchema,
    StopsSchema,
    RoutesSchema,
    TripsSchema,
    ShapesSchema,
    StopTimesSchema,
)
from ..utils import fk_in_pk, update_df_by_col_value
from ..logger import WranglerLogger


# Raised when there is an issue reading the GTFS feed.
class FeedReadError(Exception):
    pass


# Raised when there is an issue with the validation of the GTFS data.
class FeedValidationError(Exception):
    pass


class Feed:
    """
    Wrapper class around GTFS feed to allow abstraction from partridge.

    TODO: Replace usage of partridge
    """

    # A list of GTFS files that are required to be present in the feed.
    REQUIRED_FILES = [
        "agency.txt",
        "frequencies.txt",
        "routes.txt",
        "shapes.txt",
        "stop_times.txt",
        "stops.txt",
        "trips.txt",
    ]

    # Dictionary mapping table names to their corresponding schema classes for validation purposes.
    SCHEMAS = {
        "frequencies": FrequenciesSchema,
        "routes": RoutesSchema,
        "trips": TripsSchema,
        "stops": StopsSchema,
        "shapes": ShapesSchema,
        "stop_times": StopTimesSchema,
    }

    # List of table names used for calculating a hash representing the content of the entire feed.
    TABLES_IN_FEED_HASH = [
        "frequencies",
        "routes",
        "shapes",
        "stop_times",
        "stops",
        "trips",
    ]

    """
    Mapping of foreign keys in the transit network which refer to primary keys in the highway
    Network.
    """
    INTRA_FEED_FOREIGN_KEYS = {
        "stop_times": {"trip_id": ("trips", "trip_id"), "stop_id": ("stop", "stop_id")},
        "frequencies": {"trip_id": ("trips", "trip_id")},
        "trips": {
            "route_id": ("route", "route_id"),
            "shape_id": ("shapes", "shape_id"),
        },
        "stops": {"parent_station": ("stop", "stop_id")},
        "transfers": {
            "from_stop_id": ("stop", "stop_id"),
            "to_stop_id": ("stop", "stop_id"),
        },
        "pathways": {
            "from_stop_id": ("stop", "stop_id"),
            "to_stop_id": ("stop", "stop_id"),
        },
    }

    def __init__(self, feed_path: Union[Path, str]):
        """Constructor for GTFS Feed.

        Updates partridge config based on which files are available.
        Validates each table to schemas in SCHEMAS as they are read.
        Validates foreign keys after all tables read in.

        Args:
            feed_path (Union[Path,str]): Path of GTFS feed files.
        """
        WranglerLogger.info(f"Creating transit feed from: {feed_path}")

        self.feed_path = feed_path
        self._ptg_feed = ptg.load_feed(feed_path)
        self._config = self._config_from_files(self._ptg_feed)
        self._net = None

        for node in self.config.nodes.keys():
            _table = node.replace(".txt", "")
            WranglerLogger.debug(f"...setting {_table}")
            setattr(self, _table, self._read_from_file(node))

        assert self.foreign_keys_valid

        msg = f"Read GTFS feed tables from {self.feed_path}:\n- " + "\n- ".join(
            self.config.nodes.keys()
        )
        WranglerLogger.info(msg)

    @property
    def config(self) -> DiGraph:
        """
        Internal configuration of the GTFS feed.
        """
        return self._config

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
    def table_names(self) -> list[str]:
        """
        Returns list of tables from config.
        """
        return list(t.replace(".txt", "") for t in self.config.nodes.keys())

    @property
    def tables(self) -> list[pd.DataFrame]:
        """
        Returns list of tables from config.
        """
        return [self.get(table_name) for table_name in self.config.nodes.keys()]

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

    def _read_from_file(self, node: str) -> pd.DataFrame:
        """Read node from file + validate to schema if table name in SCHEMAS and return dataframe.

        Args:
            node (str): name of feed file, e.g. `stops.txt`

        Returns:
            pd.DataFrame: Dataframe of file.
        """
        _table = node.replace(".txt", "")
        df = self._ptg_feed.get(node)
        df = self.validate_df_as_table(_table, df)
        return df.copy()

    def get(self, table: str) -> pd.DataFrame:
        """Get table by name.

        args:
            table: table name. e.g. frequencies, stops, etc.
        """
        table = table.replace(".txt", "")
        df = getattr(self,table)
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
        self._stop_times = StopTimesSchema.validate(self._stop_times, lazy=True)
        return self._stop_times

    @stop_times.setter
    def stop_times(self, df):
        df = StopTimesSchema.validate(df, lazy=True)
        WranglerLogger.warning("SETTING STOPTIMES")
        self._stop_times = df

    @property
    def stops(self):
        return self._stops

    @stops.setter
    def stops(self, value):
        df = StopsSchema.validate(value, lazy=True)
        self._stops = df

    @property
    def shapes(self):
        return self._shapes

    @shapes.setter
    def shapes(self, value):
        df = ShapesSchema.validate(value, lazy=True)
        self._shapes = df

    @property
    def trips(self):
        return self._trips

    @trips.setter
    def trips(self, value):
        df = TripsSchema.validate(value, lazy=True)
        self._trips = df

    @property
    def frequencies(self):
        return self._frequencies

    @frequencies.setter
    def frequencies(self, value):
        df = FrequenciesSchema.validate(value, lazy=True)
        self._frequencies = df

    @property
    def routes(self):
        return self._routes

    @routes.setter
    def routes(self, value):
        df = RoutesSchema.validate(value, lazy=True)
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
        if self.SCHEMAS.get(table):
            # set to lazy so that all errors found before returning
            df = self.SCHEMAS[table].validate(df, lazy=True)
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

    @classmethod
    def _config_from_files(cls, ptg_feed: ptg.gtfs.Feed) -> DiGraph:
        """
        Return updated config based on which files are available & have data.

        Will fail if any Feed.REQUIRED_FILES are not present.

        Since Partridge lazily loads the df, load each file to make sure it
        actually works.

        Args:
            ptg_feed: partridge feed
        """
        updated_config = copy.deepcopy(default_config())
        _missing_files = []
        for node in ptg_feed._config.nodes.keys():
            if ptg_feed.get(node).shape[0] == 0:
                _missing_files.append(node)

        _missing_required = set(_missing_files) & set(cls.REQUIRED_FILES)
        if _missing_required:
            WranglerLogger.error(f"Couldn't find required files: {_missing_required}")
            raise FeedReadError(f"Missing required files: {_missing_required}")

        for node in _missing_files:
            WranglerLogger.debug(f"{node} empty. Removing from transit network.")
            updated_config.remove_node(node)

        return updated_config

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
        for df in self.config.nodes.keys():
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
        """A hash representing the contents of the talbes in self.TABLES_IN_FEED_HASH."""
        _table_hashes = [self.get(t).df_hash() for t in self.TABLES_IN_FEED_HASH]
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

    @check_output(TripsSchema, inplace=True)
    def trips_with_shape_id(self, shape_id: str) -> pd.DataFrame:
        trips_df = self.get("trips")

        return trips_df.loc[trips_df.shape_id == shape_id]

    @check_output(StopTimesSchema, inplace=True)
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

    @check_output(StopsSchema, inplace=True)
    def trip_stops(self, trip_id: str, pickup_type: str = "either") -> list[str]:
        """Returns stops.txt which are used for a given trip_id"""
        stop_ids = self.trip_stop_pattern(trip_id, pickup_type=pickup_type)
        return self.stops.loc[self.stops.isin(stop_ids)]

    def shape_node_pattern(self, shape_id: str) -> list[int]:
        """Returns node pattern of a shape.
         
        args:
            shape_id: string identifier of the shape.
        """
        shape_df = self.shapes.loc[self.shapes["shape_id"] == shape_id ]
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
        trip_stop_times = self.trip_stop_times_for_pickup_type(trip_id, pickup_type=pickup_type)

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

    @check_output(StopTimesSchema,inplace = True)
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

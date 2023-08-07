import copy
import hashlib

from typing import Union
from pathlib import Path

import pandas as pd
import partridge as ptg

from networkx import DiGraph
from partridge.config import default_config

from .schemas import (
    FrequenciesSchema,
    StopsSchema,
    RoutesSchema,
    TripsSchema,
    ShapesSchema,
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

        for node in self.config.nodes.keys():
            _table = node.replace(".txt", "")
            self.__dict__[_table] = self._read_from_file(node)

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
        # First, create a new instance of the Feed class
        new_feed = Feed(feed_path=self.feed_path)

        # Now, use the deepcopy method to create deep copies of each DataFrame
        # and assign them to the corresponding attributes in the new Feed object.
        for table_name, df in self.__dict__.items():
            if isinstance(df, pd.DataFrame):
                new_feed.__dict__[table_name] = copy.deepcopy(df, memo)

        # Return the newly created deep copy of the Feed object
        return new_feed

    @property
    def table_names(self) -> list[str]:
        """
        Returns list of tables from config.
        """
        return list(self.config.nodes.keys())

    @property
    def tables(self) -> list[pd.DataFrame]:
        """
        Returns list of tables from config.
        """
        return [self.get(table_name) for table_name in self.config.nodes.keys()]

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

    def get(self, table: str, validate: bool = True) -> pd.DataFrame:
        """Get table by name and optionally validate it.

        args:
            table: table name. e.g. frequencies, stops, etc.
            validate: if True, will validate against relevant schemas and foreign keys.
                Defaults to True.
        """
        df = self.__dict__.get(table, None)
        if validate:
            df = self.validate_df_as_table(table, df)
        return df

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
        self.validate_df_as_table(table_name, updated_df)
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
        _table_hashes = [
            self.get(t, validate=False).df_hash() for t in self.TABLES_IN_FEED_HASH
        ]
        _value = str.encode("-".join(_table_hashes))

        _hash = hashlib.sha256(_value).hexdigest()
        return _hash

    def tables_with_property(self, property: str) -> list[str]:
        """
        Returns feed tables in the feed which contain the property.
        """
        return [t for t in self.table_names if property in self.get(t).columns]

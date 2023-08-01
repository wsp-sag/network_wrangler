import copy
import hashlib

from typing import Union
from pathlib import Path

import pandas as pd
import partridge as ptg

from networkx import DiGraph
from partridge.config import default_config

from .schemas import FrequenciesSchema, StopsSchema, RoutesSchema, TripsSchema
from ..utils import fk_in_pk
from ..logger import WranglerLogger


class FeedReadError(Exception):
    pass


class FeedValidationError(Exception):
    pass


class Feed:
    REQUIRED_FILES = [
        "agency.txt",
        "frequencies.txt",
        "routes.txt",
        "shapes.txt",
        "stop_times.txt",
        "stops.txt",
        "trips.txt",
    ]

    SCHEMAS = {
        "frequencies": FrequenciesSchema,
        "routes": RoutesSchema,
        "trips": TripsSchema,
        "stops": StopsSchema,
    }

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

        TODO: Replace usage of partridge
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
    def config(self):
        return self._config

    def _read_from_file(self, node: str) -> pd.DataFrame:
        """Read node from file + validate to schema if table name in SCHEMAS and return dataframe.

        Args:
            node (str): name of feed file, e.g. `stops.txt`

        Returns:
            pd.DataFrame: Dataframe of file.
        """
        _table = node.replace(".txt", "")
        df = self._ptg_feed.get(node)
        self.validate_df_as_table(_table, df)
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
            self.validate_df_as_table(table, df)
        return df

    def validate_df_as_table(self, table: str, df: pd.DataFrame) -> bool:
        """Validate a dataframe as a table: relevant schemas and foreign keys.

        Args:
            table (str): table name. e.g. frequencies, stops, etc.
            df (pd.DataFrame): dataframe to be validated as that table
        """
        if self.SCHEMAS.get(table):
            # set to lazy so that all errors found before returning
            self.SCHEMAS[table].validate(df, lazy=True)
        else:
            WranglerLogger.debug(
                f"{table} requested but no schema available to validate."
            )
        return True

    @property
    def foreign_keys_valid(self) -> bool:
        """Validate all foreign keys exist in primary key table."""
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
    ) -> Union[bool, list]:
        _fks = self.INTRA_FEED_FOREIGN_KEYS.get(table, {})
        table_valid = True
        table_fk_missing = []
        if not _fks:
            return True, []
        if df is None:
            node_df = self.get(table)
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

    def validate_fks(self):
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
    def feed_hash(self):
        _table_hashes = [
            self.get(t, validate=False).df_hash() for t in self.TABLES_IN_FEED_HASH
        ]
        _value = str.encode("-".join(_table_hashes))

        _hash = hashlib.sha256(_value).hexdigest()
        return _hash

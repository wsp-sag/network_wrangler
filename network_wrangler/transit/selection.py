import copy
import hashlib

from typing import Optional, List, Dict, Union, Literal

import pandas as pd

from pydantic import BaseModel, Field, validator, validate_arguments

from ..utils import parse_timespans_to_secs, dict_to_hexkey
from ..logger import WranglerLogger


class TransitSelectionError(Exception):
    pass


class TransitSelection:
    """Object to perform and store information about a selection from a project card "facility".
    
    Attributes:
        selection_dict: dict: Dictionary of selection criteria
        selected_trips: list: List of selected trips
        selected_trips_df: pd.DataFrame: DataFrame of selected trips
        sel_key: str: Hash of selection_dict
        net: TransitNetwork: Network to select from
    
    """

    def __init__(
        self,
        net: "TransitNetwork",
        selection_dict: TransitSelectionDict,
    ):
        """Constructor for RoadwaySelection object.

        Args:
            net (TransitNetwork): Transit network object to select from.
            selection_dict: Selection dictionary conforming to TransitSelectionDict
        """
        WranglerLogger.debug(f"Created Transit Selection: {selection_dict}")
        self.net = net
        
        self._selection_dict = selection_dict
        self.sel_key = dict_to_hexkey(selection_dict)

        # Initialize
        self._selected_trips_df = None
        self._stored_feed_hash = copy.deepcopy(self.net.feed.feed_hash)

    def __nonzero__(self):
        """Return True if there are selected trips."""
        if len(self.selected_trips_df) > 0:
            return True
        return False

    @property
    def selection_dict(self):
        """Getter for selection_dict."""
        return self._selection_dict

    @selection_dict.setter
    @validate_arguments
    def selection_dict(self, selection_dict: TransitSelectionDict):
        """Setter for selection_dict.

        Args:
            selection_dict (TransitSelectionDict): New selection dictionary
        """        
        # make sure all values tuples or list
        self._selection_dict = {
            k: [v] if not isinstance(v, (list, tuple)) else v
            for k, v in selection_dict.items()
        }
        
        self.validate_selection_dict(self._selection_dict)

    @validate_arguments
    def validate_selection_dict(self, selection_dict: TransitSelectionDict) -> None:
        """Check that selection dictionary has valid and used properties consistent with network.

        Checks that selection_dict is a valid TransitSelectionDict:
            - query vars exist in respective Feed tables
        Args:
            selection_dict (dict): selection dictionary

        Raises:
            TransitSelectionFormatError: If not valid
        """
        _trip_selection_fields = list(selection_dict.get("trip_properties", {}).keys())
        _missing_trip_fields = set(_trip_selection_fields) - set(self.net.feed.trips.columns)
        
        if _missing_trip_fields:
            raise TransitSelectionFormatError(f"Fields in trip selection dictionary but not trips.txt: {_missing_trip_fields}")
        
        _route_selection_fields = list(selection_dict.get("route_properties", {}).keys())
        _missing_route_fields = set(_route_selection_fields)-set(self.net.feed.routes.columns)

        if _missing_route_fields:
            raise TransitSelectionFormatError(f"Fields in route selection dictionary but not routes.txt: {_missing_route_fields}")

    @property
    def selected_trips(self) -> list:
        """List of selected trip_ids."""
        if self.selected_trips_df is None:
            return []
        return self.selected_trips_df.trip_id.tolist()

    @property
    def selected_trips_df(self) -> pd.DataFrame:
        """Lazily evaluates selection for trips or returns stored value in self._selected_trips_df.

        Will re-evaluate if the current network hash is different than the stored one from the
        last selection.

        Returns:
            pd.DataFrame: DataFrame of selected trips
        """
        if (
            self._selected_trips_df is not None
        ) and self._stored_feed_hash == self.net.feed.feed_hash:
            return self._selected_trips_df

        self._selected_trips_df = self._select_trips()
        self._stored_feed_hash = copy.deepcopy(self.net.feed.feed_hash)
        return self._selected_trips_df

    def _select_trips(self) -> pd.DataFrame:
        """
        Selects transit trips based on selection dictionary.

        Selects first by nodes and then attributes.

        Returns:
            pd.DataFrame: DataFrame of selected trips
        """
        trips_df = self.net.feed.trips
        _tot_trips = len(trips_df)

        trips_df = self._filter_trips_by_nodes(trips_df)
        trips_df = self._filter_trips_by_route(trips_df)
        trips_df = self._filter_trips_by_trip(trips_df)
        trips_df = self._filter_trips_by_timespan(trips_df)

        _num_sel_trips = len(trips_df)
        WranglerLogger.debug(f"Selected {_num_sel_trips}/{_tot_trips} trips.")
        if not _num_sel_trips:
            WranglerLogger.error(
                "No Trips Found with selection: \n{self.selection_dict}"
            )
            raise TransitSelectionError("Couldn't find trips in mode selection.")
        return trips_df

    def _filter_trips_by_nodes(
        self, trips_df: pd.DataFrame = None, 
    ) -> Union[None, pd.DataFrame]:
        """
        Selects transit trips that use any one of a list of nodes in shapes.txt.

        If require = all, the returned trip_ids must traverse all of the nodes
        Else, filter any shapes that use any one of the nodes in node_ids

        Args:
            trips_df: List of trips to filter. If not provided, assumes whole network.

        Returns:
            Copy of filtered trips_df or None
        """
    
        _tot_trips = len(trips_df)
        _sel_dict = self.selection_dict.get("nodes", {})

        if not _sel_dict:
            WranglerLogger.debug("Skipping - no node selection.")
            return trips_df
        WranglerLogger.debug(f"Filtering {_tot_trips} by nodes.")
        shapes_df = self.net.feed.shapes
        require = _sel_dict.get("require", "any").lower()
        node_ids = self.node_selection_dict["nodes"]
        node_fk, rd_field = self.net.TRANSIT_FOREIGN_KEYS_TO_ROADWAY["shapes"]["links"]
        if require == "all":
            shape_ids = (
                shapes_df.groupby("shape_id").filter(
                    lambda x: all(i in x[node_fk].tolist() for i in node_ids)
                )
            ).shape_id.drop_duplicates()
        elif require == "any":
            shape_ids = shapes_df.loc[
                shapes_df[node_fk].isin(node_ids)
            ].shape_id.drop_duplicates()
        else:
            raise TransitSelectionFormatError("Require must be 'any' or 'all'")

        trips_df = trips_df.loc[trips_df.shape_id.isin(shape_ids)].copy()

        _sel_trips = len(trips_df)
        WranglerLogger.debug(f"Selected {_sel_trips}/{_tot_trips} trips.")
        if _sel_trips < 10:
            WranglerLogger.debug(f"{trips_df.trip_id}")

        return trips_df


    def _filter_trips_by_trip(self, trips_df: pd.DataFrame) -> pd.DataFrame:
        """Filter trips by trip properties.

        Args:
            trips_df (pd.DataFrame): DataFrame of trips to filter

        Returns:
            pd.DataFrame: Filtered trips
        """        
        _unfiltered_trips = len(trips_df)
        _sel_dict = self.selection_dict.get("trip_properties", {})
        if not _sel_dict: 
            WranglerLogger.debug("Skipping - no trip properties selection.")
            return trips_df
        WranglerLogger.debug(f"Filtering {_unfiltered_trips} by trip properties.")

        trips_df = trips_df.dict_query(_sel_dict)
        _filtered_trips = len(trips_df)

        WranglerLogger.debug(
            f"{_filtered_trips}/{_unfiltered_trips} trips remain after \
            filtering to trip selection {self.selection_dict['trip_properties']}"
        )
        if _filtered_trips < 10:
            WranglerLogger.debug(f"{trips_df.trip_id}")

        return trips_df

    def _filter_trips_by_route(self, trips_df: pd.DataFrame) -> pd.DataFrame:
        """Filter trips by route properties.

        Args:
            trips_df (pd.DataFrame): DataFrame of trips to filter

        Returns:
            pd.DataFrame: Filtered trips
        """        
        _unfiltered_trips = len(trips_df)
        routes_df = self.net.feed.routes
        _sel_dict = self.selection_dict.get("route_properties", {})
        if not _sel_dict: 
            WranglerLogger.debug("Skipping - no route properties selection.")
            return trips_df
        WranglerLogger.debug(f"Filtering {_unfiltered_trips} by route properties.")
        _selected_routes_df = routes_df.dict_query(_sel_dict)

        trips_df = trips_df.loc[trips_df.route_id.isin(_selected_routes_df["route_id"])]
        _filtered_trips = len(trips_df)

        WranglerLogger.debug(
            f"{_filtered_trips}/{_unfiltered_trips} trips remain after \
            filtering to route selection {self.selection_dict['route_properties']}"
        )
        if _filtered_trips < 10:
            WranglerLogger.debug(f"{trips_df.trip_id}")

        return trips_df

    def _filter_trips_by_timespan(self, trips_df: pd.DataFrame) -> pd.DataFrame:
        _unfiltered_trips = len(trips_df)

        _sel_dict = self.selection_dict.get("timespan", [])
        if not _sel_dict:
            WranglerLogger.debug("Skipping - no timespan selection.")
            return trips_df
        WranglerLogger.debug(f"Filtering {_unfiltered_trips} by timespan.")
        # Filter freq to trips in selection
        freq_df = self.net.feed.frequencies
        freq_df = freq_df.loc[freq_df.trip_id.isin(trips_df["trip_id"])]

        # Filter freq to time that overlaps selection
        freq_df = freq_df.loc[
            freq_df.end_time >= _sel_dict["start_time"]
        ]
        freq_df = freq_df.loc[
            freq_df.start_time <= _sel_dict["end_time"]
        ]

        # Filter trips table to those still in freq table
        trips_df = trips_df.loc[trips_df.trip_id.isin(freq_df["trip_id"])]
        _filtered_trips = len(trips_df)

        WranglerLogger.debug(
            f"{_filtered_trips}/{_unfiltered_trips} trips remain after \
            filtering to timeframe {self.selection_dict['timespan']}"
        )

        if _filtered_trips < 10:
            WranglerLogger.debug(f"{trips_df.trip_id}")

        return trips_df

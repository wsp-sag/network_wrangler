import copy
import hashlib

from typing import Collection, Union

import pandas as pd

from ..utils import parse_time_spans_to_secs
from ..logger import WranglerLogger


class TransitSelectionFormatError(Exception):
    pass


class TransitSelectionError(Exception):
    pass


class TransitSelection:
    """Object to perform and store information about a selection from a project card "facility"."""

    FREQ_QUERY = ["time", "start_time", "end_time"]
    ROUTE_QUERY = ["route_short_name", "route_long_name"]
    TRIP_QUERY = ["trip_id", "route_id"]
    NODE_QUERY = ["nodes", "require_all"]
    QUERY_KEYS = FREQ_QUERY + ROUTE_QUERY + TRIP_QUERY + NODE_QUERY
    def __init__(
        self,
        net: "TransitNetwork",
        selection_dict: dict,
    ):
        """Constructor for RoadwaySelection object.

        Args:
            net (TransitNetwork): Transit network object to select from.
            selection_dict (dict): Selection dictionary.
        """

        self.net = net

        # make sure all values tuples or list
        self.selection_dict = {
            k: [v] if not isinstance(v, (list, tuple)) else v
            for k, v in selection_dict.items()
        }
        self.validate_selection_dict(self.selection_dict)

        self.sel_key = self._assign_selection_key(self.selection_dict)

        # Initialize
        self._selected_trips_df = None
        self._stored_feed_hash = copy.deepcopy(self.net.feed_hash)

        

    def __nonzero__(self):
        if len(self.selected_trips_df) > 0:
            return True
        return False

    @property
    def route_selection_dict(self):
        "Finds values in selection_dict which pertain to routes."
        d = {}
        for k, v in self.selection_dict.items():
            if k in self.ROUTE_QUERY:
                d[k] = v
            elif k[:7] == "routes.":
                k_col = k[7:]
                d[k_col] = v
        return d

    @property
    def trip_selection_dict(self):
        "Finds values in selection_dict which pertain to trips."
        d = {}
        for k, v in self.selection_dict.items():
            if k in self.TRIP_QUERY:
                d[k] = v
            elif k[:6] == "trips.":
                k_col = k[6:]
                d[k_col] = v
        return d

    @property
    def freq_selection_dict(self):
        "Finds values in selection_dict which pertain to freq and transforms time to secs."
        d = {}
        if "time" in self.selection_dict:
            d["start_time"], d["end_time"] = parse_time_spans_to_secs(
                self.selection_dict["time"]
            )
        elif all(i in self.selection_dict for i in ["start_time", "end_time"]):
            d["start_time"], d["end_time"] = parse_time_spans_to_secs(
                [
                    self.selection_dict["start_time"][0],
                    self.selection_dict["end_time"][0],
                ]
            )
        return d

    @property
    def node_selection_dict(self):
        "Finds values in selection_dict which pertain to node-based-selection."
        d = {}
        for k, v in self.selection_dict.items():
            if k in self.NODE_QUERY:
                d[k] = v
        return d

    def validate_selection_dict(self,selection_dict:dict)->None:
        """Check that selection dictionary has valid and used properties consistent with network.

        Args:
            selection_dict (dict): selection dictionary

        Raises:
            TransitSelectionFormatError: If not valid
        """
        _nonstandard_sks = list(set(self.selection_dict.keys()) - set(self.QUERY_KEYS))
        _unused_sks = [ i for i in _nonstandard_sks if i.split(".")[0] not in ('trips','routes')]
        if _unused_sks:
            WranglerLogger.error(
                f"Following keys in selection query are not valid: {_unused_sks}"
            )

        _bad_route_keys = set(self.route_selection_dict.keys()) - set(self.net.feed.routes.columns)
        if _bad_route_keys:
            WranglerLogger.error(
                f"Following route keys not found in routes.txt: {_bad_route_keys}"
            )

        _bad_trip_keys = set(self.trip_selection_dict.keys()) - set(self.net.feed.trips.columns)
        if _bad_trip_keys:
            WranglerLogger.error(
                f"Following trip keys not found in trips.txt: {_bad_trip_keys}"
            )

        if _bad_route_keys or _bad_trip_keys or _unused_sks:
            raise TransitSelectionFormatError("Invalid selection keys in selection dictionary.")

    @property
    def selected_trips(self) -> list:
        if self.selected_trips_df is None:
            return []
        return self.selected_trips_df.trip_id.tolist()

    @property
    def selected_trips_df(self) -> pd.DataFrame:
        """Lazily evaluates selection for trips or returns stored value in self._selected_trips_df.

        Will re-evaluate if the current network hash is different than the stored one from the
        last selection.

        Returns:
            _type_: _description_
        """
        if (
            self._selected_trips_df is not None
        ) and self._stored_feed_hash == self.net.feed_hash:
            return self._selected_trips_df

        self._selected_trips_df = self._select_trips()
        self._stored_feed_hash = copy.deepcopy(self.net.feed_hash)
        return self._selected_trips_df

    @staticmethod
    def _assign_selection_key(selection_dict: dict) -> tuple:
        """
        Selections are stored by a sha1 hash of the bit-encoded string of the selection dictionary.

        Args:
            selection_dictonary: Selection Dictionary

        Returns: Hex code for hash
        """
        return hashlib.sha1(str(selection_dict).encode()).hexdigest()

    def _select_trips(self) -> pd.DataFrame:
        """
        Selects transit trips based on selection dictionary.

        Selects first by nodes and then attributes.

        Returns:
            pd.DataFrame: _description_
        """
        trips_df = self.net.feed.trips
        _tot_trips = len(trips_df)

        if self.node_selection_dict:
            trips_df = self._select_trips_by_nodes(trips_df)
        
        trips_df = self._select_trips_by_properties(trips_df)

        _sel_trips = len(trips_df)
        WranglerLogger.debug(f"Selected {_sel_trips}/{_tot_trips} trips.")
        if not _sel_trips:
            WranglerLogger.error(
                "No Trips Found with selection: \n{self.selection_dict}"
            )
            raise TransitSelectionError("Couldn't find trips in mode selection.")
        return trips_df

    def _select_trips_by_nodes(
        self, trips_df: pd.DataFrame = None
    ) -> Union[None, pd.DataFrame]:
        """
        Selects transit trips that use any one of a list of nodes in shapes.txt.

        If require_all, the returned trip_ids must traverse all of the nodes
        Else, filter any shapes that use any one of the nodes in node_ids

        Args:
            trips_df: List of trips to filter. If not provided, assumes whole network.

        Returns:
            Copy of filtered trips_df or None
        """
        if trips_df is None:
            trips_df = self.net.feed.trips_df
        _tot_trips = len(trips_df)

        shapes_df = self.net.feed.shapes
        require_all = self.node_selection_dict.get("require_all", False)
        node_ids = self.node_selection_dict["nodes"]
        node_fk = self.net.SHAPES_FOREIGN_KEY
        if require_all:
            shape_ids = (
                shapes_df.groupby("shape_id").filter(
                    lambda x: all(i in x[node_fk].tolist() for i in node_ids)
                )
            ).shape_id.drop_duplicates()
        else:
            shape_ids = shapes_df.loc[
                shapes_df[node_fk].isin(node_ids)
            ].shape_id.drop_duplicates()

        trips_df = trips_df.loc[trips_df.shape_id.isin(shape_ids)].copy()

        _sel_trips = len(trips_df)
        WranglerLogger.debug(f"Selected {_sel_trips}/{_tot_trips} trips.")
        if _sel_trips<10: 
            WranglerLogger.debug(f"{trips_df.trip_id}")
            
        return trips_df

    def _select_trips_by_properties(
        self, trips_df: pd.DataFrame = None
    ) -> Union[None, pd.DataFrame]:
        """
        Selects transit features that based on property selection.

        Args:
            trips_df: List of trips to filter. If not provided, assumes whole network.

        Returns:
            Copy of filtered trips_df or None
        """
        if trips_df is None:
            trips_df = self.net.feed.trips
        _tot_trips = len(trips_df)

        trips_df = self._filter_trips_by_route(trips_df)
        trips_df = self._filter_trips_by_trip(trips_df)
        trips_df = self._filter_trips_by_time_period(trips_df)
        trips_df = trips_df.copy()

        _sel_trips = len(trips_df)
        
        WranglerLogger.debug(f"Selected {_sel_trips}/{_tot_trips} trips.")
        if _sel_trips<10: 
            WranglerLogger.debug(f"{trips_df.trip_id}")

        return trips_df

    def _filter_trips_by_trip(self, trips_df: pd.DataFrame) -> pd.DataFrame:
        _unfiltered_trips = len(trips_df)
        if not self.trip_selection_dict:
            return trips_df
        trips_df = trips_df.dict_query(self.trip_selection_dict)
        _filtered_trips = len(trips_df)

        WranglerLogger.debug(
            f"{_filtered_trips}/{_unfiltered_trips} trips remain after \
            filtering to trip selection {self.trip_selection_dict}"
        )
        if _filtered_trips<10:
            WranglerLogger.debug(f"{trips_df.trip_id}")
        
        return trips_df

    def _filter_trips_by_route(self, trips_df: pd.DataFrame) -> pd.DataFrame:
        _unfiltered_trips = len(trips_df)
        routes_df = self.net.feed.routes
        if not self.route_selection_dict:
            return trips_df
        _selected_routes_df = routes_df.dict_query(self.route_selection_dict)

        trips_df = trips_df.loc[trips_df.route_id.isin(_selected_routes_df["route_id"])]
        _filtered_trips = len(trips_df)

        WranglerLogger.debug(
            f"{_filtered_trips}/{_unfiltered_trips} trips remain after \
            filtering to route selection {self.route_selection_dict}"
        )
        if _filtered_trips<10:
            WranglerLogger.debug(f"{trips_df.trip_id}")

        return trips_df

    def _filter_trips_by_time_period(self, trips_df: pd.DataFrame) -> pd.DataFrame:
        _unfiltered_trips = len(trips_df)
        if not self.freq_selection_dict:
            return trips_df
        # Filter freq to trips in selection
        freq_df = self.net.feed.frequencies
        freq_df = freq_df.loc[freq_df.trip_id.isin(trips_df["trip_id"])]

        # Filter freq to time that overlaps selection
        freq_df = freq_df.loc[
            freq_df.end_time >= self.freq_selection_dict["start_time"]
        ]
        freq_df = freq_df.loc[
            freq_df.start_time <= self.freq_selection_dict["end_time"]
        ]

        # Filter trips table to those still in freq table
        trips_df = trips_df.loc[trips_df.trip_id.isin(freq_df["trip_id"])]
        _filtered_trips = len(trips_df)

        WranglerLogger.debug(
            f"{_filtered_trips}/{_unfiltered_trips} trips remain after \
            filtering to timeframe {self.freq_selection_dict['start_time']} - \
                {self.freq_selection_dict['end_time']}"
        )

        if _filtered_trips<10:
            WranglerLogger.debug(f"{trips_df.trip_id}")

        return trips_df

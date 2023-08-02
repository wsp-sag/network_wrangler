#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import copy
import os
from typing import Union

import networkx as nx
import numpy as np
import pandas as pd

from projectcard import ProjectCard

from .logger import WranglerLogger
from .utils import fk_in_pk
from .transit import Feed, TransitSelection


class TransitRoadwayConsistencyError(Exception):
    pass


class TransitNetwork(object):
    """
    Representation of a Transit Network.

    Typical usage example:
    ``` py
    import network_wrangler as wr
    tc=wr.TransitNetwork.read(path=stpaul_gtfs)
    ```

    Attributes:
        feed: Partridge feed mapping dataframes.
        config (nx.DiGraph): Partridge config
        road_net (RoadwayNetwork): Associated roadway network object.
        graph (nx.MultiDiGraph): Graph for associated roadway network object.
        feed_path (str): Where the feed was read in from.
        validated_frequencies (bool): The frequencies have been validated.
        validated_road_network_consistency (): The network has been validated against
            the road network.
        ID_SCALAR (int): scalar value added to create new IDs when necessary.
        REQUIRED_FILES (list[str]): list of files that the transit network requires.

    .. todo::
      investigate consolidating scalars this with RoadwayNetwork
      consolidate thes foreign key constants into one if possible
    """

    """
    Mapping of foreign keys in the transit network which refer to primary keys in the roadway
    Network.
    """
    TRANSIT_FOREIGN_KEYS_TO_ROADWAY = {
        "stops": {"nodes": ("model_node_id", "model_node_id")},
        "shapes": {
            "nodes": ("shape_model_node_id", "model_node_id"),
            "links": ("shape_model_node_id", "model_node_id"),
        },
    }

    TIME_COLS = ["arrival_time", "departure_time", "start_time", "end_time"]

    ID_SCALAR = 100000000

    def __init__(self, feed: Feed = None):
        """
        Constructor for TransitNetwork.

        args:
            feed: Feed object mimicing partridge feed
        """
        self.feed: Feed = feed
        self._road_net: "RoadwayNetwork" = None
        self.graph: nx.MultiDiGraph = None

        # initialize
        self.validated_road_network_consistency = False

        # cached selections
        self._selections = {}

    @staticmethod
    def read(feed_path: str) -> TransitNetwork:
        """
        Create TransitNetwork object from path to a GTFS transit feed.

        Args:
            feed_path: where to read transit network files from
        """
        return TransitNetwork(Feed(feed_path))

    @property
    def feed_path(self):
        """Pass through property from Feed."""
        return self.feed.feed_path

    @property
    def config(self):
        """Pass through property from Feed."""
        return self.feed.config

    @property
    def road_net(self):
        return self._road_net

    @road_net.setter
    def road_net(self, road_net: "RoadwayNetwork"):
        if "RoadwayNetwork" not in str(type(road_net)):
            WranglerLogger.error(f"Cannot assign to road_net - type {type(road_net)}")
            raise ValueError("road_net must be a RoadwayNetwork object.")
        if self._evaluate_consistency_with_road_net(road_net):
            self._road_net = road_net
            self._road_net_hash = copy.deepcopy(self.road_net.network_hash)

        else:
            raise TransitRoadwayConsistencyError(
                "RoadwayNetwork not as TransitNetwork base."
            )

    @property
    def consistent_with_road_net(self) -> bool:
        """Indicate if road_net is consistent with transit network.

        Checks the network hash of when consistency was last evaluated. If transit network or
        roadway network has changed, will re-evaluate consistency and return the updated value and
        update self._road_net_hash.

        Returns:
            Boolean indicating if road_net is consistent with transit network.
        """
        if not self.road_net_hash == self.road_net.network_hash:
            self._consistent_with_road_net = self._evaluate_consistency_with_road_net(
                self.road_net
            )
            self._road_net_hash = copy.deepcopy(self.road_net.network_hash)
        return self._consistent_with_road_net

    def _evaluate_consistency_with_road_net(
        self, road_net: "RoadwayNetwork" = None
    ) -> bool:
        """Checks foreign key and network link relationships between transit feed and a road_net.

        Args:
            road_net (RoadwayNetwork): Roadway network to check relationship with. If None, will
                check self.road_net.

        Returns:
            bool: boolean indicating if road_net is consistent with transit network.
        """
        if road_net is None:
            road_net = self.road_net
        _consistency = self._nodes_in_road_net(
            road_net.nodes_df
        ) and self._shape_links_in_road_net(road_net.links_df)
        return _consistency

    @property
    def transit_shape_links(self) -> pd.DataFrame:
        fk_field, pk_field = self.TRANSIT_FOREIGN_KEYS_TO_ROADWAY["shapes"]["links"]

        transit_shapes_df = (
            self.feed.get_table("shapes")[["shape_pt_sequence", "shape_id", fk_field]]
            .sort_values(by=["shape_pt_sequence"])
            .groupby("shape_id")[fk_field]
            .shift()
            .df.dropna(subset=f"{fk_field}_shift", inplace=True)
            .rename({fk_field: "to_node", f"{fk_field}_shift": "from_node"})
        )
        return transit_shapes_df

    def validate_roadway_nodes_for_table(
        self, table: str, nodes_df: pd.DataFrame = None, _raise_error: bool = True
    ) -> Union[bool, str]:
        """Validate that a transit feed table's foreign key exists in referenced roadway node.

        Uses `self.TRANSIT_FOREIGN_KEYS_TO_ROADWAY` to find the foreign key primary key
        relationship.

        Args:
            table (str): transit feed table name e.g. stops, shapes
            nodes_df (pd.DataFrame, optional): Nodes dataframe from roadway network to validate
                foreign key to. Defaults to self.roadway_net.nodes_df
            _raise_error (bool, optional): If True, will raise an error if . Defaults to True.

        Returns:
            Union[bool,list]: Tuple of a boolean indicating if relationship is valid and an
                error messages
        """
        _fk_field, _pk_field = self.TRANSIT_FOREIGN_KEYS_TO_ROADWAY.get(table, {}).get(
            "nodes"
        )

        if nodes_df is None:
            nodes_df = self.road_net.nodes_df

        fk_valid, fk_missing = fk_in_pk(
            nodes_df[_pk_field], self.feed.get(table)[_fk_field]
        )
        fk_missing_msg = ""
        if fk_missing:
            fk_missing_msg = f"{nodes_df}.{_pk_field} missing values from {table}.{_fk_field}\
                :{fk_missing}"
            if _raise_error:
                WranglerLogger.error(fk_missing_msg)
                raise TransitRoadwayConsistencyError(
                    "{table} missing Foreign Keys in Roadway Network Nodes."
                )
        return fk_valid, fk_missing_msg

    def _nodes_in_road_net(self, nodes_df: pd.DataFrame = None) -> bool:
        """Validate all of a transit feeds node foreign keys exist in referenced roadway nodes.

        Uses `self.TRANSIT_FOREIGN_KEYS_TO_ROADWAY` to find the foreign key primary key
        relationship.

        Args:
            nodes_df (pd.DataFrame, optional): Nodes dataframe from roadway network to validate
                foreign key to. Defaults to self.roadway_net.nodes_df

        Returns:
            boolean indicating if relationships are all valid
        """
        if self.road_net is None:
            return ValueError(
                "Cannot evaluate consistency because roadway network us not set."
            )
        all_valid = True
        missing = []
        if nodes_df is None:
            nodes_df = self.road_net.nodes_df
        for table in self.TRANSIT_FOREIGN_KEYS_TO_ROADWAY.keys():
            _valid, _missing = self.validate_roadway_nodes_for_table(
                table, nodes_df, _raise_error=False
            )
            all_valid = all_valid and _valid
            if _missing:
                missing.append(_missing)

        if missing:
            WranglerLogger.error(missing)
            raise TransitRoadwayConsistencyError(
                "Missing Foreign Keys in Roadway Network Nodes."
            )
        return all_valid

    def _shape_links_in_road_net(self, links_df: pd.DataFrame = None) -> bool:
        """Validate that links in transit shapes exist in referenced roadway links.
        FIXME
        Args:
            links_df (pd.DataFrame, optional): Links dataframe from roadway network to validate
                foreign key to. Defaults to self.roadway_net.links_df

        Returns:
            boolean indicating if relationships are all valid
        """
        tr_field, rd_field = self.TRANSIT_FOREIGN_KEYS_TO_ROADWAY["shapes"]["links"]
        if links_df is None:
            links_df = self.road_net.links_df
        shapes_df = self.feed.shapes.astype({tr_field: int})
        unique_shape_ids = shapes_df.shape_id.unique().tolist()
        valid = True
        for id in unique_shape_ids:
            subset_shapes_df = shapes_df[shapes_df["shape_id"] == id]
            subset_shapes_df = subset_shapes_df.sort_values(by=["shape_pt_sequence"])
            subset_shapes_df = subset_shapes_df.add_suffix("_1").join(
                subset_shapes_df.shift(-1).add_suffix("_2")
            )
            subset_shapes_df = subset_shapes_df.dropna()

            merged_df = subset_shapes_df.merge(
                links_df,
                how="left",
                left_on=[
                    tr_field + "_1",
                    tr_field + "_2",
                ],
                right_on=["A", "B"],
                indicator=True,
            )

            missing_links_df = merged_df.query('_merge == "left_only"')

            # there are shape links which does not exist in the roadway network
            if len(missing_links_df.index) > 0:
                valid = False
                msg = f"There are links for shape id {id} which are missing in the \
                    roadway network."
                WranglerLogger.error(msg)

            transit_not_allowed_df = merged_df.query(
                '_merge == "both" & drive_access == 0 & bus_only == 0 & rail_only == 0'
            )

            # there are shape links where transit is not allowed
            if len(transit_not_allowed_df.index) > 0:
                valid = False
                msg = f"There are links for shape id {id} which does not allow transit \
                    in the roadway network."
                WranglerLogger.error(msg)

        return valid

    def write(self, path: str = ".", filename: str = None) -> None:
        """
        Writes a network in the transit network standard

        Args:
            path: the path were the output will be saved
            filename: the name prefix of the transit files that will be generated
        """
        WranglerLogger.info("Writing transit to directory: {}".format(path))
        for node, config in self.config.nodes.items():
            df = self.feed.get(node.replace(".txt", ""))
            if not df.empty:
                if filename:
                    outpath = os.path.join(path, filename + "_" + node)
                else:
                    outpath = os.path.join(path, node)
                _time_cols = [c for c in TransitNetwork.TIME_COLS if c in df.columns]

                if _time_cols:
                    WranglerLogger.debug(f"Converting cols to datetime: {_time_cols}")
                    df = df.copy()
                    df[_time_cols] = df[_time_cols].applymap(
                        lambda x: pd.to_datetime(x, unit="s")
                    )

                WranglerLogger.debug("Writing file: {}".format(outpath))
                df.to_csv(outpath, index=False, date_format="%H:%M:%S")

    @staticmethod
    def transit_net_to_gdf(transit: Union("TransitNetwork", pd.DataFrame)):
        """
        Returns a geodataframe given a TransitNetwork or a valid Shapes DataFrame.

        Args:
            transit: either a TransitNetwork or a Shapes GeoDataFrame

        .. todo:: Make more sophisticated.
        """
        from partridge import geo

        if type(transit) is pd.DataFrame:
            shapes = transit
        else:
            shapes = transit.feed.shapes

        transit_gdf = geo.build_shapes(shapes)
        return transit_gdf

    def get_selection(
        self,
        selection_dict: dict,
        overwrite: bool = False,
    ) -> TransitSelection:
        """Return selection if it already exists, otherwise performs selection.

        Will raise an error if no trips found.

        Args:
            selection_dict (dict): _description_
            overwrite: if True, will overwrite any previously cached searches. Defaults to False.

        Returns:
            Selection: Selection object
        """
        key = TransitSelection._assign_selection_key(selection_dict)

        if (key not in self._selections) or overwrite:
            WranglerLogger.debug(f"Performing selection from key: {key}")
            self._selections[key] = TransitSelection(self, selection_dict)
        else:
            WranglerLogger.debug(f"Using cached selection from key: {key}")

        if not self._selections[key]:
            WranglerLogger.debug(
                f"No links or nodes found for selection dict:\n {selection_dict}"
            )
            raise ValueError("Selection not successful.")
        return self._selections[key]

    def apply(
        self, project_card: Union[ProjectCard, dict], _subproject: bool = False
    ) -> "TransitNetwork":
        """
        Wrapper method to apply a project to a transit network.

        Args:
            project_card: either a dictionary of the project card object or ProjectCard instance
            _subproject: boolean indicating if this is a subproject under a "changes" heading.
                Defaults to False. Will be set to true with code when necessary.

        """
        if isinstance(project_card, ProjectCard):
            project_card_dictionary = project_card.__dict__
        elif isinstance(project_card, dict):
            project_card_dictionary = project_card
        else:
            WranglerLogger.error(
                f"project_card is of invalid type: {type(project_card)}"
            )
            raise TypeError("project_card must be of type ProjectCard or dict")

        WranglerLogger.info(
            f"Applying Project to Transit Network: { project_card_dictionary['project']}"
        )

        if not _subproject:
            WranglerLogger.info(
                "Applying Project to Roadway Network: {}".format(
                    project_card_dictionary["project"]
                )
            )

        if project_card_dictionary.get("changes"):
            for project_dictionary in project_card_dictionary["changes"]:
                return self.apply(project_dictionary, _subproject=True)
        else:
            project_dictionary = project_card_dictionary

        if "transit_property_change" in project_dictionary:
            return self.apply_transit_feature_change(
                self.get_selection(
                    project_dictionary["transit_property_change"]["service"]
                ).selected_trips,
                project_dictionary["transit_property_change"]["property_changes"],
            )

        elif project_dictionary.get("pycode"):
            return self.apply_python_calculation(project_dictionary["pycode"])

        else:
            msg = "Cannot find transit project in project_dictionary – not implemented yet."
            WranglerLogger.error(msg)
            raise (msg)

    def apply_python_calculation(self, pycode: str) -> "TransitNetwork":
        """
        Changes roadway network object by executing pycode.

        Args:
            pycode: python code which changes values in the roadway network object
        """
        net = copy.deepcopy(self)
        exec(pycode)
        return net

    def apply_transit_feature_change(
        self,
        trip_ids: pd.Series,
        property_changes: dict,
    ) -> "TransitNetwork":
        """
        Changes the transit attributes for the selected features based on the
        project card information passed

        Args:
            net: transit network to manipulate
            trip_ids : pd.Series
                all trip_ids to apply change to
            properties : list of dictionaries
                transit properties to change

        Returns:
            None
        """
        net = copy.deepcopy(self)

        # Grab the list of nodes in the facility from road_net
        # It should be cached because managed lane projects are
        # processed by RoadwayNetwork first via
        # Scenario.apply_all_projects
        # managed_lane_nodes = self.road_net.selections(
        #    self.road_net.build_selection_key(project_dictionary["facility"])
        # )["route"]

        for property, p_changes in property_changes.items():
            if property in ["headway_secs"]:
                net = TransitNetwork._apply_transit_feature_change_frequencies(
                    net, trip_ids, property, p_changes
                )

            elif property in ["routing"]:
                net = TransitNetwork._apply_transit_feature_change_routing(
                    net, trip_ids, p_changes
                )
        return net

    def _apply_transit_feature_change_routing(
        self, trip_ids: pd.Series, routing_change: dict
    ) -> TransitNetwork:
        net = copy.deepcopy(self)
        shapes = net.feed.shapes.copy()
        stop_times = net.feed.stop_times.copy()
        stops = net.feed.stops.copy()

        # A negative sign in "set" indicates a traversed node without a stop
        # If any positive numbers, stops have changed
        stops_change = False
        if any(x > 0 for x in routing_change["set"]):
            # Simplify "set" and "existing" to only stops
            routing_change["set_stops"] = [
                str(i) for i in routing_change["set"] if i > 0
            ]
            if routing_change.get("existing") is not None:
                routing_change["existing_stops"] = [
                    str(i) for i in routing_change["existing"] if i > 0
                ]
            stops_change = True

        # Convert ints to objects
        routing_change["set_shapes"] = [str(abs(i)) for i in routing_change["set"]]
        if routing_change.get("existing") is not None:
            routing_change["existing_shapes"] = [
                str(abs(i)) for i in routing_change["existing"]
            ]

        # Replace shapes records
        trips = net.feed.trips  # create pointer rather than a copy
        shape_ids = trips[trips["trip_id"].isin(trip_ids)].shape_id
        for shape_id in shape_ids:
            # Check if `shape_id` is used by trips that are not in
            # parameter `trip_ids`
            trips_using_shape_id = trips.loc[trips["shape_id"] == shape_id, ["trip_id"]]
            if not all(trips_using_shape_id.isin(trip_ids)["trip_id"]):
                # In this case, we need to create a new shape_id so as to leave
                # the trips not part of the query alone
                WranglerLogger.warning(
                    "Trips that were not in your query selection use the "
                    "same `shape_id` as trips that are in your query. Only "
                    "the trips' shape in your query will be changed."
                )
                old_shape_id = shape_id
                shape_id = str(int(shape_id) + TransitNetwork.ID_SCALAR)
                if shape_id in shapes["shape_id"].tolist():
                    WranglerLogger.error("Cannot create a unique new shape_id.")
                dup_shape = shapes[shapes.shape_id == old_shape_id].copy()
                dup_shape["shape_id"] = shape_id
                shapes = pd.concat([shapes, dup_shape], ignore_index=True)

            # Pop the rows that match shape_id
            this_shape = shapes[shapes.shape_id == shape_id]

            # Make sure they are ordered by shape_pt_sequence
            this_shape = this_shape.sort_values(by=["shape_pt_sequence"])

            shape_node_fk, rd_field = self.net.TRANSIT_FOREIGN_KEYS_TO_ROADWAY[
                "shapes"
            ]["links"]
            # Build a pd.DataFrame of new shape records
            new_shape_rows = pd.DataFrame(
                {
                    "shape_id": shape_id,
                    "shape_pt_lat": None,  # FIXME Populate from self.road_net?
                    "shape_pt_lon": None,  # FIXME
                    "shape_osm_node_id": None,  # FIXME
                    "shape_pt_sequence": None,
                    shape_node_fk: routing_change["set_shapes"],
                }
            )

            # If "existing" is specified, replace only that segment
            # Else, replace the whole thing
            if routing_change.get("existing") is not None:
                # Match list
                nodes = this_shape[shape_node_fk].tolist()
                index_replacement_starts = [
                    i
                    for i, d in enumerate(nodes)
                    if d == routing_change["existing_shapes"][0]
                ][0]
                index_replacement_ends = [
                    i
                    for i, d in enumerate(nodes)
                    if d == routing_change["existing_shapes"][-1]
                ][-1]
                this_shape = pd.concat(
                    [
                        this_shape.iloc[:index_replacement_starts],
                        new_shape_rows,
                        this_shape.iloc[index_replacement_ends + 1 :],
                    ],
                    ignore_index=True,
                    sort=False,
                )
            else:
                this_shape = new_shape_rows

            # Renumber shape_pt_sequence
            this_shape["shape_pt_sequence"] = np.arange(len(this_shape))

            # Add rows back into shapes
            shapes = pd.concat(
                [shapes[shapes.shape_id != shape_id], this_shape],
                ignore_index=True,
                sort=False,
            )

        # Replace stop_times and stops records (if required)
        if stops_change:
            # If node IDs in routing_change["set_stops"] are not already
            # in stops.txt, create a new stop_id for them in stops
            existing_fk_ids = set(stops[TransitNetwork.STOPS_FOREIGN_KEY].tolist())
            nodes_df = net.road_net.nodes_df.loc[
                :, [TransitNetwork.STOPS_FOREIGN_KEY, "X", "Y"]
            ]
            for fk_i in routing_change["set_stops"]:
                if fk_i not in existing_fk_ids:
                    WranglerLogger.info(
                        "Creating a new stop in stops.txt for node ID: {}".format(fk_i)
                    )
                    # Add new row to stops
                    new_stop_id = str(int(fk_i) + TransitNetwork.ID_SCALAR)
                    if new_stop_id in stops["stop_id"].tolist():
                        WranglerLogger.error("Cannot create a unique new stop_id.")
                    stops.loc[
                        len(stops.index) + 1,
                        [
                            "stop_id",
                            "stop_lat",
                            "stop_lon",
                            TransitNetwork.STOPS_FOREIGN_KEY,
                        ],
                    ] = [
                        new_stop_id,
                        nodes_df.loc[
                            nodes_df[TransitNetwork.STOPS_FOREIGN_KEY] == int(fk_i), "Y"
                        ],
                        nodes_df.loc[
                            nodes_df[TransitNetwork.STOPS_FOREIGN_KEY] == int(fk_i), "X"
                        ],
                        fk_i,
                    ]

            # Loop through all the trip_ids
            for trip_id in trip_ids:
                # Pop the rows that match trip_id
                this_stoptime = stop_times[stop_times.trip_id == trip_id]

                # Merge on node IDs using stop_id (one node ID per stop_id)
                this_stoptime = this_stoptime.merge(
                    stops[["stop_id", TransitNetwork.STOPS_FOREIGN_KEY]],
                    how="left",
                    on="stop_id",
                )

                # Make sure the stop_times are ordered by stop_sequence
                this_stoptime = this_stoptime.sort_values(by=["stop_sequence"])

                # Build a pd.DataFrame of new shape records from properties
                new_stoptime_rows = pd.DataFrame(
                    {
                        "trip_id": trip_id,
                        "arrival_time": None,
                        "departure_time": None,
                        "pickup_type": None,
                        "drop_off_type": None,
                        "stop_distance": None,
                        "timepoint": None,
                        "stop_is_skipped": None,
                        TransitNetwork.STOPS_FOREIGN_KEY: routing_change["set_stops"],
                    }
                )

                # Merge on stop_id using node IDs (many stop_id per node ID)
                new_stoptime_rows = (
                    new_stoptime_rows.merge(
                        stops[["stop_id", TransitNetwork.STOPS_FOREIGN_KEY]],
                        how="left",
                        on=TransitNetwork.STOPS_FOREIGN_KEY,
                    )
                    .groupby([TransitNetwork.STOPS_FOREIGN_KEY])
                    .head(1)
                )  # pick first

                # If "existing" is specified, replace only that segment
                # Else, replace the whole thing
                if routing_change.get("existing") is not None:
                    # Match list (remember stops are passed in with node IDs)
                    nodes = this_stoptime[TransitNetwork.STOPS_FOREIGN_KEY].tolist()
                    index_replacement_starts = nodes.index(
                        routing_change["existing_stops"][0]
                    )
                    index_replacement_ends = nodes.index(
                        routing_change["existing_stops"][-1]
                    )
                    this_stoptime = pd.concat(
                        [
                            this_stoptime.iloc[:index_replacement_starts],
                            new_stoptime_rows,
                            this_stoptime.iloc[index_replacement_ends + 1 :],
                        ],
                        ignore_index=True,
                        sort=False,
                    )
                else:
                    this_stoptime = new_stoptime_rows

                # Remove node ID
                del this_stoptime[TransitNetwork.STOPS_FOREIGN_KEY]

                # Renumber stop_sequence
                this_stoptime["stop_sequence"] = np.arange(len(this_stoptime))

                # Add rows back into stoptime
                stop_times = pd.concat(
                    [stop_times[stop_times.trip_id != trip_id], this_stoptime],
                    ignore_index=True,
                    sort=False,
                )

            net.feed.shapes = shapes
            net.feed.stops = stops
            net.feed.stop_times = stop_times
        return net

    def _apply_transit_feature_change_frequencies(
        self, trip_ids: pd.Series, property: str, property_change: dict
    ) -> TransitNetwork:
        net = copy.deepcopy(self)
        freq = net.feed.frequencies.copy()

        # Grab only those records matching trip_ids (aka selection)
        freq = freq[freq.trip_id.isin(trip_ids)]

        # Check all `existing` properties if given
        if property_change.get("existing") is not None:
            if not all(freq.headway_secs == property_change["existing"]):
                WranglerLogger.error(
                    "Existing does not match for at least "
                    "1 trip in:\n {}".format(trip_ids.to_string())
                )
                raise ValueError

        # Calculate build value
        if property_change.get("set") is not None:
            build_value = property_change["set"]
        else:
            build_value = [i + property_change["change"] for i in freq.headway_secs]

        q = net.feed.frequencies.trip_id.isin(freq["trip_id"])

        net.feed.frequencies.loc[q, property] = build_value
        return net


def create_empty_transit_network() -> TransitNetwork:
    """
    Create an empty transit network instance using the default config.

    .. todo:: fill out this method
    """
    # TODO

    msg = "TransitNetwork.empty is not implemented."
    WranglerLogger.error(msg)
    raise NotImplementedError(msg)

#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import copy
import os
import re
from typing import Union

import networkx as nx
import numpy as np
import pandas as pd
import partridge as ptg
from partridge.config import default_config

from projectcard import ProjectCard

from .logger import WranglerLogger
from .utils import parse_time_spans_to_secs
from .roadwaynetwork import RoadwayNetwork


class TransitNetwork(object):
    """
    Representation of a Transit Network.

    Typical usage example:
    ``` py
    import network_wrangler as wr
    stpaul = r'/home/jovyan/work/example/stpaul'
    tc=wr.TransitNetwork.read(path=stpaul)
    ```

    Attributes:
        feed (DotDict): Partridge feed mapping dataframes.
        config (nx.DiGraph): Partridge config
        road_net (RoadwayNetwork): Associated roadway network object.
        graph (nx.MultiDiGraph): Graph for associated roadway network object.
        feed_path (str): Where the feed was read in from.
        validated_frequencies (bool): The frequencies have been validated.
        validated_road_network_consistency (): The network has been validated against
            the road network.
        SHAPES_FOREIGN_KEY (str): foreign key between shapes dataframe and roadway network nodes
        STOPS_FOREIGN_KEY (str): foreign  key between stops dataframe and roadway network nodes
        ID_SCALAR (int): scalar value added to create new IDs when necessary.
        REQUIRED_FILES (list[str]): list of files that the transit network requires.

    .. todo::
      investigate consolidating scalars this with RoadwayNetwork
      consolidate thes foreign key constants into one if possible
    """

    # PK = primary key, FK = foreign key
    SHAPES_FOREIGN_KEY = "shape_model_node_id"
    STOPS_FOREIGN_KEY = "model_node_id"
    TIME_COLS = ["arrival_time", "departure_time", "start_time", "end_time"]

    # TODO consolidate these two ^^^ constants if possible

    ID_SCALAR = 100000000

    # TODO investigate consolidating this with RoadwayNetwork

    REQUIRED_FILES = [
        "agency.txt",
        "frequencies.txt",
        "routes.txt",
        "shapes.txt",
        "stop_times.txt",
        "stops.txt",
        "trips.txt",
    ]

    def __init__(self, feed: DotDict = None, config: nx.DiGraph = None):
        """
        Constructor

        .. todo:: Make graph a reference to associated RoadwayNetwork's graph, not its own thing.
        """
        self.feed: DotDict = feed
        self.config: nx.DiGraph = config
        self.road_net: RoadwayNetwork = None
        self.graph: nx.MultiDiGraph = None
        self.feed_path = None

        self.validated_frequencies = False
        self.validated_road_network_consistency = False

        if not self.validate_frequencies():
            raise ValueError(
                "Transit lines with non-positive frequencies exist in the network"
            )

    @staticmethod
    def empty() -> TransitNetwork:
        """
        Create an empty transit network instance using the default config.

        .. todo:: fill out this method
        """
        # TODO

        msg = "TransitNetwork.empty is not implemented."
        WranglerLogger.error(msg)
        raise NotImplementedError(msg)

    @staticmethod
    def read(feed_path: str) -> TransitNetwork:
        """
        Read GTFS feed from folder and TransitNetwork object

        Args:
            feed_path: where to read transit network files from

        Returns: a TransitNetwork object.
        """
        config = default_config()
        feed = ptg.load_feed(feed_path, config=config)
        WranglerLogger.info("Read in transit feed from: {}".format(feed_path))

        updated_config = TransitNetwork.validate_feed(feed, config)

        # Read in each feed so we can write over them
        editable_feed = DotDict()
        for node in updated_config.nodes.keys():
            # Load (initiate Partridge's lazy load)
            editable_feed[node.replace(".txt", "")] = feed.get(node)

        transit_network = TransitNetwork(feed=editable_feed, config=updated_config)
        transit_network.feed_path = feed_path
        return transit_network

    @staticmethod
    def validate_feed(feed: DotDict, config: nx.DiGraph) -> bool:
        """
        Since Partridge lazily loads the df, load each file to make sure it
        actually works.

        Partridge uses a DiGraph from the networkx library to represent the
        relationships between GTFS files. Each file is a 'node', and the
        relationship between files are 'edges'.

        Args:
            feed: partridge feed
            config: partridge config
        """
        updated_config = copy.deepcopy(config)
        files_not_found = []
        for node in config.nodes.keys():
            n = feed.get(node)
            WranglerLogger.debug("...{}:\n{}".format(node, n[:10]))
            if n.shape[0] == 0:
                WranglerLogger.info(
                    "Removing {} from transit network config because file not found".format(
                        node
                    )
                )
                updated_config.remove_node(node)
                if node in TransitNetwork.REQUIRED_FILES:
                    files_not_found.append(node)

        if files_not_found:
            msg = "Required files not found or valid: {}".format(
                ",".join(files_not_found)
            )
            WranglerLogger.error(msg)
            raise AttributeError(msg)
            return False

        TransitNetwork.validate_network_keys(feed)

        return updated_config

    def validate_frequencies(self) -> bool:
        """
        Validates that there are no transit trips in the feed with zero frequencies.

        Changes state of self.validated_frequencies boolean based on outcome.

        Returns:
            boolean indicating if valid or not.
        """

        _valid = True
        zero_freq = self.feed.frequencies[self.feed.frequencies.headway_secs <= 0]

        if len(zero_freq.index) > 0:
            _valid = False
            msg = "Transit lines {} have non-positive frequencies".format(
                zero_freq.trip_id.to_list()
            )
            WranglerLogger.error(msg)

        self.validated_frequencies = True

        return _valid

    def evaluate_road_network_consistencies(self) -> bool:
        """
        Validates transit network against the road network for both stops
        and shapes.

        Returns:
            boolean indicating if valid or not.
        """
        if self.road_net is None:
            raise ValueError(
                "RoadwayNetwork not set yet, see TransitNetwork.set_roadnet()"
            )

        valid = True

        valid_stops = self.validate_transit_stops()
        valid_shapes = self.validate_transit_shapes()

        if not valid_stops or not valid_shapes:
            valid = False
            raise ValueError("Transit network is not consistent with road network.")

        self.validated_road_network_consistency = True

        return valid

    def validate_transit_stops(self) -> bool:
        """
        Validates that all transit stops are part of the roadway network.

        Returns:
            Boolean indicating if valid or not.
        """

        if self.road_net is None:
            raise ValueError(
                "RoadwayNetwork not set yet, see TransitNetwork.set_roadnet()"
            )

        stops = self.feed.stops
        nodes = self.road_net.nodes_df

        valid = True

        # convert to string for comparison
        stop_ids = [str(s) for s in stops[TransitNetwork.STOPS_FOREIGN_KEY].to_list()]
        node_ids = [str(s) for s in nodes[nodes.params.primary_key].to_list()]

        if not set(stop_ids).issubset(node_ids):
            valid = False
            missing_stops = list(set(stop_ids) - set(node_ids))
            msg = "Not all transit stops are part of the roadyway network. "
            msg += "Missing stops ({}) from the roadway nodes are {}.".format(
                TransitNetwork.STOPS_FOREIGN_KEY, missing_stops
            )
            WranglerLogger.error(msg)

        return valid

    def validate_transit_shapes(self) -> bool:
        """
        Validates that all transit shapes are part of the roadway network.

        Returns:
            Boolean indicating if valid or not.
        """

        if self.road_net is None:
            raise ValueError(
                "RoadwayNetwork not set yet, see TransitNetwork.set_roadnet()"
            )

        shapes_df = self.feed.shapes
        nodes_df = self.road_net.nodes_df
        links_df = self.road_net.links_df

        valid = True

        # check if all the node ids exist in the network
        shape_ids = [
            int(s) for s in shapes_df[TransitNetwork.SHAPES_FOREIGN_KEY].to_list()
        ]
        node_ids = [
            int(n) for n in nodes_df[RoadwayNetwork.NODE_FOREIGN_KEY_TO_LINK].to_list()
        ]

        if not set(shape_ids).issubset(node_ids):
            valid = False
            missing_shapes = list(set(shape_ids) - set(node_ids))
            msg = "Not all transit shapes are part of the roadyway network. "
            msg += "Missing shapes ({}) from the roadway network are {}.".format(
                TransitNetwork.SHAPES_FOREIGN_KEY, missing_shapes
            )
            WranglerLogger.error(msg)
            return valid

        # check if all the links in transit shapes exist in the network
        # and transit is allowed
        shapes_df = shapes_df.astype({TransitNetwork.SHAPES_FOREIGN_KEY: int})
        unique_shape_ids = shapes_df.shape_id.unique().tolist()

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
                    TransitNetwork.SHAPES_FOREIGN_KEY + "_1",
                    TransitNetwork.SHAPES_FOREIGN_KEY + "_2",
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

    @staticmethod
    def route_ids_in_routestxt(feed: DotDict) -> bool:
        """
        Wherever route_id occurs, make sure it is in routes.txt

        Args:
            feed: partridge feed object

        Returns:
            Boolean indicating if feed is okay.
        """
        route_ids_routestxt = set(feed.routes.route_id.tolist())
        route_ids_referenced = set(feed.trips.route_id.tolist())

        missing_routes = route_ids_referenced - route_ids_routestxt

        if missing_routes:
            WranglerLogger.warning(
                "The following route_ids are referenced but missing from routes.txt: {}".format(
                    list(missing_routes)
                )
            )
            return False
        return True

    @staticmethod
    def trip_ids_in_tripstxt(feed: DotDict) -> bool:
        """
        Wherever trip_id occurs, make sure it is in trips.txt

        Args:
            feed: partridge feed object

        Returns:
            Boolean indicating if feed is okay.
        """
        trip_ids_tripstxt = set(feed.trips.trip_id.tolist())
        trip_ids_referenced = set(
            feed.stop_times.trip_id.tolist() + feed.frequencies.trip_id.tolist()
        )

        missing_trips = trip_ids_referenced - trip_ids_tripstxt

        if missing_trips:
            WranglerLogger.warning(
                "The following trip_ids are referenced but missing from trips.txt: {}".format(
                    list(missing_trips)
                )
            )
            return False
        return True

    @staticmethod
    def shape_ids_in_shapestxt(feed: DotDict) -> bool:
        """
        Wherever shape_id occurs, make sure it is in shapes.txt

        Args:
            feed: partridge feed object

        Returns:
            Boolean indicating if feed is okay.
        """

        shape_ids_shapestxt = set(feed.shapes.shape_id.tolist())
        shape_ids_referenced = set(feed.trips.shape_id.tolist())

        missing_shapes = shape_ids_referenced - shape_ids_shapestxt

        if missing_shapes:
            WranglerLogger.warning(
                "The following shape_ids from trips.txt are missing from shapes.txt: {}".format(
                    list(missing_shapes)
                )
            )
            return False
        return True

    @staticmethod
    def stop_ids_in_stopstxt(feed: DotDict) -> bool:
        """
        Wherever stop_id occurs, make sure it is in stops.txt

        Args:
            feed: partridge feed object

        Returns:
            Boolean indicating if feed is okay.
        """
        stop_ids_stopstxt = set(feed.stops.stop_id.tolist())
        stop_ids_referenced = []

        # STOP_TIMES
        stop_ids_referenced.extend(feed.stop_times.stop_id.dropna().tolist())
        stop_ids_referenced.extend(feed.stops.parent_station.dropna().tolist())

        # TRANSFERS
        if feed.get("transfers.txt").shape[0] > 0:
            stop_ids_referenced.extend(feed.transfers.from_stop_id.dropna().tolist())
            stop_ids_referenced.extend(feed.transfers.to_stop_id.dropna().tolist())

        # PATHWAYS
        if feed.get("pathways.txt").shape[0] > 0:
            stop_ids_referenced.extend(feed.pathways.from_stop_id.dropna().tolist())
            stop_ids_referenced.extend(feed.pathways.to_stop_id.dropna().tolist())

        stop_ids_referenced = set(stop_ids_referenced)

        missing_stops = stop_ids_referenced - stop_ids_stopstxt

        if missing_stops:
            WranglerLogger.warning(
                "The following stop_ids from are referenced but missing from stops.txt: {}".format(
                    list(missing_stops)
                )
            )
            return False
        return True

    @staticmethod
    def validate_network_keys(feed: DotDict) -> bool:
        """
        Validates foreign keys are present in all connecting feed files.

        Args:
            feed: partridge feed object

        Returns:
            Boolean indicating if feed is okay.
        """
        result = True
        result = result and TransitNetwork.route_ids_in_routestxt(feed)
        result = result and TransitNetwork.trip_ids_in_tripstxt(feed)
        result = result and TransitNetwork.shape_ids_in_shapestxt(feed)
        result = result and TransitNetwork.stop_ids_in_stopstxt(feed)
        return result

    def set_roadnet(
        self,
        road_net: RoadwayNetwork,
        validate_consistency: bool = True,
    ) -> None:
        self.road_net = road_net
        if validate_consistency:
            self.evaluate_road_network_consistencies()

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

        if project_dictionary["type"].lower() == "transit_property_change:":
            return self.apply_transit_feature_change(
                self.select_transit_features(project_dictionary["facility"]),
                project_dictionary["properties"],
            )

        elif project_dictionary.get("pycode"):
            return self.apply_python_calculation(project_dictionary["pycode"])

        else:
            msg = f"Can't apply {project_dictionary['type']} – not implemented yet."
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

    def select_transit_features(self, selection: dict) -> pd.Series:
        """
        combines multiple selections

        Args:
            selection : selection dictionary

        Returns: trip identifiers : list of GTFS trip IDs in the selection
        """
        trip_ids = pd.Series()

        if selection.get("route"):
            for route_dictionary in selection["route"]:
                trip_ids = trip_ids.append(
                    self._select_transit_features(route_dictionary)
                )
        else:
            trip_ids = self._select_transit_features(selection)

        return trip_ids

    def _select_transit_features(self, selection: dict) -> pd.Series:
        """
        Selects transit features that satisfy selection criteria

        Args:
            selection : selection dictionary

        Returns: trip identifiers : list of GTFS trip IDs in the selection
        """
        trips = self.feed.trips
        routes = self.feed.routes
        freq = self.feed.frequencies

        # Turn selection's values into lists if they are not already
        for key in selection.keys():
            if type(selection[key]) not in [list, tuple]:
                selection[key] = [selection[key]]

        # Based on the key in selection, filter trips
        if "trip_id" in selection:
            trips = trips[trips.trip_id.isin(selection["trip_id"])]

        elif "route_id" in selection:
            trips = trips[trips.route_id.isin(selection["route_id"])]

        elif "route_short_name" in selection:
            routes = routes[routes.route_short_name.isin(selection["route_short_name"])]
            trips = trips[trips.route_id.isin(routes["route_id"])]

        elif "route_long_name" in selection:
            matches = []
            for sel in selection["route_long_name"]:
                for route_long_name in routes["route_long_name"]:
                    x = re.search(sel, route_long_name)
                    if x is not None:
                        matches.append(route_long_name)

            routes = routes[routes.route_long_name.isin(matches)]
            trips = trips[trips.route_id.isin(routes["route_id"])]

        else:
            WranglerLogger.error("Selection not supported %s", selection.keys())
            raise ValueError

        # If a time key exists, filter trips using frequency table
        if selection.get("time"):
            selection["time"] = parse_time_spans_to_secs(selection["time"])
        elif selection.get("start_time") and selection.get("end_time"):
            selection["time"] = parse_time_spans_to_secs(
                [selection["start_time"][0], selection["end_time"][0]]
            )
            # Filter freq to trips in selection
            freq = freq[freq.trip_id.isin(trips["trip_id"])]
            freq = freq[freq.start_time == selection["time"][0]]
            freq = freq[freq.end_time == selection["time"][1]]

            # Filter trips table to those still in freq table
            trips = trips[trips.trip_id.isin(freq["trip_id"])]

        # If any other key exists, filter routes or trips accordingly
        for key in selection.keys():
            if key not in [
                "trip_id",
                "route_id",
                "route_short_name",
                "route_long_name",
                "time",
                "start_time",
                "end_time",
            ]:
                if key in trips:
                    trips = trips[trips[key].isin(selection[key])]
                elif key in routes:
                    routes = routes[routes[key].isin(selection[key])]
                    trips = trips[trips.route_id.isin(routes["route_id"])]
                else:
                    WranglerLogger.error("Selection not supported %s", key)
                    raise ValueError

        # Check that there is at least one trip in trips table or raise error
        if len(trips) < 1:
            WranglerLogger.error("Selection returned zero trips")
            raise ValueError

        # Return pandas.Series of trip_ids
        return trips["trip_id"]

    def select_transit_features_by_nodes(
        self, node_ids: list, require_all: bool = False
    ) -> pd.Series:
        """
        Selects transit features that use any one of a list of node_ids

        Args:
            node_ids: list (generally coming from nx.shortest_path)
            require_all : bool if True, the returned trip_ids must traverse all of
              the nodes (default = False)

        Returns:
            trip identifiers  list of GTFS trip IDs in the selection
        """
        # If require_all, the returned trip_ids must traverse all of the nodes
        # Else, filter any shapes that use any one of the nodes in node_ids
        if require_all:
            shape_ids = (
                self.feed.shapes.groupby("shape_id").filter(
                    lambda x: all(
                        i in x[TransitNetwork.SHAPES_FOREIGN_KEY].tolist()
                        for i in node_ids
                    )
                )
            ).shape_id.drop_duplicates()
        else:
            shape_ids = self.feed.shapes[
                self.feed.shapes[TransitNetwork.SHAPES_FOREIGN_KEY].isin(node_ids)
            ].shape_id.drop_duplicates()

        # Return pandas.Series of trip_ids
        return self.feed.trips[self.feed.trips.shape_id.isin(shape_ids)].trip_id

    def apply_transit_feature_change(
        self,
        trip_ids: pd.Series,
        properties: list,
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

        for i in properties:
            if i["property"] in ["headway_secs"]:
                net = TransitNetwork._apply_transit_feature_change_frequencies(
                    net, trip_ids, i
                )

            elif i["property"] in ["routing"]:
                net = TransitNetwork._apply_transit_feature_change_routing(
                    net, trip_ids, i
                )
        return net

    def _apply_transit_feature_change_routing(
        self,
        trip_ids: pd.Series,
        properties: dict,
    ) -> TransitNetwork:
        net = copy.deepcopy(self)
        shapes = net.feed.shapes.copy()
        stop_times = net.feed.stop_times.copy()
        stops = net.feed.stops.copy()

        # A negative sign in "set" indicates a traversed node without a stop
        # If any positive numbers, stops have changed
        stops_change = False
        if any(x > 0 for x in properties["set"]):
            # Simplify "set" and "existing" to only stops
            properties["set_stops"] = [str(i) for i in properties["set"] if i > 0]
            if properties.get("existing") is not None:
                properties["existing_stops"] = [
                    str(i) for i in properties["existing"] if i > 0
                ]
            stops_change = True

        # Convert ints to objects
        properties["set_shapes"] = [str(abs(i)) for i in properties["set"]]
        if properties.get("existing") is not None:
            properties["existing_shapes"] = [
                str(abs(i)) for i in properties["existing"]
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

            # Build a pd.DataFrame of new shape records
            new_shape_rows = pd.DataFrame(
                {
                    "shape_id": shape_id,
                    "shape_pt_lat": None,  # FIXME Populate from self.road_net?
                    "shape_pt_lon": None,  # FIXME
                    "shape_osm_node_id": None,  # FIXME
                    "shape_pt_sequence": None,
                    TransitNetwork.SHAPES_FOREIGN_KEY: properties["set_shapes"],
                }
            )

            # If "existing" is specified, replace only that segment
            # Else, replace the whole thing
            if properties.get("existing") is not None:
                # Match list
                nodes = this_shape[TransitNetwork.SHAPES_FOREIGN_KEY].tolist()
                index_replacement_starts = [
                    i
                    for i, d in enumerate(nodes)
                    if d == properties["existing_shapes"][0]
                ][0]
                index_replacement_ends = [
                    i
                    for i, d in enumerate(nodes)
                    if d == properties["existing_shapes"][-1]
                ][-1]
                this_shape = pd.concat(
                    [
                        this_shape.iloc[:index_replacement_starts],
                        new_shape_rows,
                        this_shape.iloc[index_replacement_ends + 1:],
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
            # If node IDs in properties["set_stops"] are not already
            # in stops.txt, create a new stop_id for them in stops
            existing_fk_ids = set(stops[TransitNetwork.STOPS_FOREIGN_KEY].tolist())
            nodes_df = net.road_net.nodes_df.loc[
                :, [TransitNetwork.STOPS_FOREIGN_KEY, "X", "Y"]
            ]
            for fk_i in properties["set_stops"]:
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
                        TransitNetwork.STOPS_FOREIGN_KEY: properties["set_stops"],
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
                if properties.get("existing") is not None:
                    # Match list (remember stops are passed in with node IDs)
                    nodes = this_stoptime[TransitNetwork.STOPS_FOREIGN_KEY].tolist()
                    index_replacement_starts = nodes.index(
                        properties["existing_stops"][0]
                    )
                    index_replacement_ends = nodes.index(
                        properties["existing_stops"][-1]
                    )
                    this_stoptime = pd.concat(
                        [
                            this_stoptime.iloc[:index_replacement_starts],
                            new_stoptime_rows,
                            this_stoptime.iloc[index_replacement_ends + 1:],
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
        self, trip_ids: pd.Series, properties: dict
    ) -> TransitNetwork:
        net = copy.deepcopy(self)
        freq = net.feed.frequencies.copy()

        # Grab only those records matching trip_ids (aka selection)
        freq = freq[freq.trip_id.isin(trip_ids)]

        # Check all `existing` properties if given
        if properties.get("existing") is not None:
            if not all(freq.headway_secs == properties["existing"]):
                WranglerLogger.error(
                    "Existing does not match for at least "
                    "1 trip in:\n {}".format(trip_ids.to_string())
                )
                raise ValueError

        # Calculate build value
        if properties.get("set") is not None:
            build_value = properties["set"]
        else:
            build_value = [i + properties["change"] for i in freq.headway_secs]

        q = net.feed.frequencies.trip_id.isin(freq["trip_id"])

        net.feed.frequencies.loc[q, properties["property"]] = build_value
        return net

    def apply_transit_managed_lane(
        self,
        trip_ids: pd.Series,
        node_ids: list,
    ) -> TransitNetwork:
        # Traversed nodes without a stop should be negative integers
        net = copy.deepcopy(self)
        all_stops = net.feed.stops[TransitNetwork.STOPS_FOREIGN_KEY].tolist()
        node_ids = [int(x) if str(x) in all_stops else int(x) * -1 for x in node_ids]

        TransitNetwork._apply_transit_feature_change_routing(
            net,
            trip_ids=trip_ids,
            properties={
                "existing": node_ids,
                "set": RoadwayNetwork.get_managed_lane_node_ids(node_ids),
            },
        )
        return net


class DotDict(dict):
    """
    dot.notation access to dictionary attributes
    Source:
        https://stackoverflow.com/questions/2352181/how-to-use-a-dot-to-access-members-of-dictionary
    """

    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)

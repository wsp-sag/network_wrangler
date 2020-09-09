#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import copy
import os
import re
from typing import Tuple, Union

import networkx as nx
import numpy as np
import pandas as pd
import partridge as ptg
from partridge.config import default_config

from .logger import WranglerLogger
from .utils import parse_time_spans
from .roadwaynetwork import RoadwayNetwork


class TransitNetwork(object):
    """
    Representation of a Transit Network.

    .. highlight:: python

    Typical usage example:
    ::
        import network_wrangler as wr
        stpaul = r'/home/jovyan/work/example/stpaul'
        tc=wr.TransitNetwork.read(path=stpaul)

    Attributes:
        feed (DotDict): Partridge feed mapping dataframes.
        config (nx.DiGraph): Partridge config
        road_net (RoadwayNetwork): Associated roadway network object.
        graph (nx.MultiDiGraph): Graph for associated roadway network object.
        feed_path (str): Where the feed was read in from.
        validated_frequencies (bool): The frequencies have been validated.
        validated_road_network_consistency (): The network has been validated against the road network.
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

    ##TODO consolidate these two ^^^ constants if possible

    ID_SCALAR = 100000000

    ##TODO investigate consolidating this with RoadwayNetwork

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
        ##TODO

        msg = "TransitNetwork.empty is not implemented."
        WranglerLogger.error(msg)
        raise NotImplemented(msg)

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

    def validate_road_network_consistencies(self) -> bool:
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

        self.validated_road_network_consistency = True

        if not valid_stops or not valid_shapes:
            valid = False
            raise ValueError("Transit network is not consistent with road network.")

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

        stop_ids = [int(s) for s in stops[TransitNetwork.STOPS_FOREIGN_KEY].to_list()]
        node_ids = [int(n) for n in nodes[RoadwayNetwork.NODE_FOREIGN_KEY].to_list()]

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
        node_ids = [int(n) for n in nodes_df[RoadwayNetwork.NODE_FOREIGN_KEY].to_list()]

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
                msg = "There are links for shape id {} which are missing in the roadway network.".format(
                    id
                )
                WranglerLogger.error(msg)

            transit_not_allowed_df = merged_df.query(
                '_merge == "both" & drive_access == 0 & bus_only == 0 & rail_only == 0'
            )

            # there are shape links where transit is not allowed
            if len(transit_not_allowed_df.index) > 0:
                valid = False
                msg = "There are links for shape id {} which does not allow transit in the roadway network.".format(
                    id
                )
                WranglerLogger.error(msg)

        return valid

    @staticmethod
    def route_ids_in_routestxt(feed: DotDict) -> Bool:
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
    def trip_ids_in_tripstxt(feed: DotDict) -> Bool:
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
    def shape_ids_in_shapestxt(feed: DotDict) -> Bool:
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
    def stop_ids_in_stopstxt(feed: DotDict) -> Bool:
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
    def validate_network_keys(feed: DotDict) -> Bool:
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
        graph_shapes: bool = False,
        graph_stops: bool = False,
        validate_consistency: bool = True,
    ) -> None:
        self.road_net: RoadwayNetwork = road_net
        self.graph: nx.MultiDiGraph = RoadwayNetwork.ox_graph(
            road_net.nodes_df, road_net.links_df
        )
        if graph_shapes:
            self._graph_shapes()
        if graph_stops:
            self._graph_stops()

        if validate_consistency:
            self.validate_road_network_consistencies()

    def _graph_shapes(self) -> None:
        """

        .. todo:: Fill out this method.
        """
        existing_shapes = self.feed.shapes
        msg = "_graph_shapes() not implemented yet."
        WranglerLogger.error(msg)
        raise NotImplemented(msg)
        # graphed_shapes = pd.DataFrame()

        # for shape_id in shapes:
        # TODO traverse point by point, mapping shortest path on graph,
        # then append to a list
        # return total list of all link ids
        # rebuild rows in shapes dataframe and add to graphed_shapes
        # make graphed_shapes a GeoDataFrame

        # self.feed.shapes = graphed_shapes

    def _graph_stops(self) -> None:
        """
        .. todo:: Fill out this method.
        """
        existing_stops = self.feed.stops
        msg = "_graph_stops() not implemented yet."
        WranglerLogger.error(msg)
        raise NotImplemented(msg)
        # graphed_stops = pd.DataFrame()

        # for stop_id in stops:
        # TODO

        # self.feed.stops = graphed_stops

    def write(self, path: str = ".", filename: str = None) -> None:
        """
        Writes a network in the transit network standard

        Args:
            path: the path were the output will be saved
            filename: the name prefix of the transit files that will be generated
        """
        WranglerLogger.info("Writing transit to directory: {}".format(path))
        for node in self.config.nodes.keys():

            df = self.feed.get(node.replace(".txt", ""))
            if not df.empty:
                if filename:
                    outpath = os.path.join(path, filename + "_" + node)
                else:
                    outpath = os.path.join(path, node)
                WranglerLogger.debug("Writing file: {}".format(outpath))

                df.to_csv(outpath, index=False)

    @staticmethod
    def transit_net_to_gdf(transit: Union(TransitNetwork, pd.DataFrame)):
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

    def apply(self, project_card_dictionary: dict):
        """
        Wrapper method to apply a project to a transit network.

        Args:
            project_card_dictionary: dict
                a dictionary of the project card object

        """
        WranglerLogger.info(
            "Applying Project to Transit Network: {}".format(
                project_card_dictionary["project"]
            )
        )

        def _apply_individual_change(project_dictionary: dict):
            if (
                project_dictionary["category"].lower()
                == "transit service property change"
            ):
                self.apply_transit_feature_change(
                    self.select_transit_features(project_dictionary["facility"]),
                    project_dictionary["properties"],
                )
            elif project_dictionary["category"].lower() == "parallel managed lanes":
                # Grab the list of nodes in the facility from road_net
                # It should be cached because managed lane projects are
                # processed by RoadwayNetwork first via
                # Scenario.apply_all_projects
                try:
                    managed_lane_nodes = self.road_net.selections(
                        self.road_net.build_selection_key(
                            project_dictionary["facility"]
                        )
                    )["route"]
                except ValueError:
                    WranglerLogger.error(
                        "RoadwayNetwork not set yet, see TransitNetwork.set_roadnet()"
                    )

                # Reroute any transit using these nodes
                self.apply_transit_managed_lane(
                    self.select_transit_features_by_nodes(managed_lane_nodes),
                    managed_lane_nodes,
                )
            elif project_dictionary["category"].lower() == "roadway deletion":
                WranglerLogger.warning(
                    "Roadway Deletion not yet implemented in Transit; ignoring"
                )
            else:
                msg = "{} not implemented yet in TransitNetwork; can't apply.".format(
                    project_dictionary["category"]
                )
                WranglerLogger.error(msg)
                raise (msg)

        if project_card_dictionary.get("changes"):
            for project_dictionary in project_card_dictionary["changes"]:
                _apply_individual_change(project_dictionary)
        else:
            _apply_individual_change(project_card_dictionary)

    def select_transit_features(self, selection: dict) -> pd.Series:
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
            selection["time"] = parse_time_spans(selection["time"])
        elif selection.get("start_time") and selection.get("end_time"):
            selection["time"] = parse_time_spans(
                [selection["start_time"], selection["end_time"]]
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
        self, trip_ids: pd.Series, properties: list, in_place: bool = True
    ) -> Union(None, TransitNetwork):
        """
        Changes the transit attributes for the selected features based on the
        project card information passed

        Args:
            trip_ids : pd.Series
                all trip_ids to apply change to
            properties : list of dictionaries
                transit properties to change
            in_place : bool
                whether to apply changes in place or return a new network

        Returns:
            None
        """
        for i in properties:
            if i["property"] in ["headway_secs"]:
                self._apply_transit_feature_change_frequencies(trip_ids, i, in_place)

            elif i["property"] in ["routing"]:
                self._apply_transit_feature_change_routing(trip_ids, i, in_place)

    def _apply_transit_feature_change_routing(
        self, trip_ids: pd.Series, properties: dict, in_place: bool = True
    ) -> Union(None, TransitNetwork):
        shapes = self.feed.shapes.copy()
        stop_times = self.feed.stop_times.copy()
        stops = self.feed.stops.copy()

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
        trips = self.feed.trips  # create pointer rather than a copy
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
                index_replacement_starts = nodes.index(properties["existing_shapes"][0])
                index_replacement_ends = nodes.index(properties["existing_shapes"][-1])
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
            # If node IDs in properties["set_stops"] are not already
            # in stops.txt, create a new stop_id for them in stops
            existing_fk_ids = set(stops[TransitNetwork.STOPS_FOREIGN_KEY].tolist())
            nodes_df = self.road_net.nodes_df.loc[
                :, [TransitNetwork.STOPS_FOREIGN_KEY, "X", "Y"]
            ]
            for fk_i in properties["set_stops"]:
                if fk_i not in existing_fk_ids:
                    WranglerLogger.info(
                        "Creating a new stop in stops.txt for node ID: {}".format(fk_i)
                    )
                    # Add new row to stops
                    new_stop_id = str(int(fk_i) + TransitNetwork.ID_SCALAR)
                    if stop_id in stops["stop_id"].tolist():
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
                        nodes_df.loc[int(fk_i), "Y"],
                        nodes_df.loc[int(fk_i), "X"],
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

        # Replace self if in_place, else return
        if in_place:
            self.feed.shapes = shapes
            self.feed.stops = stops
            self.feed.stop_times = stop_times
        else:
            updated_network = copy.deepcopy(self)
            updated_network.feed.shapes = shapes
            updated_network.feed.stops = stops
            updated_network.feed.stop_times = stop_times
            return updated_network

    def _apply_transit_feature_change_frequencies(
        self, trip_ids: pd.Series, properties: dict, in_place: bool = True
    ) -> Union(None, TransitNetwork):
        freq = self.feed.frequencies.copy()

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

        # Update self or return a new object
        q = self.feed.frequencies.trip_id.isin(freq["trip_id"])
        if in_place:
            self.feed.frequencies.loc[q, properties["property"]] = build_value
        else:
            updated_network = copy.deepcopy(self)
            updated_network.loc[q, properties["property"]] = build_value
            return updated_network

    def apply_transit_managed_lane(
        self, trip_ids: pd.Series, node_ids: list, in_place: bool = True
    ) -> Union(None, TransitNetwork):
        # Traversed nodes without a stop should be negative integers
        all_stops = self.feed.stops[TransitNetwork.STOPS_FOREIGN_KEY].tolist()
        node_ids = [int(x) if str(x) in all_stops else int(x) * -1 for x in node_ids]

        self._apply_transit_feature_change_routing(
            trip_ids=trip_ids,
            properties={
                "existing": node_ids,
                "set": RoadwayNetwork.get_managed_lane_node_ids(node_ids),
            },
            in_place=in_place,
        )


class DotDict(dict):
    """
    dot.notation access to dictionary attributes
    Source: https://stackoverflow.com/questions/2352181/how-to-use-a-dot-to-access-members-of-dictionary
    """

    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)

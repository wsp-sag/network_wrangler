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

from .Logger import WranglerLogger
from .Utils import parse_time_spans
from .RoadwayNetwork import RoadwayNetwork


class TransitNetwork(object):
    """
    Representation of a Transit Network.

    Usage:
      import network_wrangler as wr
      stpaul = r'/home/jovyan/work/example/stpaul'
      tc=wr.TransitNetwork.read(path=stpaul)

    """

    # PK = primary key, FK = foreign key
    SHAPES_FOREIGN_KEY = "shape_model_node_id"
    STOPS_FOREIGN_KEY = "model_node_id"

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
    def read(feed_path: str, fast: bool = False) -> TransitNetwork:
        """
        Read GTFS feed from folder and TransitNetwork object
        """
        config = default_config()
        feed = ptg.load_feed(feed_path, config=config)
        WranglerLogger.info("Read in transit feed from: {}".format(feed_path))
        updated_config = TransitNetwork.validate_feed(feed, config)

        # Read in each feed so we can write over them
        new_feed = DotDict()
        for node in updated_config.nodes.keys():
            # Load (initiate Partridge's lazy load)
            new_feed[node.replace(".txt", "")] = feed.get(node)

        transit_network = TransitNetwork(feed=new_feed, config=updated_config)
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

        return updated_config

    def validate_frequencies(self) -> Bool:
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

    def validate_road_network_consistencies(self) -> Bool:
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

    def validate_transit_stops(self) -> Bool:
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

    def validate_transit_shapes(self) -> Bool:
        """
        Validates that all transit shapes are part of the roadway network.

        Returns:
            Boolean indicating if valid or not.
        """

        if self.road_net is None:
            raise ValueError(
                "RoadwayNetwork not set yet, see TransitNetwork.set_roadnet()"
            )

        shapes = self.feed.shapes
        nodes = self.road_net.nodes_df

        valid = True

        shape_ids = [
            int(s) for s in shapes[TransitNetwork.SHAPES_FOREIGN_KEY].to_list()
        ]
        node_ids = [int(n) for n in nodes[RoadwayNetwork.NODE_FOREIGN_KEY].to_list()]

        if not set(shape_ids).issubset(node_ids):
            valid = False
            missing_shapes = list(set(shape_ids) - set(node_ids))
            msg = "Not all transit shapes are part of the roadyway network. "
            msg += "Missing shapes ({}) from the roadway network are {}.".format(
                TransitNetwork.SHAPES_FOREIGN_KEY, missing_shapes
            )
            WranglerLogger.error(msg)

        return valid

    @staticmethod
    def route_ids_in_routestxt(feed):
        """
        STUB: Wherever route_id occurs, make sure it is in routes.txt

        Returns:
            Boolean
        """
        pass

    @staticmethod
    def trip_ids_in_tripstxt(feed):
        """
        STUB: Wherever trip_id occurs, make sure it is in trips.txt

        Returns:
            Boolean
        """
        pass

    @staticmethod
    def shape_ids_in_shapestxt(feed):
        """
        STUB: Wherever shape_id occurs, make sure it is in shapes.txt

        Returns:
            Boolean
        """
        pass

    @staticmethod
    def stop_ids_in_stopstxt(feed):
        """
        STUB: Wherever stop_id occurs, make sure it is in stops.txt

        Returns:
            Boolean
        """
        pass

    @staticmethod
    def validate_network_connectivity(feed):
        """
        Validates foreign keys are present in all connecting feed files.


        Returns:
            Boolean
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
        existing_shapes = self.feed.shapes
        # graphed_shapes = pd.DataFrame()

        # for shape_id in shapes:
        # TODO traverse point by point, mapping shortest path on graph,
        # then append to a list
        # return total list of all link ids
        # rebuild rows in shapes dataframe and add to graphed_shapes
        # make graphed_shapes a GeoDataFrame

        # self.feed.shapes = graphed_shapes

    def _graph_stops(self) -> None:
        existing_stops = self.feed.stops
        # graphed_stops = pd.DataFrame()

        # for stop_id in stops:
        # TODO

        # self.feed.stops = graphed_stops

    def write(self, path: str = ".", filename: str = None) -> None:
        """
        Writes a network in the transit network standard

        Parameters
        ------------
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

    def apply(self, project_card_dictionary: dict):
        """
        Wrapper method to apply a project to a transit network.

        args
        ------
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
        shapes = self.feed.shapes
        stop_times = self.feed.stop_times
        stops = self.feed.stops

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
        shape_ids = self.feed.trips[self.feed.trips.trip_id.isin(trip_ids)].shape_id
        for shape_id in shape_ids:
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
                    sort=False,
                    ignore_index=True,
                )
            else:
                this_shape = new_shape_rows

            # Renumber shape_pt_sequence
            this_shape["shape_pt_sequence"] = np.arange(len(this_shape))

            # Add rows back into shapes
            shapes = pd.concat(
                [shapes[shapes.shape_id != shape_id], this_shape], ignore_index=True
            )

        # Replace stop_times and stops records (if required)
        if stops_change:
            # If node IDs in properties["set_stops"] are not already
            # in stops.txt, create a new stop_id for them in stops
            if any(
                x not in stops[TransitNetwork.STOPS_FOREIGN_KEY].tolist()
                for x in properties["set_stops"]
            ):
                # FIXME
                WranglerLogger.error(
                    "Node ID is used that does not have an existing stop."
                )
                raise ValueError

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
                        sort=False,
                        ignore_index=True,
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
        freq = self.feed.frequencies

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

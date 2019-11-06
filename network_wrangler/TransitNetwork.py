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
from partridge.gtfs import Feed

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
    FK_SHAPES = "shape_model_node_id"
    FK_STOPS = "model_node_id"

    def __init__(self, feed: Feed = None, config: nx.DiGraph = None):
        """
        Constructor
        """
        self.feed: Feed = feed
        self.config: nx.DiGraph = config
        self.road_net: RoadwayNetwork = None
        self.graph: nx.MultiDiGraph = None

    @staticmethod
    def validate_feed(feed: Feed, config: nx.DiGraph) -> bool:
        """
        Since Partridge lazily loads the df, load each file to make sure it
        actually works.

        Partridge uses a DiGraph from the networkx library to represent the
        relationships between GTFS files. Each file is a 'node', and the
        relationship between files are 'edges'.
        """
        try:
            for node in config.nodes.keys():
                feed.get(node)
            return True
        except AttributeError:
            return False

    @staticmethod
    def read(feed_path: str, fast: bool = False) -> Tuple[nx.DiGraph, Feed]:
        """
        Read GTFS feed from folder and return a config and Partridge Feed object
        """
        config = default_config()
        feed = ptg.load_feed(feed_path, config=config)

        TransitNetwork.validate_feed(feed, config)

        transit_network = TransitNetwork(feed=feed, config=config)

        return transit_network

    def set_roadnet(self, road_net: RoadwayNetwork,
                    graph_shapes: bool = False, graph_stops: bool = False
                    ) -> None:
        self.road_net: RoadwayNetwork = road_net
        self.graph: nx.MultiDiGraph = RoadwayNetwork.ox_graph(
            road_net.nodes_df, road_net.links_df
        )
        if graph_shapes:
            self._graph_shapes()
        if graph_stops:
            self._graph_stops()

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
        for node in self.config.nodes.keys():
            df = self.feed.get(node)
            if not df.empty:
                if filename:
                    outpath = os.path.join(path, filename + "_" + node)
                else:
                    outpath = os.path.join(path, node)

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
                WranglerLogger.warning(
                    "Parallel Managed Lanes not implemented yet in Transit"
                )
                # TODO
            else:
                raise (BaseException)

        if project_card_dictionary.get("changes"):
            for project_dictionary in project_card_dictionary["changes"]:
                _apply_individual_change(project_dictionary)
        else:
            _apply_individual_change(project_card_dictionary)

    def select_transit_features(self, selection: dict) -> pd.Series:
        """
        Selects transit features that satisfy selection criteria

        Parameters
        ------------
        selection : dictionary

        Returns
        -------
        trip identifiers : list
           list of GTFS trip IDs in the selection
        """
        trips = self.feed.trips
        routes = self.feed.routes
        freq = self.feed.frequencies

        # Turn selection's values into lists if they are not already
        for key in selection.keys():
            if type(selection[key]) != list:
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
        if selection.get("time") is not None:
            selection["time"] = parse_time_spans(selection["time"])

            # Filter freq to trips in selection
            freq = freq[freq.trip_id.isin(trips["trip_id"])]
            freq = freq[freq.start_time == selection["time"][0]]
            freq = freq[freq.end_time == selection["time"][1]]

            # Filter trips table to those still in freq table
            trips = trips[trips.trip_id.isin(freq["trip_id"])]

        # If any other key exists, filter routes or trips accordingly
        for key in selection.keys():
            if key not in [
                "trip_id", "route_id", "route_short_name", "route_long_name",
                "time"
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

    def apply_transit_feature_change(
        self, trip_ids: pd.Series, properties: dict, in_place: bool = True
    ) -> Union(None, TransitNetwork):
        """
        Changes the transit attributes for the selected features based on the
        project card information passed

        Parameters
        ------------
        trip_ids : pd.Series
            all trip_ids to apply change to
        properties : list of dictionarys
            transit properties to change
        in_place : bool
            whether to apply changes in place or return a new network

        Returns
        -------
        None
        """
        for i in properties:
            if i["property"] in ["headway_secs"]:
                self.apply_transit_feature_change_frequencies(trip_ids, i, in_place)

            elif i["property"] in ["routing"]:
                self.apply_transit_feature_change_routing(trip_ids, i, in_place)

    def apply_transit_feature_change_routing(
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
            properties["set_stops"] = [
                str(i) for i in properties["set"] if i > 0
            ]
            if properties.get("existing") is not None:
                properties["existing_stops"] = [
                    str(i) for i in properties["existing"] if i > 0
                ]
            stops_change = True

        # Convert ints to objects
        properties["set_shapes"] = [str(abs(i)) for i in properties["set"]]
        properties["existing_shapes"] = [
            str(abs(i)) for i in properties["existing"]
        ]

        # Replace shapes records
        shape_ids = self.feed.trips[
            self.feed.trips.trip_id.isin(trip_ids)
        ].shape_id
        for shape_id in shape_ids:
            # Pop the rows that match shape_id
            this_shape = shapes[shapes.shape_id == shape_id]

            # Make sure they are ordered by shape_pt_sequence
            this_shape = this_shape.sort_values(by=["shape_pt_sequence"])

            # Build a pd.DataFrame of new shape records
            new_shape_rows = pd.DataFrame({
                "shape_id": shape_id,
                "shape_pt_lat": None,  # FIXME Populate from self.road_net?
                "shape_pt_lon": None,  # FIXME
                "shape_osm_node_id": None,  # FIXME
                "shape_pt_sequence": None,
                TransitNetwork.FK_SHAPES: properties["set_shapes"]
            })

            # If "existing" is specified, replace only that segment
            # Else, replace the whole thing
            if properties.get("existing") is not None:
                # Match list
                nodes = this_shape[TransitNetwork.FK_SHAPES].tolist()
                index_replacement_starts = nodes.index(
                    properties["existing_shapes"][0]
                )
                index_replacement_ends = nodes.index(
                    properties["existing_shapes"][-1]
                )
                this_shape = pd.concat([
                    this_shape.iloc[:index_replacement_starts-1],
                    new_shape_rows,
                    this_shape.iloc[index_replacement_ends+1:]
                ], sort=False, ignore_index=True)
            else:
                this_shape = new_shape_rows

            # Renumber shape_pt_sequence
            this_shape["shape_pt_sequence"] = np.arange(len(this_shape))

            # Add rows back into shapes
            shapes = pd.concat([
                shapes[shapes.shape_id != shape_id],
                this_shape
            ], ignore_index=True)

        # Replace stop_times and stops records (if required)
        if stops_change:
            # If node IDs in properties["set_stops"] are not already
            # in stops.txt, create a new stop_id for them in stops
            if any(x not in stops[TransitNetwork.FK_STOPS] for x in properties["set_stops"]):
                # FIXME
                WranglerLogger.error(
                    "Node ID is used that does not have an existing stop.")
                raise ValueError

            # Loop through all the trip_ids
            for trip_id in trip_ids:
                # Pop the rows that match trip_id
                this_stoptime = stop_times[stop_times.trip_id == trip_id]

                # Make sure the stop_times are ordered by stop_sequence
                this_stoptime = this_stoptime.sort_values(by=["stop_sequence"])

                # Build a pd.DataFrame of new shape records
                new_stoptime_rows = pd.DataFrame({
                    "trip_id": trip_id,
                    "arrival_time": None,
                    "departure_time": None,
                    "pickup_type": None,
                    "drop_off_type": None,
                    "stop_distance": None,
                    "timepoint": None,
                    "stop_is_skipped": None,
                    TransitNetwork.FK_STOPS: properties["set_stops"]
                })
                # Merge on stop_id
                new_stoptime_rows = new_stoptime_rows.merge(
                    stops[["stop_id", TransitNetwork.FK_STOPS]],
                    how="left",
                    left_on=TransitNetwork.FK_STOPS,
                    right_on=TransitNetwork.FK_STOPS
                )

                # If "existing" is specified, replace only that segment
                # Else, replace the whole thing
                if properties.get("existing") is not None:
                    # Match list
                    nodes = this_stoptime[TransitNetwork.FK_STOPS].tolist()
                    index_replacement_starts = nodes.index(
                        properties["existing_stops"][0]
                    )
                    index_replacement_ends = nodes.index(
                        properties["existing_stops"][-1]
                    )
                    this_stoptime = pd.concat([
                        this_stoptime.iloc[:index_replacement_starts-1],
                        new_stoptime_rows,
                        this_stoptime.iloc[index_replacement_ends+1:]
                    ], sort=False, ignore_index=True)
                else:
                    this_stoptime = new_stoptime_rows

                # Remove node ID
                del new_stoptime_rows[TransitNetwork.FK_STOPS]

                # Renumber stop_sequence
                this_stoptime["stop_sequence"] = np.arange(len(this_stoptime))

                # Add rows back into stoptime
                stop_times = pd.concat([
                    stop_times[stop_times.trip_id != trip_id],
                    this_stoptime
                ], ignore_index=True)

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

    def apply_transit_feature_change_frequencies(
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

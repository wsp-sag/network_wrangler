"""TransitNetwork class for representing a transit network.

Transit Networks are represented as a Wrangler-flavored GTFS Feed and optionally mapped to
a RoadwayNetwork object. The TransitNetwork object is the primary object for managing transit
networks in Wrangler.

Usage:

    ```python
    import network_wrangler as wr
    t = wr.load_transit(stpaul_gtfs)
    t.road_net = wr.load_roadway(stpaul_roadway)
    t = t.apply(project_card)
    write_transit(t, "output_dir")
    ```
"""

from __future__ import annotations

import copy
from typing import Union, Optional

import networkx as nx
import geopandas as gpd

from .validate import transit_road_net_consistency

from projectcard import ProjectCard, SubProject

from ..logger import WranglerLogger
from ..utils.utils import dict_to_hexkey
from ..utils.geo import to_points_gdf
from ..roadway.network import RoadwayNetwork
from .projects import (
    apply_transit_routing_change,
    apply_transit_property_change,
    apply_calculated_transit,
    apply_add_transit_route_change,
)
from .selection import TransitSelection
from .feed.feed import Feed

from .geo import (
    shapes_to_shape_links_gdf,
    stop_times_to_stop_time_links_gdf,
    stop_times_to_stop_time_points_gdf,
    shapes_to_trip_shapes_gdf,
)


class TransitRoadwayConsistencyError(Exception):
    """Error raised when transit network is inconsistent with roadway network."""

    pass


class TransitNetwork(object):
    """Representation of a Transit Network.

    Typical usage example:
    ``` py
    import network_wrangler as wr
    tc=wr.load_transit(stpaul_gtfs)
    ```

    Attributes:
        feed: gtfs feed object with interlinked tables.
        road_net (RoadwayNetwork): Associated roadway network object.
        graph (nx.MultiDiGraph): Graph for associated roadway network object.
        feed_path (str): Where the feed was read in from.
        validated_frequencies (bool): The frequencies have been validated.
        validated_road_network_consistency (): The network has been validated against
            the road network.
    """

    TIME_COLS = ["arrival_time", "departure_time", "start_time", "end_time"]

    def __init__(self, feed: Feed):
        """Constructor for TransitNetwork.

        Args:
            feed: Feed object representing the transit network gtfs tables
        """
        WranglerLogger.debug("Creating new TransitNetwork.")

        self._road_net: Optional[RoadwayNetwork] = None
        self.feed: Feed = feed
        self.graph: nx.MultiDiGraph = None

        # initialize
        self._consistent_with_road_net = False

        # cached selections
        self._selections: dict[str, dict] = {}

    @property
    def feed_path(self):
        """Pass through property from Feed."""
        return self.feed.feed_path

    @property
    def config(self):
        """Pass through property from Feed."""
        return self.feed.config

    @property
    def feed(self):
        """Feed associated with the transit network."""
        return self._feed

    @feed.setter
    def feed(self, feed: Feed):
        if not isinstance(feed, Feed):
            msg = f"TransitNetwork's feed value must be a valid Feed instance. \
                             This is a {type(feed)}."
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if self._road_net is None or transit_road_net_consistency(feed, self._road_net):
            self._feed = feed
            self._stored_feed_hash = copy.deepcopy(feed.hash)
        else:
            WranglerLogger.error("Can't assign Feed inconsistent with set Roadway Network.")
            raise TransitRoadwayConsistencyError(
                "Can't assign Feed inconsistent with set RoadwayNetwork."
            )

    @property
    def road_net(self) -> RoadwayNetwork:
        """Roadway network associated with the transit network."""
        return self._road_net

    @road_net.setter
    def road_net(self, road_net: RoadwayNetwork):
        if not isinstance(road_net, RoadwayNetwork):
            msg = f"TransitNetwork's road_net: value must be a valid RoadwayNetwork instance. \
                             This is a {type(road_net)}."
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if transit_road_net_consistency(self.feed, road_net):
            self._road_net = road_net
            self._stored_road_net_hash = copy.deepcopy(self.road_net.network_hash)
            self._consistent_with_road_net = True
        else:
            WranglerLogger.error(
                "Can't assign inconsistent RoadwayNetwork - Roadway Network not \
                                 set, but can be referenced separately."
            )
            raise TransitRoadwayConsistencyError("Can't assign inconsistent RoadwayNetwork.")

    @property
    def feed_hash(self):
        """Return the hash of the feed."""
        return self.feed.hash

    @property
    def consistent_with_road_net(self) -> bool:
        """Indicate if road_net is consistent with transit network.

        Checks the network hash of when consistency was last evaluated. If transit network or
        roadway network has changed, will re-evaluate consistency and return the updated value and
        update self._stored_road_net_hash.

        Returns:
            Boolean indicating if road_net is consistent with transit network.
        """
        updated_road = self.road_net.network_hash != self._stored_road_net_hash
        updated_feed = self.feed_hash != self._stored_feed_hash

        if updated_road or updated_feed:
            self._consistent_with_road_net = transit_road_net_consistency(self.feed, self.road_net)
            self._stored_road_net_hash = copy.deepcopy(self.road_net.network_hash)
            self._stored_feed_hash = copy.deepcopy(self.feed_hash)
        return self._consistent_with_road_net

    def __deepcopy__(self, memo):
        """Returns copied TransitNetwork instance with deep copy of Feed but not roadway net."""
        COPY_REF_NOT_VALUE = ["_road_net"]
        # Create a new, empty instance
        copied_net = self.__class__.__new__(self.__class__)
        # Return the new TransitNetwork instance
        attribute_dict = vars(self)

        # Copy the attributes to the new instance
        for attr_name, attr_value in attribute_dict.items():
            # WranglerLogger.debug(f"Copying {attr_name}")
            if attr_name in COPY_REF_NOT_VALUE:
                # If the attribute is in the COPY_REF_NOT_VALUE list, assign the reference
                setattr(copied_net, attr_name, attr_value)
            else:
                # WranglerLogger.debug(f"making deep copy: {attr_name}")
                # For other attributes, perform a deep copy
                setattr(copied_net, attr_name, copy.deepcopy(attr_value, memo))

        return copied_net

    def deepcopy(self):
        """Returns copied TransitNetwork instance with deep copy of Feed but not roadway net."""
        return copy.deepcopy(self)

    @property
    def stops_gdf(self) -> gpd.GeoDataFrame:
        """Return stops as a GeoDataFrame using set roadway geometry."""
        if self.road_net is not None:
            ref_nodes = self.road_net.nodes_df
        else:
            ref_nodes = None
        return to_points_gdf(self.feed.stops, nodes_df=ref_nodes)

    @property
    def shapes_gdf(self) -> gpd.GeoDataFrame:
        """Return aggregated shapes as a GeoDataFrame using set roadway geometry."""
        if self.road_net is not None:
            ref_nodes = self.road_net.nodes_df
        else:
            ref_nodes = None
        return shapes_to_trip_shapes_gdf(self.feed.shapes, ref_nodes_df=ref_nodes)

    @property
    def shape_links_gdf(self) -> gpd.GeoDataFrame:
        """Return shape-links as a GeoDataFrame using set roadway geometry."""
        if self.road_net is not None:
            ref_nodes = self.road_net.nodes_df
        else:
            ref_nodes = None
        return shapes_to_shape_links_gdf(self.feed.shapes, ref_nodes_df=ref_nodes)

    @property
    def stop_time_links_gdf(self) -> gpd.GeoDataFrame:
        """Return stop-time-links as a GeoDataFrame using set roadway geometry."""
        if self.road_net is not None:
            ref_nodes = self.road_net.nodes_df
        else:
            ref_nodes = None
        return stop_times_to_stop_time_links_gdf(
            self.feed.stop_times, self.feed.stops, ref_nodes_df=ref_nodes
        )

    @property
    def stop_times_points_gdf(self) -> gpd.GeoDataFrame:
        """Return stop-time-points as a GeoDataFrame using set roadway geometry."""
        if self.road_net is not None:
            ref_nodes = self.road_net.nodes_df
        else:
            ref_nodes = None

        return stop_times_to_stop_time_points_gdf(
            self.feed.stop_times, self.feed.stops, ref_nodes_df=ref_nodes
        )

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
        key = dict_to_hexkey(selection_dict)

        if (key not in self._selections) or overwrite:
            WranglerLogger.debug(f"Performing selection from key: {key}")
            self._selections[key] = TransitSelection(self, selection_dict)
        else:
            WranglerLogger.debug(f"Using cached selection from key: {key}")

        if not self._selections[key]:
            WranglerLogger.debug(
                f"No links or nodes found for selection dict: \n {selection_dict}"
            )
            raise ValueError("Selection not successful.")
        return self._selections[key]

    def apply(self, project_card: Union[ProjectCard, dict], **kwargs) -> "TransitNetwork":
        """Wrapper method to apply a roadway project, returning a new TransitNetwork instance.

        Args:
            project_card: either a dictionary of the project card object or ProjectCard instance
            **kwargs: keyword arguments to pass to project application
        """
        if not (isinstance(project_card, ProjectCard) or isinstance(project_card, SubProject)):
            project_card = ProjectCard(project_card)

        if not project_card.valid:
            WranglerLogger.error("Invalid Project Card: {project_card}")
            raise ValueError(f"Project card {project_card.project} not valid.")

        if project_card.sub_projects:
            for sp in project_card.sub_projects:
                WranglerLogger.debug(f"- applying subproject: {sp.change_type}")
                self._apply_change(sp, **kwargs)
            return self
        else:
            return self._apply_change(project_card, **kwargs)

    def _apply_change(
        self,
        change: Union[ProjectCard, SubProject],
        reference_road_net: Optional[RoadwayNetwork] = None,
    ) -> TransitNetwork:
        """Apply a single change: a single-project project or a sub-project."""
        if not isinstance(change, SubProject):
            WranglerLogger.info(f"Applying Project to Transit Network: {change.project}")

        if change.change_type == "transit_property_change":
            return apply_transit_property_change(
                self,
                self.get_selection(change.service),
                change.transit_property_change,
            )

        elif change.change_type == "transit_routing_change":
            return apply_transit_routing_change(
                self,
                self.get_selection(change.service),
                change.transit_routing_change,
                reference_road_net=reference_road_net,
            )

        elif change.change_type == "add_new_route":
            return apply_add_transit_route_change(self, change.transit_route_addition)

        elif change.change_type == "roadway_deletion":
            # FIXME
            raise NotImplementedError("Roadway deletion check not yet implemented.")

        elif change.change_type == "pycode":
            return apply_calculated_transit(self, change.pycode)

        else:
            msg = f"Not a currently valid transit project: {change}."
            WranglerLogger.error(msg)
            raise NotImplementedError(msg)

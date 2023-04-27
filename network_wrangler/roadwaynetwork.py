#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
import copy
from collections import defaultdict
from random import randint
from typing import Any, Collection, List, Optional, Union, Tuple

import pandas as pd
import geopandas as gpd
import json
import networkx as nx
import numpy as np
import osmnx as ox

from geopandas.geodataframe import GeoDataFrame

from pandas.core.frame import DataFrame

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError


from shapely.geometry import Point

import projects
from .logger import WranglerLogger
from .projectcard import ProjectCard
from .roadway.model_roadway import ModelRoadwayNetwork
from .roadway.graph import net_to_graph
from .roadway.links import read_links
from .roadway.nodes import read_nodes
from .roadway.shapes import read_shapes
from .selection import Selection, MODES_TO_NETWORK_LINK_VARIABLES
from .utils import (
    create_unique_shape_id,
    location_reference_from_nodes,
    line_string_from_location_references,
    links_df_to_json,
    parse_time_spans_to_secs,
    point_from_xy,
    point_df_to_geojson,
    update_points_in_linestring,
    get_point_geometry_from_linestring,
)


class RoadwayNetwork(object):
    """
    Representation of a Roadway Network.

    Typical usage example:

    ```py
    net = RoadwayNetwork.read(
        link_file=MY_LINK_FILE,
        node_file=MY_NODE_FILE,
        shape_file=MY_SHAPE_FILE,
    )
    my_selection = {
        "link": [{"name": ["I 35E"]}],
        "A": {"osm_node_id": "961117623"},  # start searching for segments at A
        "B": {"osm_node_id": "2564047368"},
    }
    net.select_roadway_features(my_selection)

    my_change = [
        {
            'property': 'lanes',
            'existing': 1,
            'set': 2,
        },
        {
            'property': 'drive_access',
            'set': 0,
        },
    ]

    my_net.apply_roadway_feature_change(
        my_net.select_roadway_features(my_selection),
        my_change
    )

        net = create_managed_lane_network(net)
        net.is_network_connected(mode="drive", nodes=self.m_nodes_df, links=self.m_links_df)
        _, disconnected_nodes = net.assess_connectivity(
            mode="walk",
            ignore_end_nodes=True,
            nodes=self.m_nodes_df,
            links=self.m_links_df
        )
        net.write(filename=my_out_prefix, path=my_dir, for_model = True)
    ```

    Attributes:
        nodes_df (GeoDataFrame): dataframe of of node records. Contains
            `NodesParams` dataclass which is mapped to the `.params` attribute of the dataframe.
            primary key: `.params.primary_key`

        links_df (GeoDataFrame): dataframe of link records and associated properties. Contains
            `LinksParams` dataclass which is mapped to the `.params` attribute of the dataframe.
            primary key: `.params.primary_key`
            foreign key to from node: `.params.from_node`
            foreign key to to node: `.params.to_node`
            foreign key to shapes: `.params.fk_to_shape`


        shapes_df (GeoDataFrame): data from of detailed shape records. Contains
            `ShapesParams` dataclass which is mapped to the `.params` attribute of the dataframe.
            primary key: `.params.primary_key`.  This is lazily created iff it is called because
            shapes files can be expensive to read.

        selections (dict): dictionary of stored `Selection` objects, mapped by `Selection.sel_key`
            in case they are made repeatedly.

        crs (str): coordinate reference system in ESPG number format. Defaults to DEFAUULT_CRS
            which is set to 4326, WGS 84 Lat/Long

        network_hash: dynamic property of the hashed value of links_df and nodes_df. Used for
            quickly identifying if a network has changed since various expensive operations have
            taken place (i.e. generating a ModelRoadwayNetwork or a network graph)

        model_net (ModelRoadwayNetwork): referenced `ModelRoadwayNetwork` object which will be
            lazily created if None or if the `network_hash` has changed.

        num_managed_lane_links (int): dynamic property number of managed lane links.
    """

    DEFAULT_CRS = 4326

    def __init__(
        self,
        links_df: GeoDataFrame,
        nodes_df: GeoDataFrame,
        shapes_df: GeoDataFrame = None,
        shapes_file: str = None,
        crs: int = DEFAULT_CRS,
    ):
        """
        Constructor for RoadwayNetwork object.

        Args:
            nodes_df: GeoDataFrame of of node records.
            links_df: GeoDataFrame of of link records.
            shapes_df: GeoDataFrame of detailed shape records
            shapes_file: path to shapes file to lazily read it in when needed.
            crs: coordinate reference system in ESPG number format. Defaults to DEFAUULT_CRS
                which is set to 4326, WGS 84 Lat/Long

        """

        self.crs = crs
        self.nodes_df = nodes_df
        self.links_df = links_df

        if shapes_df is None and shapes_file is None:
            WranglerLogger.warning("No shapes associated with network!")
            raise ValueError("Should specify either shapes or shapes_file.")

        self._shapes_df = shapes_df

        # Model network
        self._model_net = None

        # cached selections
        self._selections = {}

        # cached modal graphs of full network
        self._modal_graphs = defaultdict(lambda: {"graph": None, "hash": None})

    @property
    def shapes_df(self):
        if self._shapes is None:
            self._shapes = read_shapes(
                self.shapes_file, crs=self.crs, shapes_params=self.shapes_params
            )
        return self.shapes_df

    @property
    def network_hash(self):
        return hash(tuple(self.links_df, self.nodes_df))

    @property
    def model_net(self):
        if self._model_net is None or self._model_net._net_hash != self.network_hash:
            self._model_net = ModelRoadwayNetwork(self)
        return self._model_net

    @property
    def num_managed_lane_links(self):
        if "managed" in self.links_df.columns:
            return len((self.links_df[self.links_df["managed"] == 1]).index)
        else:
            return 0

    @staticmethod
    def read(
        links_file: str,
        nodes_file: str,
        shapes_file: str = None,
        links_params: "LinksParams" = None,
        nodes_params: "NodesParams" = None,
        shapes_params: "ShapesParams" = None,
        crs: int = DEFAULT_CRS,
        read_shapes: bool = False,
    ) -> RoadwayNetwork:
        """
        Reads a network from the roadway network standard
        Validates that it conforms to the schema

        args:
            links_file: full path to the link file
            nodes_file: full path to the node file
            shapes_file: full path to the shape file
            links_params: LinkParams instance to use. Will default to default
                values for LinkParams
            nodes_params: NodeParames instance to use. Will default to default
                values for NodeParams
            shapes_params: ShapeParames instance to use. Will default to default
                values for ShapeParams
            crs: coordinate reference system. Defaults to DEFAULT_CRS which defaults to 4326
                which is WGS84 lat/long.
            read_shapes: if True, will read shapes into network instead of only lazily
                reading them when they are called. Defaults to False.

        Returns: a RoadwayNetwork instance

        .. todo:: Turn off fast=True as default
        """
        links_df = read_links(links_file, crs=crs, links_params=links_params)
        nodes_df = read_nodes(nodes_file, crs=crs, nodes_params=nodes_params)

        shapes_df = None
        if read_shapes:
            shapes_df = read_shapes(shapes_file, crs=crs, shapes_params=shapes_params)

        roadway_network = RoadwayNetwork(
            nodes=nodes_df,
            links=links_df,
            shapes=shapes_df,
            crs=crs,
            link_file=links_file,
            node_file=nodes_file,
            shape_file=shapes_file,
        )

        return roadway_network

    def write(
        self,
        path: str = ".",
        filename: str = "",
    ) -> None:
        """
        Writes a network in the roadway network standard

        args:
            path: the path were the output will be saved
            filename: the name prefix of the roadway files that will be generated
        """

        if not os.path.exists(path):
            WranglerLogger.debug("\nPath [%s] doesn't exist; creating." % path)
            os.mkdir(path)

        links_file = os.path.join(path, f"{filename}{'_' if filename else ''}link.json")
        nodes_file = os.path.join(
            path, f"{filename}{'_' if filename else ''}node.geojson"
        )
        shapes_file = os.path.join(
            path, f"{filename}{'_' if filename else ''}shape.geojson"
        )

        link_property_columns = self.links_df.columns.values.tolist()
        link_property_columns.remove("geometry")
        links_json = links_df_to_json(self.links_df, link_property_columns)
        with open(links_file, "w") as f:
            json.dump(links_json, f)

        # geopandas wont let you write to geojson because
        # it uses fiona, which doesn't accept a list as one of the properties
        # so need to convert the df to geojson manually first
        property_columns = self.nodes_df.columns.values.tolist()
        property_columns.remove("geometry")

        nodes_geojson = point_df_to_geojson(self.nodes_df, property_columns)

        with open(nodes_file, "w") as f:
            json.dump(nodes_geojson, f)

        self.shapes_df.to_file(shapes_file, driver="GeoJSON")

    @property
    def link_shapes_df(self) -> gpd.GeoDataFrame:
        """
        Add shape geometry to liks if available

        returns: shapes merged to nodes dataframe
        """

        _links_df = copy.deepcopy(self.links_df)
        link_shapes_df = _links_df.merge(
            self.shapes_df,
            left_on=self.links_df.params.fk_to_shape,
            right_on=self.shapes_df.params.primary_key,
            how="left",
        )
        return link_shapes_df

    def get_selection(self, selection_dict: dict) -> Selection:
        """Return selection if it already exists, otherwise performan selection.

        Args:
            selection_dict (dict): _description_

        Returns:
            Selection: _description_
        """
        key = Selection._assign_selection_key(selection_dict)
        if not key in self._selection:
            self._selection[key] = Selection(self, selection_dict)
        return self._selection[key]

    def modal_graph_hash(self, mode):
        """Hash of the links in order to detect a network change from when graph created."""
        return hash(tuple(self.links_df, mode))

    def get_modal_graph(self, mode):
        if mode not in MODES_TO_NETWORK_LINK_VARIABLES.keys():
            raise KeyError(
                f"Mode {mode} is not a valid network selection mode. Valid modes: {MODES_TO_NETWORK_LINK_VARIABLES.keys()}"
            )
        if self._modal_graphs[mode]["hash"] != self.modal_graph_hash(mode):
            self._modal_graphs[mode]["graph"] = net_to_graph(self, [mode])

        return self._modal_graphs[mode]["graph"]

    def select_roadway_features(
        self, selection: dict, search_mode="drive", force_search=False
    ) -> list:
        """
        Selects roadway features that satisfy selection criteria
        #TODO
        Example usage:
            net.select_roadway_features(
              selection = [ {
                #   a match condition for the from node using osm,
                #   shared streets, or model node number
                'from': {'osm_model_link_id': '1234'},
                #   a match for the to-node..
                'to': {'shstid': '4321'},
                #   a regex or match for facility condition
                #   could be # of lanes, facility type, etc.
                'facility': {'name':'Main St'},
                }, ... ])

        Args:
            selection : dictionary with keys for:
                 A - from node
                 B - to node
                 link - which includes at least a variable for `name` or 'all' if all selected
            search_mode: will be overridden if 'link':'all'

        Returns: a list of indices for the selected links or nodes
        TODO
        """
        WranglerLogger.debug("validating selection")
        self.validate_selection(selection)

        # create a unique key for the selection so that we can cache it
        sel_key = self.build_selection_key(selection)
        WranglerLogger.debug("Selection Key: {}".format(sel_key))

        self.selections[sel_key] = {"selection_found": False}

        if "links" in selection:
            return self.select_roadway_link_features(
                selection,
                sel_key,
                force_search=force_search,
                search_mode=search_mode,
            )
        if "nodes" in selection:
            return self.select_node_features(
                selection,
                sel_key,
            )

        raise ValueError("Invalid selection type. Must be either 'links' or 'nodes'.")

    def select_node_features(
        self,
        selection: dict,
        sel_key: str,
    ) -> list:
        """Select Node Features.
        #TODO
        Args:
            selection (dict): selection dictionary from project card.
            sel_key (str): key to store selection in self.selections under.

        Returns:
            List of indices for selected nodes in self.nodes_df
        """
        WranglerLogger.debug("Selecting nodes.")
        if selection.get("nodes") == "all":
            return self.nodes_df.index.tolist()

        sel_query = ProjectCard.build_selection_query(
            selection=selection,
            type="nodes",
            unique_ids=self.nodes_df.params.unique_ids,
        )
        WranglerLogger.debug("Selecting node features:\n{}".format(sel_query))

        self.selections[sel_key]["selected_nodes"] = self.nodes_df.query(
            sel_query, engine="python"
        )

        if len(self.selections[sel_key]["selected_nodes"]) > 0:
            self.selections[sel_key]["selection_found"] = True
        else:
            raise ValueError(f"No nodes found for selection: {selection}")

        return self.selections[sel_key]["selected_nodes"].index.tolist()

    def select_roadway_link_features(
        self,
        selection_dict: dict,
        force_search: bool = False,
        additional_requirements: dict = {"drive_access": True},
        ignore: list = [],
    ) -> list:
        """_summary_
        #TODO
        Args:
            selection (dict): _description_
            force_search (bool, optional): _description_. Defaults to False.
            search_mode (str, optional): _description_. Defaults to "drive".

        Returns:
            List of indices for selected links in self.links_df
        """
        sel_key = Selection._assign_selection_key(selection_dict)

        # if this selection has been found before, return the previously selected links
        if sel_key in self.selections:
            if self.selections[sel_key].found and not force_search:
                return self.selections[sel_key].selected_links

            _sel = self.selections[sel_key].select()
            return _sel.selected_links

        self.selections[sel_key] = Selection(
            self,
            selection_dict,
            additional_requirements=additional_requirements,
            ignore=ignore,
        )
        return self.selections[sel_key].select()

    def validate_properties(
        self,
        df: pd.DataFrame,
        properties: dict,
        ignore_existing: bool = False,
        require_existing_for_change: bool = False,
    ) -> bool:
        """
        If there are change or existing commands, make sure that that
        property exists in the network.

        Args:
            properties : properties dictionary to be evaluated
            df: links_df or nodes_df or shapes_df to check for compatibility with
            ignore_existing: If True, will only warn about properties
                that specify an "existing" value.  If False, will fail.
            require_existing_for_change: If True, will fail if there isn't
                a specified value in theproject card for existing when a
                change is specified.

        Returns: boolean value as to whether the properties dictonary is valid.
        """

        valid = True
        for p in properties:
            if p["property"] not in df.columns and p.get("change"):
                WranglerLogger.error(
                    f'"Change" is specified for attribute { p["property"]}, but doesn\'t \
                            exist in base network'
                )
                valid = False
            if (
                p["property"] not in df.columns
                and p.get("existing")
                and not ignore_existing
            ):
                WranglerLogger.error(
                    f'"Existing" is specified for attribute { p["property"]}, but doesn\'t \
                        exist in base network'
                )
                valid = False
            if p.get("change") and not p.get("existing"):
                if require_existing_for_change:
                    WranglerLogger.error(
                        f'"Change" is specified for attribute {p["property"]}, but there \
                            isn\'t a value for existing.\nTo proceed, run with the setting \
                            require_existing_for_change=False'
                    )
                    valid = False
                else:
                    WranglerLogger.warning(
                        f'"Change" is specified for attribute {p["property"]}, but there \
                            isn\'t a value for existing'
                    )

        if not valid:
            raise ValueError("Property changes are not valid:\n  {properties")

    def apply(
        self, project_card_dictionary: dict, _subproject: bool = False
    ) -> "RoadwayNetwork":
        """
        Wrapper method to apply a roadway project, returning a new RoadwayNetwork instance.

        Args:
            project_card_dictionary: a dictionary of the project card object
            _subproject: boolean indicating if this is a subproject under a "changes" heading.
                Defaults to False. Will be set to true with code when necessary.

        """
        # Need to reset the network graph every time the network changes
        self._graph = {}

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

        _facility = project_dictionary.get("facility")
        _category = project_dictionary.get("category").lower()

        if _facility:
            WranglerLogger.info(f"Selecting Facility: {_facility}")

            _geometry_type = list({"links", "nodes"}.intersection(set(_facility)))
            assert (
                len(_geometry_type) == 1
            ), "Facility must have exactly one of 'links' or 'nodes'"
            _geometry_type = _geometry_type[0]

            _df_idx = self.select_roadway_features(_facility)

        if _category == "roadway property change":
            return projects.apply_roadway_property_change(
                self,
                _df_idx,
                project_dictionary["properties"],
                geometry_type=_geometry_type,
            )
        elif _category == "parallel managed lanes":
            return projects.apply_parallel_managed_lanes(
                self,
                _df_idx,
                project_dictionary["properties"],
            )
        elif _category == "add new roadway":
            return projects.apply_add_new_roadway(
                self,
                project_dictionary.get("links", []),
                project_dictionary.get("nodes", []),
            )
        elif _category == "roadway deletion":
            return projects.apply_roadway_deletion(
                self,
                project_dictionary.get("links", []),
                project_dictionary.get("nodes", []),
            )
        elif _category == "calculated roadway":
            return projects.apply_calculated_roadway(
                self,
                project_dictionary["pycode"],
            )
        else:
            raise (ValueError(f"Invalid Project Card Category: {_category}"))

    def update_node_geometry(self, updated_nodes: List = None) -> gpd.GeoDataFrame:
        """Adds or updates the geometry of the nodes in the network based on XY coordinates.

        Assumes XY are in self.crs.
        Also updates the geometry of links and shapes that reference these nodes.

        Args:
            updated_nodes: List of nodes to update. Defaults to all nodes.

        Returns:
           gpd.GeoDataFrame: nodes geodataframe with updated geometry.
        """
        if updated_nodes:
            updated_nodes_df = copy.deepcopy(
                self.nodes_df.loc[
                    self.nodes_df[self.nodes_df.params.primary_key].isin(updated_nodes)
                ]
            )
        else:
            updated_nodes_df = copy.deepcopy(self.nodes_df)
            updated_nodes = self.nodes_df.index.values.tolist()

        if len(updated_nodes_df) < 25:
            WranglerLogger.debug(
                f"Original Nodes:\n{updated_nodes_df[['X','Y','geometry']]}"
            )

        updated_nodes_df["geometry"] = updated_nodes_df.apply(
            lambda x: point_from_xy(
                x["X"],
                x["Y"],
                xy_crs=updated_nodes_df.crs,
                point_crs=updated_nodes_df.crs,
            ),
            axis=1,
        )
        WranglerLogger.debug(f"{len(self.nodes_df)} nodes in network before update")
        if len(updated_nodes_df) < 25:
            WranglerLogger.debug(
                f"Updated Nodes:\n{updated_nodes_df[['X','Y','geometry']]}"
            )
        self.nodes_df.update(
            updated_nodes_df[[updated_nodes_df.params.primary_key, "geometry"]]
        )
        WranglerLogger.debug(f"{len(self.nodes_df)} nodes in network after update")
        if len(self.nodes_df) < 25:
            WranglerLogger.debug(
                f"Updated self.nodes_df:\n{self.nodes_df[['X','Y','geometry']]}"
            )

        self._update_node_geometry_in_links_shapes(updated_nodes_df)

    @staticmethod
    def nodes_in_links(
        links_df: pd.DataFrame,
    ) -> Collection:
        """Returns a list of nodes that are contained in the links.

        Args:
            links_df: Links which to return node list for
        """
        if len(links_df) < 25:
            WranglerLogger.debug(f"Links:\n{links_df[links_df.params.fks_to_nodes]}")
        nodes_list = list(
            set(pd.concat([links_df[c] for c in links_df.params.fks_to_nodes]).tolist())
        )
        if len(nodes_list) < 25:
            WranglerLogger.debug(f"_node_list:\n{nodes_list}")
        return nodes_list

    @staticmethod
    def links_with_nodes(
        links_df: pd.DataFrame, node_id_list: list
    ) -> gpd.GeoDataFrame:
        """Returns a links geodataframe which start or end at the nodes in the list.

        Args:
            links_df: dataframe of links to search for nodes in
            node_id_list (list): List of nodes to find links for.  Nodes should be identified
                by the foreign key - the one that is referenced in LINK_FOREIGN_KEY.
        """
        # If nodes are equal to all the nodes in the links, return all the links
        _nodes_in_links = RoadwayNetwork.nodes_in_links(links_df)
        WranglerLogger.debug(
            f"# Nodes: {len(node_id_list)}\nNodes in links:{len(_nodes_in_links)}"
        )
        if len(set(node_id_list) - set(_nodes_in_links)) == 0:
            return links_df

        WranglerLogger.debug(f"Finding links assocated with {len(node_id_list)} nodes.")
        if len(node_id_list) < 25:
            WranglerLogger.debug(f"node_id_list: {node_id_list}")

        _selected_links_df = links_df[
            links_df.isin({c: node_id_list for c in links_df.params.fks_to_nodes})
        ]
        WranglerLogger.debug(
            f"Temp Selected {len(_selected_links_df)} associated with {len(node_id_list)} nodes."
        )
        """TODO
        _query_parts = [
            f"{prop} == {str(n)}"
            for prop in links_df.params.fks_to_node
            for n in node_id_list
        ]
        
        _query = " or ".join(_query_parts)
        _selected_links_df = links_df.query(_query, engine="python")
        """
        WranglerLogger.debug(
            f"Selected {len(_selected_links_df)} associated with {len(node_id_list)} nodes."
        )

        return _selected_links_df

    def _update_node_geometry_in_links_shapes(
        self,
        updated_nodes_df: gpd.GeoDataFrame,
    ) -> None:
        """Updates the locationReferences & geometry for given links & shapes for a given node df

        Should be called by any function that changes a node location.

        NOTES:
         - For shapes, this will mutate the geometry of a shape in place for the start and end node
            ...but not the nodes in-between.  Something to consider...

        Args:
            updated_nodes_df: gdf of nodes with updated geometry.
        """
        _node_ids = updated_nodes_df.index.tolist()
        updated_links_df = copy.deepcopy(
            RoadwayNetwork.links_with_nodes(self.links_df, _node_ids)
        )

        _shape_ids = updated_links_df[self.links_df.params.fk_to_shape].tolist()
        updated_shapes_df = copy.deepcopy(
            self.shapes_df.loc[
                self.shapes_df[self.sshapes_df.params.primary_key].isin(_shape_ids)
            ]
        )

        updated_links_df["locationReferences"] = self._create_link_locationreferences(
            updated_links_df
        )
        updated_links_df["geometry"] = updated_links_df["locationReferences"].apply(
            line_string_from_location_references,
        )

        updated_shapes_df["geometry"] = self._update_existing_shape_geometry_from_nodes(
            updated_shapes_df, updated_links_df
        )

        self.links_df.update(
            updated_links_df[
                [self.links_df.params.primary_key, "geometry", "locationReferences"]
            ]
        )
        self.shapes_df.update(
            updated_shapes_df[[self.shapes_df.params.primary_key, "geometry"]]
        )

    def _create_link_locationreferences(self, links_df: pd.DataFrame) -> pd.Series:
        locationreferences_s = links_df.apply(
            lambda x: location_reference_from_nodes(
                [
                    self.nodes_df.loc[
                        self.nodes_df.index == x[links_df.params.from_node]
                    ].squeeze(),
                    self.nodes_df[
                        self.nodes_df.index == x[links_df.params.to_node]
                    ].squeeze(),
                ]
            ),
            axis=1,
        )
        return locationreferences_s

    def _update_existing_shape_geometry_from_nodes(
        self, updated_shapes_df, updated_links_df
    ) -> gpd.GeoSeries:
        # WranglerLogger.debug(f"updated_shapes_df:\n {updated_shapes_df}")
        # update the first and last coordinates for the shape

        _df = updated_shapes_df[[self.shapes_df.params.primary_key, "geometry"]].merge(
            updated_links_df[[self.links_df.params.fk_to_shape, "geometry"]],
            left_on=self.shapes_df.params.primary_key,
            right_on=self.links_df.params.fk_to_shape,
            suffixes=["_old_shape", "_link"],
            how="left",
        )

        for position in [0, -1]:
            _df["geometry"] = _df.apply(
                lambda x: update_points_in_linestring(
                    x["geometry_old_shape"],
                    _df["geometry_link"][0].coords[position],
                    position,
                ),
                axis=1,
            )
        return _df["geometry"]

    def has_node(self, unique_node_id) -> bool:
        """Queries if network has node based on nodes_df.params.primary_key.

        Args:
            unique_node_id (_type_): value of nodes_df.params.primary_key
        """

        has_node = (
            self.nodes_df[self.nodes_df.params.primary_key].isin([unique_node_id]).any()
        )

        return has_node

    def has_link(self, link_key_values: tuple) -> bool:
        """Returns true if network has links with link_key_values in the fields associated with
        self.links_df.params.fks_to_nodes.

        Args:
            link_key_values: Tuple of values corresponding with RoadwayNetwork.LINK_FOREIGN_KEY_TO_ ODE properties.
                If self.links_df.params.fks_to_nodes is ("A","B"), then (1,2) references the link of A=1 and B=2.
        """
        _query_parts = [
            f"{k} == {str(v)}"
            for k, v in zip(self.links_df.params.fks_to_nodes, link_key_values)
        ]
        _query = " and ".join(_query_parts)
        _links = self.links_df.query(_query, engine="python")

        return bool(len(_links))

    def _shapes_without_links(self) -> pd.Series:
        """Returns shape ids that don't have associated links."""

        _ids_in_shapes = self.shapes_df[self.shapes_df.params.primary_key]
        _ids_in_links = self.links_df[self.links_df.params.fk_to_shape]

        shapes_missing_links = _ids_in_shapes[~_ids_in_shapes.isin(_ids_in_links)]
        return shapes_missing_links

    def get_property_by_time_period_and_group(
        self, property, time_period=None, category=None
    ):
        """
        Return a series for the properties with a specific group or time period.

        args
        ------
        property: str
          the variable that you want from network
        time_period: list(str)
          the time period that you are querying for
          i.e. ['16:00', '19:00']
        category: str or list(str)(Optional)
          the group category
          i.e. "sov"

          or

          list of group categories in order of search, i.e.
          ["hov3","hov2"]

        returns
        --------
        pandas series
        """

        def _get_property(
            v,
            time_spans=None,
            category=None,
            return_partial_match: bool = False,
            partial_match_minutes: int = 60,
        ):
            """

            .. todo:: return the time period with the largest overlap

            """

            if category and not time_spans:
                WranglerLogger.error(
                    "\nShouldn't have a category group without time spans"
                )
                raise ValueError("Shouldn't have a category group without time spans")

            # simple case
            if type(v) in (int, float, str):
                return v

            if not category:
                category = ["default"]
            elif isinstance(category, str):
                category = [category]
            search_cats = [c.lower() for c in category]

            # if no time or group specified, but it is a complex link situation
            if not time_spans:
                if "default" in v.keys():
                    return v["default"]
                else:
                    WranglerLogger.debug(f"variable: {v}")
                    msg = f"Variable {v} is more complex in network than query"
                    WranglerLogger.error(msg)
                    raise ValueError(msg)

            if v.get("timeofday"):
                categories = []
                for tg in v["timeofday"]:
                    if (
                        (time_spans[0] >= tg["time"][0])
                        and (time_spans[1] <= tg["time"][1])
                        and (time_spans[0] <= time_spans[1])
                    ):
                        if tg.get("category"):
                            categories += tg["category"]
                            for c in search_cats:
                                print("CAT:", c, tg["category"])
                                if c in tg["category"]:
                                    # print("Var:", v)
                                    # print(
                                    #    "RETURNING:", time_spans, category, tg["value"]
                                    # )
                                    return tg["value"]
                        else:
                            # print("Var:", v)
                            # print("RETURNING:", time_spans, category, tg["value"])
                            return tg["value"]

                    if (
                        (time_spans[0] >= tg["time"][0])
                        and (time_spans[1] <= tg["time"][1])
                        and (time_spans[0] > time_spans[1])
                        and (tg["time"][0] > tg["time"][1])
                    ):
                        if tg.get("category"):
                            categories += tg["category"]
                            for c in search_cats:
                                print("CAT:", c, tg["category"])
                                if c in tg["category"]:
                                    # print("Var:", v)
                                    # print(
                                    #    "RETURNING:", time_spans, category, tg["value"]
                                    # )
                                    return tg["value"]
                        else:
                            # print("Var:", v)
                            # print("RETURNING:", time_spans, category, tg["value"])
                            return tg["value"]

                    # if there isn't a fully matched time period, see if there is an overlapping
                    # one right now just return the first overlapping ones
                    # TODO return the time period with the largest overlap

                    if (
                        (time_spans[0] >= tg["time"][0])
                        and (time_spans[0] <= tg["time"][1])
                    ) or (
                        (time_spans[1] >= tg["time"][0])
                        and (time_spans[1] <= tg["time"][1])
                    ):
                        overlap_minutes = max(
                            0,
                            min(tg["time"][1], time_spans[1])
                            - max(time_spans[0], tg["time"][0]),
                        )
                        # print("OLM",overlap_minutes)
                        if not return_partial_match and overlap_minutes > 0:
                            WranglerLogger.debug(
                                f"Couldn't find time period consistent with {time_spans}, but \
                                    found a partial match: {tg['time']}. Consider allowing \
                                    partial matches using 'return_partial_match' keyword or \
                                    updating query."
                            )
                        elif (
                            overlap_minutes < partial_match_minutes
                            and overlap_minutes > 0
                        ):
                            WranglerLogger.debug(
                                f"Time period: {time_spans} overlapped less than the minimum number \
                                of minutes ({overlap_minutes}<{partial_match_minutes}) to be \
                                considered a match with time period in network: {tg['time']}."
                            )
                        elif overlap_minutes > 0:
                            WranglerLogger.debug(
                                f"Returning a partial time period match. Time period: {time_spans}\
                                overlapped the minimum number of minutes ({overlap_minutes}>=\
                                {partial_match_minutes}) to be considered a match with time period\
                                 in network: {tg['time']}."
                            )
                            if tg.get("category"):
                                categories += tg["category"]
                                for c in search_cats:
                                    print("CAT:", c, tg["category"])
                                    if c in tg["category"]:
                                        # print("Var:", v)
                                        # print(
                                        #    "RETURNING:",
                                        #    time_spans,
                                        #    category,
                                        #    tg["value"],
                                        # )
                                        return tg["value"]
                            else:
                                # print("Var:", v)
                                # print("RETURNING:", time_spans, category, tg["value"])
                                return tg["value"]

                """
                WranglerLogger.debug(
                    "\nCouldn't find time period for {}, returning default".format(
                        str(time_spans)
                    )
                )
                """
                if "default" in v.keys():
                    # print("Var:", v)
                    # print("RETURNING:", time_spans, v["default"])
                    return v["default"]
                else:
                    # print("Var:", v)
                    WranglerLogger.error(
                        "\nCan't find default; must specify a category in {}".format(
                            str(categories)
                        )
                    )
                    raise ValueError(
                        "Can't find default, must specify a category in: {}".format(
                            str(categories)
                        )
                    )

        time_spans = parse_time_spans_to_secs(time_period)

        return self.links_df[property].apply(
            _get_property, time_spans=time_spans, category=category
        )

    def _nodes_from_link(
        self, links_df: gpd.GeoDataFrame, link_pos: int, node_key_field: str
    ) -> gpd.GeoDataFrame:
        """Creates a basic list of node entries from links, their geometry, and a position.

        TODO: Does not currently fill in additional values used in nodes.

        Args:
            links_df (gpd.GeoDataFrame): subset of self.links_df or similar which needs nodes created
            link_pos (int): Position within geometry collection to use for geometry
            node_key_field (str): field name to use for generating index and node key

        Returns:
            gpd.GeoDataFrame: _description_
        """

        nodes_df = copy.deepcopy(
            links_df[[node_key_field, "geometry"]].drop_duplicates()
        )
        # WranglerLogger.debug(f"ct1: nodes_df:\n{nodes_df}")
        nodes_df = nodes_df.rename(
            columns={node_key_field: nodes_df.params.primary_key}
        )
        # WranglerLogger.debug(f"ct2: nodes_df:\n{nodes_df}")
        nodes_df["geometry"] = nodes_df["geometry"].apply(
            get_point_geometry_from_linestring, pos=link_pos
        )
        nodes_df["X"] = nodes_df.geometry.x
        nodes_df["Y"] = nodes_df.geometry.y
        nodes_df[nodes_df.params.primary_key + "_idx"] = nodes_df[
            nodes_df.params.primary_key
        ]
        nodes_df.set_index(nodes_df.params.primary_key + "_idx", inplace=True)
        # WranglerLogger.debug(f"ct3: nodes_df:\n{nodes_df}")
        return nodes_df

    def is_connected(self, mode: str) -> bool:
        """
        Determines if the network graph is "strongly" connected.

        A graph is strongly connected if each vertex is reachable from every other vertex.

        Args:
            mode:  mode of the network, one of `drive`,`transit`,`walk`, `bike`

        Returns: boolean
        """
        is_connected = nx.is_strongly_connected(self.graph[mode])

        return is_connected

    @staticmethod
    def add_incident_link_data_to_nodes(
        links_df: DataFrame = None,
        nodes_df: DataFrame = None,
        link_variables: list = [],
    ) -> DataFrame:
        """
        Add data from links going to/from nodes to node.

        Args:
            links_df: if specified, will assess connectivity of this
                links list rather than self.links_df
            nodes_df: if specified, will assess connectivity of this
                nodes list rather than self.nodes_df
            link_variables: list of columns in links dataframe to add to incident nodes

        Returns:
            nodes DataFrame with link data where length is N*number of links going in/out
        """
        WranglerLogger.debug("Adding following link data to nodes: ".format())

        _link_vals_to_nodes = [x for x in link_variables if x in links_df.columns]
        if link_variables not in _link_vals_to_nodes:
            WranglerLogger.warning(
                "Following columns not in links_df and wont be added to nodes: {} ".format(
                    list(set(link_variables) - set(_link_vals_to_nodes))
                )
            )

        _nodes_from_links_A = nodes_df.merge(
            links_df[["A"] + _link_vals_to_nodes],
            how="outer",
            left_on=nodes_df.params.primary_key,
            right_on="A",
        )
        _nodes_from_links_B = nodes_df.merge(
            links_df[["B"] + _link_vals_to_nodes],
            how="outer",
            left_on=nodes_df.params.primary_key,
            right_on="B",
        )
        _nodes_from_links_ab = pd.concat([_nodes_from_links_A, _nodes_from_links_B])

        return _nodes_from_links_ab

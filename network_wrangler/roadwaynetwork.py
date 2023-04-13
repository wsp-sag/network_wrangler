#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
import copy
from random import randint
from typing import Any, Collection, List, Optional, Union, Tuple

import folium
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
from .roadway.graph import net_to_graph
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


class NoPathFound(Exception):
    """Raised when can't find path."""

    pass


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
        nodes_df (GeoDataFrame): node data

        links_df (GeoDataFrame): link data, including start and end
            nodes and associated shape

        shapes_df (GeoDataFrame): detailed shape data

        selections (dict): dictionary storing selections in case they are made repeatedly

        BOOL_PROPERTIES (list): list of properties which should be coerced to booleans

        STR_PROPERTIES (list): list of properties which should be coerced to strings

        INT_PROPERTIES (list): list of properties which should be coerced to integers

        CRS (str): coordinate reference system in PROJ4 format.
            See https://proj.org/operations/projections/index.html#

        ESPG (int): integer representing coordinate system https://epsg.io/

        NODE_FOREIGN_KEY_TO_LINK (str): column in `nodes_df` associated with the
            LINK_FOREIGN_KEY

        LINK_FOREIGN_KEY_TO_NODE (list(str)): list of columns in `links_df` that
            represent the NODE_FOREIGN_KEY

        UNIQUE_LINK_KEY (str): column that is a unique key for links

        UNIQUE_NODE_KEY (str): column that is a unique key for nodes

        UNIQUE_SHAPE_KEY (str): column that is a unique shape key

        UNIQUE_MODEL_LINK_IDENTIFIERS (list(str)): list of all unique
            identifiers for links, including the UNIQUE_LINK_KEY

        EXPLICIT_LINK_IDENTIFIERS (list(str)): list of identifiers which are explicit enough
            to use in project selection by themselves. Includes UNIQUE_MODEL_LINK_IDENFIERS as
            well as some identifiers which may be split across model links such as osm_link_id.

        UNIQUE_NODE_IDENTIFIERS (list(str)): list of all unique identifiers
            for nodes, including the UNIQUE_NODE_KEY

        SEARCH_BREADTH (int): initial number of links from name-based
            selection that are traveresed before trying another shortest
            path when searching for paths between A and B node

        MAX_SEARCH_BREADTH (int): maximum number of links traversed between
            links that match the searched name when searching for paths
            between A and B node

        SP_WEIGHT_FACTOR (Union(int, float)): penalty assigned for each
            degree of distance between a link and a link with the searched-for
            name when searching for paths between A and B node

        MANAGED_LANES_TO_NODE_ID_SCALAR (int): scalar value added to
            the general purpose lanes' `model_node_id` when creating
            an associated node for a parallel managed lane

        MANAGED_LANES_TO_LINK_ID_SCALAR (int): scalar value added to
            the general purpose lanes' `model_link_id` when creating
            an associated link for a parallel managed lane

        MANAGED_LANES_REQUIRED_ATTRIBUTES (list(str)): list of attributes
            that must be provided in managed lanes

        KEEP_SAME_ATTRIBUTES_ML_AND_GP (list(str)): list of attributes
            to copy from a general purpose lane to managed lane
    """

    # CRS = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    CRS = 4326  # "EPSG:4326"

    NODE_FOREIGN_KEY_TO_LINK = "model_node_id"
    LINK_FOREIGN_KEY_TO_NODE = ["A", "B"]
    LINK_FOREIGN_KEY_TO_SHAPE = "shape_id"

    SEARCH_BREADTH = 5
    MAX_SEARCH_BREADTH = 10
    SP_WEIGHT_FACTOR = 100

    UNIQUE_LINK_KEY = "model_link_id"
    UNIQUE_NODE_KEY = "model_node_id"
    UNIQUE_SHAPE_KEY = "shape_id"

    UNIQUE_MODEL_LINK_IDENTIFIERS = ["model_link_id"]
    EXPLICIT_LINK_IDENTIFIERS = UNIQUE_MODEL_LINK_IDENTIFIERS + ["osm_link_id"]
    UNIQUE_NODE_IDENTIFIERS = ["model_node_id", "osm_node_id"]

    BOOL_PROPERTIES = [
        "rail_only",
        "bus_only",
        "drive_access",
        "bike_access",
        "walk_access",
        "truck_access",
    ]

    STR_PROPERTIES = [
        "osm_link_id",
        "osm_node_id",
        "shape_id",
    ]

    INT_PROPERTIES = ["model_link_id", "model_node_id", "lanes", "ML_lanes", "A", "B"]

    GEOMETRY_PROPERTIES = ["X", "Y"]

    MIN_LINK_REQUIRED_PROPS_DEFAULT = [
        "name",
        "A",
        "B",
        "roadway",
        "model_link_id",
        "lanes",
        "bus_only",
        "rail_only",
        "drive_access",
        "walk_access",
        "bike_access",
        "locationReferences",
        "geometry",
    ]

    MIN_NODE_REQUIRED_PROPS_DEFAULT = [
        "model_node_id",
        "transit_node",
        "drive_node",
        "walk_node",
        "bike_node",
        "geometry",
    ]

    MANAGED_LANES_REQUIRED_ATTRIBUTES = [
        "A",
        "B",
        "model_link_id",
        "locationReferences",
    ]

    KEEP_SAME_ATTRIBUTES_ML_AND_GP = [
        "distance",
        "bike_access",
        "drive_access",
        "transit_access",
        "walk_access",
        "maxspeed",
        "name",
        "oneway",
        "ref",
        "roadway",
        "length",
        "segment_id",
    ]

    MANAGED_LANES_SCALAR = 500000

    def __init__(self, nodes: GeoDataFrame, links: GeoDataFrame, shapes: GeoDataFrame):
        """
        Constructor
        """

        if not RoadwayNetwork.validate_object_types(nodes, links, shapes):
            sys.exit("RoadwayNetwork: Invalid constructor data type")

        self.nodes_df = nodes
        self.links_df = links
        self.shapes_df = shapes

        # Model network
        self.m_nodes_df = None
        self.m_links_df = None
        self.m_shapes_df = None

        self.link_file = None
        self.node_file = None
        self.shape_file = None

        # cached selections
        self._selection = {}

        # cached modal graphs of full network
        self._graphs = {}

        # Add non-required fields if they aren't there.
        # for field, default_value in RoadwayNetwork.OPTIONAL_FIELDS:
        #    if field not in self.links_df.columns:
        #        self.links_df[field] = default_value
        if not self.validate_uniqueness():
            raise ValueError("IDs in network not unique")

    @staticmethod
    def read(
        link_file: str, node_file: str, shape_file: str, fast: bool = True
    ) -> RoadwayNetwork:
        """
        Reads a network from the roadway network standard
        Validates that it conforms to the schema

        args:
            link_file: full path to the link file
            node_file: full path to the node file
            shape_file: full path to the shape file
            fast: boolean that will skip validation to speed up read time

        Returns: a RoadwayNetwork instance

        .. todo:: Turn off fast=True as default
        """

        for fn in (link_file, node_file, shape_file):
            if not os.path.exists(fn):
                msg = f"Specified file doesn't exist at: {fn}"
                WranglerLogger.error(msg)
                raise ValueError(msg)

        if not fast:
            if not (
                RoadwayNetwork.validate_node_schema(node_file)
                and RoadwayNetwork.validate_link_schema(link_file)
                and RoadwayNetwork.validate_shape_schema(shape_file)
            ):
                sys.exit("RoadwayNetwork: Data doesn't conform to schema")

        links_df = RoadwayNetwork.read_links(link_file)
        nodes_df = RoadwayNetwork.read_nodes(node_file)
        shapes_df = RoadwayNetwork.read_shapes(shape_file)

        roadway_network = RoadwayNetwork(
            nodes=nodes_df, links=links_df, shapes=shapes_df
        )

        roadway_network.link_file = link_file
        roadway_network.node_file = node_file
        roadway_network.shape_file = shape_file

        return roadway_network

    @classmethod
    def coerce_types(
        cls, df: pd.DataFrame, cols: Collection[str] = None
    ) -> pd.DataFrame:
        """Coerces types to bool, str and int which might default to other types based on values.

        Uses BOOL_PROPERTIES, INT_PROPERTIES and STR_PROPERTIES.

        Args:
            df: Dataframe to coerce type of
            cols: optional list of fields to check and coerce. Defaults to all fields.

        Returns:
            pd.DataFrame: Dataframe with types coerced
        """
        if cols is None:
            cols = df.columns

        for c in list(set(cls.BOOL_PROPERTIES) & set(cols)):
            df[c] = df[c].astype(bool)

        for c in list(set(cls.STR_PROPERTIES) & set(cols)):
            df[c] = df[c].astype(str)

        for c in list(set(cls.INT_PROPERTIES) & set(cols)):
            df[c] = df[c].astype(int)

        return df

    @classmethod
    def read_links(cls, filename: str) -> gpd.GeoDataFrame:
        """Reads links and returns a geodataframe of links.

        Args:
            filename (str): file to read links in from.
        """
        WranglerLogger.info(f"Reading links from {filename}.")
        with open(filename) as f:
            link_json = json.load(f)
        WranglerLogger.debug("Read link file.")
        link_properties = pd.DataFrame(link_json)
        link_geometries = [
            line_string_from_location_references(g["locationReferences"])
            for g in link_json
        ]
        links_df = gpd.GeoDataFrame(link_properties, geometry=link_geometries)
        links_df.crs = RoadwayNetwork.CRS
        links_df.gdf_name = "network_links"

        links_df = RoadwayNetwork.coerce_types(links_df)

        links_df[RoadwayNetwork.UNIQUE_LINK_KEY + "_idx"] = links_df[
            RoadwayNetwork.UNIQUE_LINK_KEY
        ]
        links_df.set_index(RoadwayNetwork.UNIQUE_LINK_KEY + "_idx", inplace=True)

        WranglerLogger.info(f"Read {len(links_df)} links.")
        return links_df

    @classmethod
    def read_nodes(cls, filename: str) -> gpd.GeoDataFrame:
        """Reads nodes and returns a geodataframe of nodes.

        Args:
            filename (str): file to read nodes in from.
        """
        # geopandas uses fiona OGR drivers, which doesn't let you have
        # a list as a property type. Therefore, must read in node_properties
        # separately in a vanilla dataframe and then convert to geopandas
        WranglerLogger.info(f"Reading nodes from {filename}.")
        with open(filename) as f:
            node_geojson = json.load(f)
        WranglerLogger.debug("Read nodes file.")
        node_properties = pd.DataFrame(
            [g["properties"] for g in node_geojson["features"]]
        )
        node_geometries = [
            Point(g["geometry"]["coordinates"]) for g in node_geojson["features"]
        ]

        nodes_df = gpd.GeoDataFrame(node_properties, geometry=node_geometries)

        nodes_df.gdf_name = "network_nodes"

        # set a copy of the  foreign key to be the index so that the
        # variable itself remains queryiable
        ## TODO this should be more elegant
        nodes_df[RoadwayNetwork.UNIQUE_NODE_KEY + "_idx"] = nodes_df[
            RoadwayNetwork.UNIQUE_NODE_KEY
        ]
        nodes_df.set_index(cls.UNIQUE_NODE_KEY + "_idx", inplace=True)

        nodes_df.crs = RoadwayNetwork.CRS
        nodes_df["X"] = nodes_df["geometry"].apply(lambda g: g.x)
        nodes_df["Y"] = nodes_df["geometry"].apply(lambda g: g.y)

        nodes_df = RoadwayNetwork.coerce_types(nodes_df)
        WranglerLogger.info(f"Read {len(nodes_df)} nodes.")
        return nodes_df

    @classmethod
    def read_shapes(cls, filename: str) -> gpd.GeoDataFrame:
        """Reads shapes and returns a geodataframe of shapes.

        Also:
        - drops records without geometry or id
        - sets CRS to RoadwayNetwork.CRS

        Args:
            filename (str): file to read shapes in from.
        """
        WranglerLogger.info(f"Reading shapes from {filename}.")
        shapes_df = gpd.read_file(filename)
        shapes_df.gdf_name = "network_shapes"
        WranglerLogger.debug("Read shapes file.")
        shapes_df.dropna(subset=["geometry", "id"], inplace=True)
        shapes_df.crs = cls.CRS
        WranglerLogger.info(f"Read {len(shapes_df)} shapes.")
        return shapes_df

    def write(
        self,
        path: str = ".",
        filename: str = None,
        model: bool = False,
    ) -> None:
        """
        Writes a network in the roadway network standard

        args:
            path: the path were the output will be saved
            filename: the name prefix of the roadway files that will be generated
            model: determines if shoudl write model network with separated managed lanes,
                or standard wrangler network. Defaults to False.
        """

        if not os.path.exists(path):
            WranglerLogger.debug("\nPath [%s] doesn't exist; creating." % path)
            os.mkdir(path)

        if filename:
            links_file = os.path.join(path, filename + "_" + "link.json")
            nodes_file = os.path.join(path, filename + "_" + "node.geojson")
            shapes_file = os.path.join(path, filename + "_" + "shape.geojson")
        else:
            links_file = os.path.join(path, "link.json")
            nodes_file = os.path.join(path, "node.geojson")
            shapes_file = os.path.join(path, "shape.geojson")

        if model:
            from .roadway import create_managed_lane_network

            net = create_managed_lane_network(self)
            links_df = net.m_links_df
            nodes_df = net.m_nodes_df
            shapes_df = net.m_shapes_df
        else:
            links_df = self.links_df
            nodes_df = self.nodes_df
            shapes_df = self.shapes_df

        # Make sure types are correct
        nodes_df = RoadwayNetwork.coerce_types(nodes_df)
        links_df = RoadwayNetwork.coerce_types(links_df)

        link_property_columns = links_df.columns.values.tolist()
        link_property_columns.remove("geometry")
        links_json = links_df_to_json(links_df, link_property_columns)
        with open(links_file, "w") as f:
            json.dump(links_json, f)

        # geopandas wont let you write to geojson because
        # it uses fiona, which doesn't accept a list as one of the properties
        # so need to convert the df to geojson manually first
        property_columns = nodes_df.columns.values.tolist()
        property_columns.remove("geometry")

        nodes_geojson = point_df_to_geojson(nodes_df, property_columns)

        with open(nodes_file, "w") as f:
            json.dump(nodes_geojson, f)

        shapes_df.to_file(shapes_file, driver="GeoJSON")

    @staticmethod
    def roadway_net_to_gdf(roadway_net: RoadwayNetwork) -> gpd.GeoDataFrame:
        """
        Turn the roadway network into a GeoDataFrame
        args:
            roadway_net: the roadway network to export

        returns: shapes dataframe

        .. todo:: Make this much more sophisticated, for example attach link info to shapes
        """
        return roadway_net.shapes_df

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

    @property
    def graph(self):
        def __get__(self, mode):
            if mode not in MODES_TO_NETWORK_LINK_VARIABLES.keys():
                raise KeyError(
                    f"Mode {mode} is not a valid network selection mode. Valid modes: {MODES_TO_NETWORK_LINK_VARIABLES.keys()}"
                )

            if not mode in self._graph:
                self._graph[mode] = net_to_graph(self, [mode])
            return self._graph[mode]

    def validate_uniqueness(self) -> bool:
        """
        Confirms that the unique identifiers are met.
        """
        valid = True

        for c in RoadwayNetwork.UNIQUE_MODEL_LINK_IDENTIFIERS:
            if c not in self.links_df.columns:
                valid = False
                msg = f"Network doesn't contain unique link identifier: {c}"
                WranglerLogger.error(msg)
            if not self.links_df[c].is_unique:
                valid = False
                msg = f"Unique identifier {c} is not unique in network links"
                WranglerLogger.error(msg)
        for c in RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE:
            if c not in self.links_df.columns:
                valid = False
                msg = f"Network doesn't contain link foreign key identifier: {c}"
                WranglerLogger.error(msg)
        link_foreign_key = self.links_df[RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE].apply(
            lambda x: "-".join(x.map(str)), axis=1
        )
        if not link_foreign_key.is_unique:
            valid = False
            msg = f"Foreign key: {RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE} is not unique in network links"

            WranglerLogger.error(msg)
        for c in RoadwayNetwork.UNIQUE_NODE_IDENTIFIERS:
            if c not in self.nodes_df.columns:
                valid = False
                msg = f"Network doesn't contain unique node identifier: {c}"
                WranglerLogger.error(msg)
            if not self.nodes_df[c].is_unique:
                valid = False
                msg = f"Unique identifier {c} is not unique in network nodes"
                WranglerLogger.error(msg)
        if RoadwayNetwork.NODE_FOREIGN_KEY_TO_LINK not in self.nodes_df.columns:
            valid = False
            msg = f"Network doesn't contain node foreign key identifier: {RoadwayNetwork.NODE_FOREIGN_KEY_TO_LINK}"
            WranglerLogger.error(msg)
        elif not self.nodes_df[RoadwayNetwork.NODE_FOREIGN_KEY_TO_LINK].is_unique:
            valid = False
            msg = f"Foreign key: {RoadwayNetwork.NODE_FOREIGN_KEY_TO_LINK} is not unique in network nodes"
            WranglerLogger.error(msg)
        if RoadwayNetwork.UNIQUE_SHAPE_KEY not in self.shapes_df.columns:
            valid = False
            msg = "Network doesn't contain unique shape id: {}".format(
                RoadwayNetwork.UNIQUE_SHAPE_KEY
            )
            WranglerLogger.error(msg)
        elif not self.shapes_df[RoadwayNetwork.UNIQUE_SHAPE_KEY].is_unique:
            valid = False
            msg = "Unique key: {} is not unique in network shapes".format(
                RoadwayNetwork.UNIQUE_SHAPE_KEY
            )
            WranglerLogger.error(msg)
        return valid

    @staticmethod
    def validate_object_types(
        nodes: GeoDataFrame, links: GeoDataFrame, shapes: GeoDataFrame
    ):
        """
        Determines if the roadway network is being built with the right object types.
        Does not validate schemas.

        Args:
            nodes: nodes geodataframe
            links: link geodataframe
            shapes: shape geodataframe

        Returns: boolean
        """

        errors = ""

        if not isinstance(nodes, GeoDataFrame):
            error_message = (
                "Incompatible nodes type:{}. Must provide a GeoDataFrame.  ".format(
                    type(nodes)
                )
            )
            WranglerLogger.error(error_message)
            errors.append(error_message)
        if not isinstance(links, GeoDataFrame):
            error_message = (
                "Incompatible links type:{}. Must provide a GeoDataFrame.  ".format(
                    type(links)
                )
            )
            WranglerLogger.error(error_message)
            errors.append(error_message)
        if not isinstance(shapes, GeoDataFrame):
            error_message = (
                "Incompatible shapes type:{}. Must provide a GeoDataFrame.  ".format(
                    type(shapes)
                )
            )
            WranglerLogger.error(error_message)
            errors.append(error_message)

        if errors:
            return False
        return True

    @staticmethod
    def validate_node_schema(
        node_file, schema_location: str = "roadway_network_node.json"
    ):
        """
        Validate roadway network data node schema and output a boolean
        """
        if not os.path.exists(schema_location):
            base_path = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "schemas"
            )
            schema_location = os.path.join(base_path, schema_location)

        with open(schema_location) as schema_json_file:
            schema = json.load(schema_json_file)

        with open(node_file) as node_json_file:
            json_data = json.load(node_json_file)

        try:
            validate(json_data, schema)
            return True

        except ValidationError as exc:
            WranglerLogger.error("Failed Node schema validation: Validation Error")
            WranglerLogger.error("Node File Loc:{}".format(node_file))
            WranglerLogger.error("Node Schema Loc:{}".format(schema_location))
            WranglerLogger.error(exc.message)

        except SchemaError as exc:
            WranglerLogger.error("Invalid Node Schema")
            WranglerLogger.error("Node Schema Loc:{}".format(schema_location))
            WranglerLogger.error(json.dumps(exc.message, indent=2))

        return False

    @staticmethod
    def validate_link_schema(
        link_file, schema_location: str = "roadway_network_link.json"
    ):
        """
        Validate roadway network data link schema and output a boolean
        """

        if not os.path.exists(schema_location):
            base_path = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "schemas"
            )
            schema_location = os.path.join(base_path, schema_location)

        with open(schema_location) as schema_json_file:
            schema = json.load(schema_json_file)

        with open(link_file) as link_json_file:
            json_data = json.load(link_json_file)

        try:
            validate(json_data, schema)
            return True

        except ValidationError as exc:
            WranglerLogger.error("Failed Link schema validation: Validation Error")
            WranglerLogger.error("Link File Loc:{}".format(link_file))
            WranglerLogger.error("Path:{}".format(exc.path))
            WranglerLogger.error(exc.message)

        except SchemaError as exc:
            WranglerLogger.error("Invalid Link Schema")
            WranglerLogger.error("Link Schema Loc: {}".format(schema_location))
            WranglerLogger.error(json.dumps(exc.message, indent=2))

        return False

    @staticmethod
    def validate_shape_schema(
        shape_file, schema_location: str = "roadway_network_shape.json"
    ):
        """
        Validate roadway network data shape schema and output a boolean
        """

        if not os.path.exists(schema_location):
            base_path = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "schemas"
            )
            schema_location = os.path.join(base_path, schema_location)

        with open(schema_location) as schema_json_file:
            schema = json.load(schema_json_file)

        with open(shape_file) as shape_json_file:
            json_data = json.load(shape_json_file)

        try:
            validate(json_data, schema)
            return True

        except ValidationError as exc:
            WranglerLogger.error("Failed Shape schema validation: Validation Error")
            WranglerLogger.error("Shape File Loc:{}".format(shape_file))
            WranglerLogger.error("Path:{}".format(exc.path))
            WranglerLogger.error(exc.message)

        except SchemaError as exc:
            WranglerLogger.error("Invalid Shape Schema")
            WranglerLogger.error("Shape Schema Loc: {}".format(schema_location))
            WranglerLogger.error(json.dumps(exc.message, indent=2))

        return False

    @property
    def num_managed_lane_links(self):
        if "managed" in self.links_df.columns:
            return len((self.links_df[self.links_df["managed"] == 1]).index)
        else:
            return 0

    def _validate_link_selection(self, selection: dict) -> bool:
        """Validates that link selection is complete/valid for given network.

        Checks:
        1. selection properties for links, a, and b are in links_df
        2. either a unique ID or name + A & B are specified

        If selection for links is "all" it is assumed valid.

        Args:
            selection (dict): selection dictionary

        Returns:
            bool: True if link selection is valid and complete.
        """

        if selection.get("links") == "all":
            return True

        valid = True

        _link_selection_props = [p for x in selection["links"] for p in x.keys()]

        _missing_link_props = set(_link_selection_props) - set(self.links_df.columns)

        if _missing_link_props:
            WranglerLogger.error(
                f"Link selection contains properties not found in the link dataframe:\n\
                {','.join(_missing_link_props)}"
            )
            valid = False

        _link_explicit_link_id = bool(
            set(RoadwayNetwork.EXPLICIT_LINK_IDENTIFIERS).intersection(
                set(_link_selection_props)
            )
        )
        # if don't have an explicit link id, then require A and B nodes
        _has_alternate_link_id = all(
            [
                selection.get("A"),
                selection.get("B"),
                any([x.get("name") for x in selection["links"]]),
            ]
        )

        if not _link_explicit_link_id and not _has_alternate_link_id:
            WranglerLogger.error(
                "Link selection does not contain unique link ID or alternate A and B nodes + 'name'."
            )
            valid = False

        _node_selection_props = list(
            set(
                list(selection.get("A", {}).keys())
                + list(selection.get("B", {}).keys())
            )
        )
        _missing_node_props = set(_node_selection_props) - set(self.nodes_df.columns)

        if _missing_node_props:
            WranglerLogger.error(
                f"Node selection contains properties not found in the node dataframe:\n\
                {','.join(_missing_node_props)}"
            )
            valid = False

        if not valid:
            raise ValueError("Link Selection is not valid for network.")
        return True

    def _validate_node_selection(self, selection: dict) -> bool:
        """Validates that node selection is complete/valid for given network.

        Checks:
        1. selection properties for nodes are in nodes_df
        2. Nodes identified by an explicit or implicit unique ID. A warning is given for using
            a property as an identifier which isn't explicitly unique.

        If selection for nodes is "all" it is assumed valid.

        Args:
            selection (dict): Project Card selection dictionary

        Returns:
            bool:True if node selection is valid and complete.
        """
        valid = True

        if selection.get("nodse") == "all":
            return True

        _node_selection_props = [p for x in selection["nodes"] for p in x.keys()]

        _missing_node_props = set(_node_selection_props) - set(self.nodes_df.columns)

        if _missing_node_props:
            WranglerLogger.error(
                f"Node selection contains properties not found in the node dataframe:\n\
                {','.join(_missing_node_props)}"
            )
            valid = False

        _has_explicit_unique_node_id = bool(
            set(RoadwayNetwork.UNIQUE_NODE_IDENTIFIERS).intersection(
                set(_node_selection_props)
            )
        )

        if not _has_explicit_unique_node_id:
            if self.nodes_df[_node_selection_props].get_value_counts().min() == 1:
                WranglerLogger.warning(
                    f"Link selection does not contain an explicit unique link ID: \
                        {RoadwayNetwork.UNIQUE_NODE_IDENTIFIERS}, \
                        but has properties which sufficiently select a single node. \
                        This selection may not work on other networks."
                )
            else:
                WranglerLogger.error(
                    "Link selection does not contain unique link ID or alternate A and B nodes + 'name'."
                )
                valid = False
        if not valid:
            raise ValueError("Node Selection is not valid for network.")
        return True

    def validate_selection(self, selection: dict) -> bool:
        """
        Evaluate whetther the selection dictionary contains the
        minimum required values.

        Args:
            selection: selection dictionary to be evaluated

        Returns: boolean value as to whether the selection dictonary is valid.
        """
        if selection.get("links"):
            return self._validate_link_selection(selection)

        elif selection.get("nodes"):
            return self._validate_node_selection(selection)

        else:
            raise ValueError(
                f"Project Card Selection requires either 'links' or 'nodes' : \
                Selection provided: {selection.keys()}"
            )

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
            unique_ids=RoadwayNetwork.UNIQUE_NODE_IDENTIFIERS,
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
        selection: dict,
        sel_key: str,
        force_search: bool = False,
        search_mode="drive",
    ) -> list:
        """_summary_
        #TODO
        Args:
            selection (dict): _description_
            sel_key: selection key hash to store selection in
            force_search (bool, optional): _description_. Defaults to False.
            search_mode (str, optional): _description_. Defaults to "drive".

        Returns:
            List of indices for selected links in self.links_df
        """
        WranglerLogger.debug("Selecting links.")
        if selection.get("links") == "all":
            return self.links_df.index.tolist()

        # if this selection has been found before, return the previously selected links
        if (
            self.selections.get(sel_key, {}).get("selection_found", None)
            and not force_search
        ):
            return self.selections[sel_key]["selected_links"].index.tolist()

        unique_model_link_identifer_in_selection = (
            RoadwayNetwork.selection_has_unique_link_id(selection)
        )
        if not unique_model_link_identifer_in_selection:
            A_id, B_id = self.orig_dest_nodes_foreign_key(selection)
        # identify candidate links which match the initial query
        # assign them as iteration = 0
        # subsequent iterations that didn't match the query will be
        # assigned a heigher weight in the shortest path
        WranglerLogger.debug("Building selection query")
        # build a selection query based on the selection dictionary

        sel_query = ProjectCard.build_selection_query(
            selection=selection,
            type="links",
            unique_ids=RoadwayNetwork.UNIQUE_MODEL_LINK_IDENTIFIERS,
            mode=RoadwayNetwork.MODES_TO_NETWORK_LINK_VARIABLES[search_mode],
        )
        WranglerLogger.debug("Selecting link features:\n{}".format(sel_query))

        self.selections[sel_key]["candidate_links"] = self.links_df.query(
            sel_query, engine="python"
        )
        WranglerLogger.debug("Completed query")
        candidate_links = self.selections[sel_key][
            "candidate_links"
        ]  # b/c too long to keep that way

        candidate_links["i"] = 0

        if len(candidate_links.index) == 0 and unique_model_link_identifer_in_selection:
            msg = "No links found based on unique link identifiers.\nSelection Failed."
            WranglerLogger.error(msg)
            raise Exception(msg)

        if len(candidate_links.index) == 0:
            WranglerLogger.debug(
                "No candidate links in initial search.\nRetrying query using 'ref' instead of \
                    'name'"
            )
            # if the query doesn't come back with something from 'name'
            # try it again with 'ref' instead
            selection_has_name_key = any("name" in d for d in selection["links"])

            if not selection_has_name_key:
                msg = "Not able to complete search using 'ref' instead of 'name' because 'name' \
                    not in search."
                WranglerLogger.error(msg)
                raise Exception(msg)

            if "ref" not in self.links_df.columns:
                msg = "Not able to complete search using 'ref' because 'ref' not in network."
                WranglerLogger.error(msg)
                raise Exception(msg)

            WranglerLogger.debug("Trying selection query replacing 'name' with 'ref'")
            sel_query = sel_query.replace("name", "ref")

            self.selections[sel_key]["candidate_links"] = self.links_df.query(
                sel_query, engine="python"
            )
            candidate_links = self.selections[sel_key]["candidate_links"]

            candidate_links["i"] = 0

            if len(candidate_links.index) == 0:
                msg = "No candidate links in search using either 'name' or 'ref' in query.\
                    Selection Failed."
                WranglerLogger.error(msg)
                raise Exception(msg)

        if unique_model_link_identifer_in_selection:
            # unique identifier exists and no need to go through big search
            self.selections[sel_key]["selected_links"] = self.selections[sel_key][
                "candidate_links"
            ]
            self.selections[sel_key]["selection_found"] = True

            return self.selections[sel_key]["selected_links"].index.tolist()

        else:
            WranglerLogger.debug("Not a unique ID selection, conduct search.")
            (
                self.selections[sel_key]["graph"],
                self.selections[sel_key]["candidate_links"],
                self.selections[sel_key]["route"],
                self.selections[sel_key]["links"],
            ) = self.path_search(
                self.selections[sel_key]["candidate_links"],
                A_id,
                B_id,
                weight_factor=RoadwayNetwork.SP_WEIGHT_FACTOR,
            )

            if len(selection["links"]) == 1:
                self.selections[sel_key]["selected_links"] = self.selections[sel_key][
                    "links"
                ]

            # Conduct a "selection on the selection" if have additional requirements to satisfy
            else:
                resel_query = ProjectCard.build_selection_query(
                    selection=selection,
                    unique_ids=RoadwayNetwork.UNIQUE_MODEL_LINK_IDENTIFIERS,
                    mode=RoadwayNetwork.MODES_TO_NETWORK_LINK_VARIABLES[search_mode],
                    ignore=["name"],
                )
                WranglerLogger.debug("Reselecting features:\n{}".format(resel_query))
                self.selections[sel_key]["selected_links"] = self.selections[sel_key][
                    "links"
                ].query(resel_query, engine="python")

            if len(self.selections[sel_key]["selected_links"]) > 0:
                self.selections[sel_key]["selection_found"] = True
            else:
                raise ValueError(f"No links found for selection: {selection}")

            self.selections[sel_key]["selection_found"] = True
            return self.selections[sel_key]["selected_links"].index.tolist()

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
                    self.nodes_df[RoadwayNetwork.UNIQUE_NODE_KEY].isin(updated_nodes)
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
            updated_nodes_df[[RoadwayNetwork.UNIQUE_NODE_KEY, "geometry"]]
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
            WranglerLogger.debug(
                f"Links:\n{links_df[RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE]}"
            )
        nodes_list = list(
            set(
                pd.concat(
                    [links_df[c] for c in RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE]
                ).tolist()
            )
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
            links_df.isin(
                {c: node_id_list for c in RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE}
            )
        ]
        WranglerLogger.debug(
            f"Temp Selected {len(_selected_links_df)} associated with {len(node_id_list)} nodes."
        )
        """
        _query_parts = [
            f"{prop} == {str(n)}"
            for prop in RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE
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
        _node_ids = updated_nodes_df[RoadwayNetwork.NODE_FOREIGN_KEY_TO_LINK].tolist()
        updated_links_df = copy.deepcopy(
            RoadwayNetwork.links_with_nodes(self.links_df, _node_ids)
        )

        _shape_ids = updated_links_df[RoadwayNetwork.LINK_FOREIGN_KEY_TO_SHAPE].tolist()
        updated_shapes_df = copy.deepcopy(
            self.shapes_df.loc[
                self.shapes_df[RoadwayNetwork.UNIQUE_SHAPE_KEY].isin(_shape_ids)
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
                [RoadwayNetwork.UNIQUE_LINK_KEY, "geometry", "locationReferences"]
            ]
        )
        self.shapes_df.update(
            updated_shapes_df[[RoadwayNetwork.UNIQUE_SHAPE_KEY, "geometry"]]
        )

    def _create_link_locationreferences(self, links_df: pd.DataFrame) -> pd.Series:
        locationreferences_s = links_df.apply(
            lambda x: location_reference_from_nodes(
                [
                    self.nodes_df[
                        self.nodes_df[RoadwayNetwork.NODE_FOREIGN_KEY_TO_LINK]
                        == x[RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE[0]]
                    ].squeeze(),
                    self.nodes_df[
                        self.nodes_df[RoadwayNetwork.NODE_FOREIGN_KEY_TO_LINK]
                        == x[RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE[1]]
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

        _df = updated_shapes_df[[RoadwayNetwork.UNIQUE_SHAPE_KEY, "geometry"]].merge(
            updated_links_df[[RoadwayNetwork.LINK_FOREIGN_KEY_TO_SHAPE, "geometry"]],
            left_on=RoadwayNetwork.UNIQUE_SHAPE_KEY,
            right_on=RoadwayNetwork.LINK_FOREIGN_KEY_TO_SHAPE,
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
        """Queries if network has node based on RoadwayNetwork.UNIQUE_NODE_KEY.

        Args:
            unique_node_id (_type_): value of RoadwayNetwork.UNIQUE_NODE_KEY
        """

        has_node = (
            self.nodes_df[RoadwayNetwork.UNIQUE_NODE_KEY].isin([unique_node_id]).any()
        )

        return has_node

    def has_link(self, link_key_values: tuple) -> bool:
        """Returns true if network has link based values corresponding with LINK_FOREIGN_KEY_TO_NODE properties.

        Args:
            link_key_values: Tuple of values corresponding with RoadwayNetwork.LINK_FOREIGN_KEY_TO_ ODE properties.
                If LINK_FOREIGN_KEY_TO_NODE is ("A","B"), then (1,2) references the link of A=1 and B=2.
        """
        _query_parts = [
            f"{k} == {str(v)}"
            for k, v in zip(RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE, link_key_values)
        ]
        _query = " and ".join(_query_parts)
        _links = self.links_df.query(_query, engine="python")

        return bool(len(_links))

    def _shapes_without_links(self) -> pd.Series:
        """Returns shape ids that don't have associated links."""

        _ids_in_shapes = self.links_df[RoadwayNetwork.UNIQUE_SHAPE_KEY]
        _ids_in_links = self.links_df[RoadwayNetwork.LINK_FOREIGN_KEY_TO_SHAPE]

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
            columns={node_key_field: RoadwayNetwork.UNIQUE_NODE_KEY}
        )
        # WranglerLogger.debug(f"ct2: nodes_df:\n{nodes_df}")
        nodes_df["geometry"] = nodes_df["geometry"].apply(
            get_point_geometry_from_linestring, pos=link_pos
        )
        nodes_df["X"] = nodes_df.geometry.x
        nodes_df["Y"] = nodes_df.geometry.y
        nodes_df[RoadwayNetwork.UNIQUE_NODE_KEY + "_idx"] = nodes_df[
            RoadwayNetwork.UNIQUE_NODE_KEY
        ]
        nodes_df.set_index(RoadwayNetwork.UNIQUE_NODE_KEY + "_idx", inplace=True)
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
            left_on=RoadwayNetwork.UNIQUE_NODE_KEY,
            right_on="A",
        )
        _nodes_from_links_B = nodes_df.merge(
            links_df[["B"] + _link_vals_to_nodes],
            how="outer",
            left_on=RoadwayNetwork.UNIQUE_NODE_KEY,
            right_on="B",
        )
        _nodes_from_links_ab = pd.concat([_nodes_from_links_A, _nodes_from_links_B])

        return _nodes_from_links_ab

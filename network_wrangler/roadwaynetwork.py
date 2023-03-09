#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
import copy
import hashlib
import numbers
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

from .logger import WranglerLogger
from .projectcard import ProjectCard

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

    .. highlight:: python

    Typical usage example:
    ::

        net = RoadwayNetwork.read(
            link_file=MY_LINK_FILE,
            node_file=MY_NODE_FILE,
            shape_file=MY_SHAPE_FILE,
        )
        my_selection = {
            "links": [{"name": ["I 35E"]}],
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

    MODES_TO_NETWORK_LINK_VARIABLES = {
        "drive": ["drive_access"],
        "bus": ["bus_only", "drive_access"],
        "rail": ["rail_only"],
        "transit": ["bus_only", "rail_only", "drive_access"],
        "walk": ["walk_access"],
        "bike": ["bike_access"],
    }

    MODES_TO_NETWORK_NODE_VARIABLES = {
        "drive": ["drive_node"],
        "rail": ["rail_only", "drive_node"],
        "bus": ["bus_only", "drive_node"],
        "transit": ["bus_only", "rail_only", "drive_node"],
        "walk": ["walk_node"],
        "bike": ["bike_node"],
    }

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

        # Add non-required fields if they aren't there.
        # for field, default_value in RoadwayNetwork.OPTIONAL_FIELDS:
        #    if field not in self.links_df.columns:
        #        self.links_df[field] = default_value
        if not self.validate_uniqueness():
            raise ValueError("IDs in network not unique")
        self.selections = {}

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

    def orig_dest_nodes_foreign_key(
        self, selection: dict, node_foreign_key: str = ""
    ) -> tuple:
        """
        Returns the foreign key id (whatever is used in the u and v
        variables in the links file) for the AB nodes as a tuple.

        Args:
            selection : selection dictionary with A and B keys
            node_foreign_key: variable name for whatever is used by the u and v variable
            in the links_df file.  If nothing is specified, assume whatever
            default is (usually osm_node_id)

        Returns: tuple of (A_id, B_id)
        """

        if not node_foreign_key:
            node_foreign_key = RoadwayNetwork.NODE_FOREIGN_KEY_TO_LINK
        if len(selection["A"]) > 1:
            raise ("Selection A node dictionary should be of length 1")
        if len(selection["B"]) > 1:
            raise ("Selection B node dictionary should be of length 1")

        A_node_key, A_id = next(iter(selection["A"].items()))
        B_node_key, B_id = next(iter(selection["B"].items()))

        if A_node_key != node_foreign_key:
            A_id = self.nodes_df[self.nodes_df[A_node_key] == A_id][
                node_foreign_key
            ].values[0]
        if B_node_key != node_foreign_key:
            B_id = self.nodes_df[self.nodes_df[B_node_key] == B_id][
                node_foreign_key
            ].values[0]

        return (A_id, B_id)

    @staticmethod
    def ox_graph(nodes_df: GeoDataFrame, links_df: GeoDataFrame):
        """
        create an osmnx-flavored network graph

        osmnx doesn't like values that are arrays, so remove the variables
        that have arrays.  osmnx also requires that certain variables
        be filled in, so do that too.

        Args:
            nodes_df : GeoDataFrame of nodes
            links_df : GeoDataFrame of links

        Returns: a networkx multidigraph
        """
        WranglerLogger.debug("starting ox_graph()")

        if "inboundReferenceIds" in nodes_df.columns:
            graph_nodes = nodes_df.copy().drop(
                ["inboundReferenceIds", "outboundReferenceIds"], axis=1
            )
        else:
            graph_nodes = nodes_df.copy()

        graph_nodes.gdf_name = "network_nodes"
        WranglerLogger.debug("GRAPH NODES: {}".format(graph_nodes.columns))
        graph_nodes["id"] = graph_nodes[RoadwayNetwork.UNIQUE_NODE_KEY]

        graph_nodes["x"] = graph_nodes["X"]
        graph_nodes["y"] = graph_nodes["Y"]

        if "osm_link_id" in links_df.columns:
            graph_links = links_df.copy().drop(
                ["osm_link_id", "locationReferences"], axis=1
            )
        else:
            graph_links = links_df.copy().drop(["locationReferences"], axis=1)

        # have to change this over into u,v b/c this is what osm-nx is expecting
        graph_links["u"] = graph_links[RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE[0]]
        graph_links["v"] = graph_links[RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE[1]]
        graph_links["key"] = graph_links[RoadwayNetwork.UNIQUE_LINK_KEY]

        # Per osmnx u,v,key should be a multi-index;
        #     https://osmnx.readthedocs.io/en/stable/osmnx.html#osmnx.utils_graph.graph_from_gdfs
        # However - if the index is set before hand in osmnx version <1.0 then it fails
        #     on the set_index line *within* osmnx.utils_graph.graph_from_gdfs():
        #           `for (u, v, k), row in gdf_edges.set_index(["u", "v", "key"]).iterrows():`

        if int(ox.__version__.split(".")[0]) >= 1:
            graph_links = graph_links.set_index(keys=["u", "v", "key"], drop=True)

        WranglerLogger.debug("starting ox.gdfs_to_graph()")
        try:
            G = ox.graph_from_gdfs(graph_nodes, graph_links)

        except AttributeError as attr_error:
            if (
                attr_error.args[0]
                == "module 'osmnx' has no attribute 'graph_from_gdfs'"
            ):
                # This is the only exception for which we have a workaround
                # Does this still work given the u,v,key multi-indexing?
                #
                WranglerLogger.warn(
                    "Please upgrade your OSMNX package. For now, using deprecated\
                         osmnx.gdfs_to_graph because osmnx.graph_from_gdfs not found"
                )
                G = ox.gdfs_to_graph(graph_nodes, graph_links)
            else:
                # for other AttributeErrors, raise further
                raise attr_error
        except Exception as e:
            # for other Exceptions, raise further
            raise e

        WranglerLogger.debug("finished ox.gdfs_to_graph()")
        return G

    @staticmethod
    def selection_has_unique_link_id(selection_dict: dict) -> bool:
        """
        Args:
            selection_dictionary: Dictionary representation of selection
                of roadway features, containing a "links" key.

        Returns: A boolean indicating if the selection dictionary contains
            a required unique link id.

        """
        selection_keys = [k for li in selection_dict["links"] for k, v in li.items()]
        return bool(
            set(RoadwayNetwork.UNIQUE_MODEL_LINK_IDENTIFIERS).intersection(
                set(selection_keys)
            )
        )

    def build_selection_key(self, selection_dict: dict) -> tuple:
        """
        Selections are stored by a hash of the selection dictionary.

        Args:
            selection_dictonary: Selection Dictionary

        Returns: Hex code for hash

        """

        return hashlib.md5(b"selection_dict").hexdigest()

    @staticmethod
    def _get_fk_nodes(_links: gpd.GeoDataFrame):
        """Find the nodes for the candidate links."""
        _n = list(
            set(
                [
                    i
                    for fk in RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE
                    for i in list(_links[fk])
                ]
            )
        )
        # WranglerLogger.debug("Node foreign key list: {}".format(_n))
        return _n

    def shortest_path(
        self,
        graph_links_df: gpd.GeoDataFrame,
        O_id,
        D_id,
        nodes_df: gpd.GeoDataFrame = None,
        weight_column: str = "i",
        weight_factor: float = 1.0,
    ) -> tuple:
        """

        Args:
            graph_links_df:
            O_id: foreign key for start node
            D_id: foreign key for end node
            nodes_df: optional nodes df, otherwise will use network instance
            weight_column: column to use as a weight, defaults to "i"
            weight_factor: any additional weighting to multiply the weight column by, defaults
                to RoadwayNetwork.SP_WEIGHT_FACTOR

        Returns: tuple with length of four
        - Boolean if shortest path found
        - nx Directed graph of graph links
        - route of shortest path nodes as List
        - links in shortest path selected from links_df
        """
        WranglerLogger.debug(
            f"Calculating shortest path from {O_id} to {D_id} using {weight_column} as \
                weight with a factor of {weight_factor}"
        )

        # Prep Graph Links
        if weight_column not in graph_links_df.columns:
            WranglerLogger.warning(
                "{} not in graph_links_df so adding and initializing to 1.".format(
                    weight_column
                )
            )
            graph_links_df[weight_column] = 1

        graph_links_df.loc[:, "weight"] = 1 + (
            graph_links_df[weight_column] * weight_factor
        )

        # Select Graph Nodes
        node_list_foreign_keys = RoadwayNetwork._get_fk_nodes(graph_links_df)

        if O_id not in node_list_foreign_keys:
            msg = "O_id: {} not in Graph for finding shortest Path".format(O_id)
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if D_id not in node_list_foreign_keys:
            msg = "D_id: {} not in Graph for finding shortest Path".format(D_id)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        if not nodes_df:
            nodes_df = self.nodes_df
        graph_nodes_df = nodes_df.loc[node_list_foreign_keys]

        # Create Graph
        WranglerLogger.debug("Creating network graph")
        G = RoadwayNetwork.ox_graph(graph_nodes_df, graph_links_df)

        try:
            sp_route = nx.shortest_path(G, O_id, D_id, weight="weight")
            WranglerLogger.debug("Shortest path successfully routed")
        except nx.NetworkXNoPath:
            WranglerLogger.debug("No SP from {} to {} Found.".format(O_id, D_id))
            return False, G, graph_links_df, None, None

        sp_links = graph_links_df[
            graph_links_df["A"].isin(sp_route) & graph_links_df["B"].isin(sp_route)
        ]

        return True, G, graph_links_df, sp_route, sp_links

    def path_search(
        self,
        candidate_links_df: gpd.GeoDataFrame,
        O_id,
        D_id,
        weight_column: str = "i",
        weight_factor: float = 1.0,
    ):
        """

        Args:
            candidate_links: selection of links geodataframe with links likely to be part of path
            O_id: origin node foreigh key ID
            D_id: destination node foreigh key ID
            weight_column: column to use for weight of shortest path. Defaults to "i" (iteration)
            weight_factor: optional weight to multiply the weight column by when finding
                the shortest path

        Returns

        """

        def _add_breadth(
            _candidate_links_df: gpd.GeoDataFrame,
            _nodes_df: gpd.GeoDataFrame,
            _links_df: gpd.GeoDataFrame,
            i: int = None,
        ):
            """
            Add outbound and inbound reference IDs to candidate links
            from existing nodes

            Args:
                _candidate_links_df : df with the links from the previous iteration
                _nodes_df : df of all nodes in the full network
                _links_df : df of all links in the full network
                i : iteration of adding breadth

            Returns:
                candidate_links : GeoDataFrame
                    updated df with one more degree of added breadth

                node_list_foreign_keys : list of foreign key ids for nodes in the updated
                    candidate links to test if the A and B nodes are in there.
            """
            WranglerLogger.debug("-Adding Breadth-")

            if not i:
                WranglerLogger.warning("i not specified in _add_breadth, using 1")
                i = 1

            _candidate_nodes_df = _nodes_df.loc[
                RoadwayNetwork._get_fk_nodes(_candidate_links_df)
            ]
            WranglerLogger.debug("Candidate Nodes: {}".format(len(_candidate_nodes_df)))

            # Identify links to add based on outbound and inbound links from nodes
            _links_shstRefId_to_add = list(
                set(
                    sum(_candidate_nodes_df["outboundReferenceIds"].tolist(), [])
                    + sum(_candidate_nodes_df["inboundReferenceIds"].tolist(), [])
                )
                - set(_candidate_links_df["shstReferenceId"].tolist())
                - set([""])
            )
            _links_to_add_df = _links_df[
                _links_df.shstReferenceId.isin(_links_shstRefId_to_add)
            ]

            WranglerLogger.debug("Adding {} links.".format(_links_to_add_df.shape[0]))

            # Add information about what iteration the link was added in
            _links_df[_links_df.model_link_id.isin(_links_shstRefId_to_add)]["i"] = i

            # Append links and update node list
            _candidate_links_df = pd.concat([_candidate_links_df, _links_to_add_df])
            _node_list_foreign_keys = RoadwayNetwork._get_fk_nodes(_candidate_links_df)

            return _candidate_links_df, _node_list_foreign_keys

        # -----------------------------------
        # Set search breadth to zero + set max
        # -----------------------------------
        i = 0
        max_i = RoadwayNetwork.SEARCH_BREADTH
        # -----------------------------------
        # Add links to the graph until
        #   (i) the A and B nodes are in the
        #       foreign key list
        #          - OR -
        #   (ii) reach maximum search breadth
        # -----------------------------------
        node_list_foreign_keys = RoadwayNetwork._get_fk_nodes(candidate_links_df)
        WranglerLogger.debug("Initial set of nodes: {}".format(node_list_foreign_keys))
        while (
            O_id not in node_list_foreign_keys or D_id not in node_list_foreign_keys
        ) and i <= max_i:
            WranglerLogger.debug(
                "Adding breadth - i: {}, Max i: {}] - {} and {} not found in node list.".format(
                    i, max_i, O_id, D_id
                )
            )
            i += 1
            candidate_links_df, node_list_foreign_keys = _add_breadth(
                candidate_links_df, self.nodes_df, self.links_df, i=i
            )
        # -----------------------------------
        #  Once have A and B in graph,
        #  Try calculating shortest path
        # -----------------------------------
        WranglerLogger.debug("calculating shortest path from graph")
        (
            sp_found,
            graph,
            candidate_links_df,
            shortest_path_route,
            shortest_path_links,
        ) = self.shortest_path(candidate_links_df, O_id, D_id)
        if sp_found:
            return graph, candidate_links_df, shortest_path_route, shortest_path_links

        if not sp_found:
            WranglerLogger.debug(
                "No shortest path found with breadth of {i}, trying greater breadth until SP \
                    found or max breadth {max_i} reached."
            )
        while not sp_found and i <= RoadwayNetwork.MAX_SEARCH_BREADTH:
            WranglerLogger.debug(
                "Adding breadth, with shortest path iteration. i: {} Max i: {}".format(
                    i, max_i
                )
            )
            i += 1
            candidate_links_df, node_list_foreign_keys = _add_breadth(
                candidate_links_df, self.nodes_df, self.links_df, i=i
            )
            (
                sp_found,
                graph,
                candidate_links_df,
                route,
                shortest_path_links,
            ) = self.shortest_path(candidate_links_df, O_id, D_id)

        if sp_found:
            return graph, candidate_links_df, route, shortest_path_links

        if not sp_found:
            msg = "Couldn't find path from {} to {} after adding {} links in breadth".format(
                O_id, D_id, i
            )
            WranglerLogger.error(msg)
            raise NoPathFound(msg)

    def select_roadway_features(
        self, selection: dict, search_mode="drive", force_search=False
    ) -> list:
        """
        Selects roadway features that satisfy selection criteria

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
            return self.apply_roadway_feature_change(
                _df_idx,
                project_dictionary["properties"],
                geometry_type=_geometry_type,
            )
        elif _category == "parallel managed lanes":
            return self.apply_managed_lane_feature_change(
                _df_idx,
                project_dictionary["properties"],
            )
        elif _category == "add new roadway":
            return self.add_new_roadway_feature_change(
                project_dictionary.get("links", []),
                project_dictionary.get("nodes", []),
            )
        elif _category == "roadway deletion":
            return self.delete_roadway_feature_change(
                project_dictionary.get("links", []),
                project_dictionary.get("nodes", []),
            )
        elif _category == "calculated roadway":
            return self.apply_python_calculation(
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

        if len(updated_nodes_df)<25:
            WranglerLogger.debug(f"Original Nodes:\n{updated_nodes_df[['X','Y','geometry']]}")

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
        if len(updated_nodes_df)<25:
            WranglerLogger.debug(f"Updated Nodes:\n{updated_nodes_df[['X','Y','geometry']]}")
        self.nodes_df.update(
            updated_nodes_df[[RoadwayNetwork.UNIQUE_NODE_KEY, "geometry"]]
        )
        WranglerLogger.debug(f"{len(self.nodes_df)} nodes in network after update")
        if len(self.nodes_df)<25:
            WranglerLogger.debug(f"Updated self.nodes_df:\n{self.nodes_df[['X','Y','geometry']]}")

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
            WranglerLogger.debug(f"Links:\n{links_df[RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE]}")
        nodes_list = list(set(
            pd.concat([links_df[c] for c in RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE]).tolist()
        ))
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
        #If nodes are equal to all the nodes in the links, return all the links
        _nodes_in_links = RoadwayNetwork.nodes_in_links(links_df)
        WranglerLogger.debug(f"# Nodes: {len(node_id_list)}\nNodes in links:{len(_nodes_in_links)}")
        if len( set(node_id_list) - set(_nodes_in_links) ) == 0:
                return links_df

        WranglerLogger.debug(f"Finding links assocated with {len(node_id_list)} nodes.")
        if len(node_id_list) < 25:
            WranglerLogger.debug(f"node_id_list: {node_id_list}")

        _selected_links_df = links_df[
            links_df.isin({c:node_id_list for c in RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE})
        ]
        WranglerLogger.debug(f"Temp Selected {len(_selected_links_df)} associated with {len(node_id_list)} nodes.")
        """
        _query_parts = [
            f"{prop} == {str(n)}"
            for prop in RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE
            for n in node_id_list
        ]
        
        _query = " or ".join(_query_parts)
        _selected_links_df = links_df.query(_query, engine="python")
        """
        WranglerLogger.debug(f"Selected {len(_selected_links_df)} associated with {len(node_id_list)} nodes.")

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

    def _add_link_geometry_from_nodes(self, links_df: pd.DataFrame) -> gpd.GeoDataFrame:
        links_df["locationReferences"] = self._create_link_locationreferences(links_df)
        links_df["geometry"] = links_df["locationReferences"].apply(
            line_string_from_location_references,
        )
        links_df = gpd.GeoDataFrame(links_df)
        return links_df

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

    def _create_new_link_geometry(self, new_links_df: pd.DataFrame) -> gpd.GeoDataFrame:
        new_links_df = self._add_link_geometry_from_nodes(new_links_df)
        new_links_df[RoadwayNetwork.UNIQUE_SHAPE_KEY] = new_links_df["geometry"].apply(
            create_unique_shape_id
        )
        return new_links_df

    @staticmethod
    def _create_new_shapes_from_links(links_df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        new_shapes_df = copy.deepcopy(
            links_df[[RoadwayNetwork.UNIQUE_SHAPE_KEY, "geometry"]]
        )
        return new_shapes_df

    def apply_python_calculation(self, pycode: str) -> "RoadwayNetwork":
        """
        Changes roadway network object by executing pycode.

        Args:
            net: network to manipulate
            pycode: python code which changes values in the roadway network object
        """
        exec(pycode)
        return self

    def _add_property(self, df: pd.DataFrame, property_dict: dict) -> pd.DataFrame:
        """
        Adds a property to a dataframe. Infers type from the property_dict "set" value.

        Args:
            df: dataframe to add property to
            property_dict: dictionary of property to add with "set" value.

        Returns:
            pd.DataFrame: dataframe with property added filled with NaN.
        """
        WranglerLogger.info(f"Adding property: {property_dict['property']}")
        df[property_dict["property"]] = np.nan
        return df

    def _update_property(self, existing_facilities_df: pd.DataFrame, property: dict):
        """_summary_

        Args:
            existing_facilities_df: selected existing facility df
            property (dict): project property update
        """
        # WranglerLogger.debug(f"property:\n{property}")
        # WranglerLogger.debug(f"existing_facilities_df:\n{existing_facilities_df}")
        if "existing" in property:
            if (
                not existing_facilities_df[property["property"]]
                .eq(property["existing"])
                .all()
            ):
                WranglerLogger.warning(
                    "Existing value defined for {} in project card does "
                    "not match the value in the roadway network for the "
                    "selected links".format(property["property"])
                )

        if "set" in property:
            _updated_series = pd.Series(
                property["set"],
                name=property["property"],
                index=existing_facilities_df.index,
            )

        elif "change" in property:
            _updated_series = (
                existing_facilities_df[property["property"]] + property["change"]
            )
        else:
            WranglerLogger.debug(f"Property: \n {property}")
            raise ValueError(
                f"No 'set' or 'change' specified for property {property['property']} \
                    in Roadway Network Change project card"
            )
        return _updated_series

    def apply_roadway_feature_change(
        self,
        df_idx: list,
        properties: dict,
        geometry_type="links",
    ) -> "RoadwayNetwork":
        """
        Changes the roadway attributes for the selected features based on the
        project card information passed

        Args:
            df_idx : list
                lndices of all links or nodes to apply change to
            properties : list of dictionarys
                roadway properties to change
            geometry_type: either 'links' or 'nodes'. Defaults to 'link'
        """
        if geometry_type == "links":
            self._apply_links_feature_change(df_idx, properties)
        elif geometry_type == "nodes":
            self._apply_nodes_feature_change(df_idx, properties)
        else:
            raise ValueError("geometry_type must be either 'links' or 'nodes'")

        return self

    def _apply_nodes_feature_change(
        self,
        node_idx: list,
        properties: dict,
    ) -> "RoadwayNetwork":
        """
        Changes the roadway attributes for the selected nodes based on the
        project card information passed

        Args:
            df_idx : list of indices of all links or nodes to apply change to
            properties : list of dictionarys
                roadway properties to change
        """
        WranglerLogger.debug("Updating Nodes")

        self.validate_properties(self.nodes_df, properties)
        for p in properties:

            if not p["property"] in self.nodes_df.columns:
                _df = self._add_property(self.nodes_df, p)

            _updated_nodes_df = self._update_property(self.nodes_df.loc[node_idx], p)
            self.nodes_df.update(_updated_nodes_df)

        _property_names = [p["property"] for p in properties]

        WranglerLogger.info(
            f"Updated following node properties: \
            {','.join(_property_names)}"
        )

        if [p for p in _property_names if p in RoadwayNetwork.GEOMETRY_PROPERTIES]:
            self.update_node_geometry(node_idx)
            WranglerLogger.debug("Updated node geometry and associated links/shapes.")
        return self.nodes_df

    def _apply_links_feature_change(
        self,
        link_idx: list,
        properties: dict,
    ) -> "RoadwayNetwork":
        """
        Changes the roadway attributes for the selected links based on the
        project card information passed

        Args:
            link_idx : list od indices of all links to apply change to
            properties : list of dictionarys
                roadway properties to change
        """
        WranglerLogger.debug("Updating Links.")

        self.validate_properties(self.links_df, properties)
        for p in properties:

            if not p["property"] in self.links_df.columns:
                self.links_df = self._add_property(self.links_df, p)

            _updated_links_df = self._update_property(self.links_df.loc[link_idx], p)
            self.links_df.update(_updated_links_df)

        WranglerLogger.info(
            f"Updated following link properties: \
            {','.join([p['property'] for p in properties])}"
        )

    def apply_managed_lane_feature_change(
        self,
        link_idx: list,
        properties: dict,
    ) -> "RoadwayNetwork":
        """
        Apply the managed lane feature changes to the roadway network

        Args:
            link_idx : list of lndices of all links to apply change to
            properties : list of dictionarys roadway properties to change

        .. todo:: decide on connectors info when they are more specific in project card
        """

        # add ML flag to relevant links
        if "managed" in self.links_df.columns:
            self.links_df.loc[link_idx, "managed"] = 1
        else:
            self.links_df["managed"] = 0
            self.links_df.loc[link_idx, "managed"] = 1

        for p in properties:
            attribute = p["property"]
            attr_value = ""

            for idx in link_idx:
                if "group" in p.keys():
                    attr_value = {}

                    if "set" in p.keys():
                        attr_value["default"] = p["set"]
                    elif "change" in p.keys():
                        attr_value["default"] = (
                            self.links_df.at[idx, attribute] + p["change"]
                        )

                    attr_value["timeofday"] = []

                    for g in p["group"]:
                        category = g["category"]
                        for tod in g["timeofday"]:
                            if "set" in tod.keys():
                                attr_value["timeofday"].append(
                                    {
                                        "category": category,
                                        "time": parse_time_spans_to_secs(tod["time"]),
                                        "value": tod["set"],
                                    }
                                )
                            elif "change" in tod.keys():
                                attr_value["timeofday"].append(
                                    {
                                        "category": category,
                                        "time": parse_time_spans_to_secs(tod["time"]),
                                        "value": self.links_df.at[idx, attribute]
                                        + tod["change"],
                                    }
                                )

                elif "timeofday" in p.keys():
                    attr_value = {}

                    if "set" in p.keys():
                        attr_value["default"] = p["set"]
                    elif "change" in p.keys():
                        attr_value["default"] = (
                            self.links_df.at[idx, attribute] + p["change"]
                        )

                    attr_value["timeofday"] = []

                    for tod in p["timeofday"]:
                        if "set" in tod.keys():
                            attr_value["timeofday"].append(
                                {
                                    "time": parse_time_spans_to_secs(tod["time"]),
                                    "value": tod["set"],
                                }
                            )
                        elif "change" in tod.keys():
                            attr_value["timeofday"].append(
                                {
                                    "time": parse_time_spans_to_secs(tod["time"]),
                                    "value": self.links_df.at[idx, attribute]
                                    + tod["change"],
                                }
                            )
                elif "set" in p.keys():
                    attr_value = p["set"]

                elif "change" in p.keys():
                    attr_value = self.links_df.at[idx, attribute] + p["change"]

                if attribute in self.links_df.columns and not isinstance(
                    attr_value, numbers.Number
                ):
                    # if the attribute already exists
                    # and the attr value we are trying to set is not numeric
                    # then change the attribute type to object
                    self.links_df[attribute] = self.links_df[attribute].astype(object)

                if attribute not in self.links_df.columns:
                    # if it is a new attribute then initialize with NaN values
                    self.links_df[attribute] = "NaN"

                self.links_df.at[idx, attribute] = attr_value

        WranglerLogger.debug(f"{len(self.nodes_df)} Nodes in Network")

        return self

    def _create_links(self, new_links: Collection[dict] = []):

        new_links_df = pd.DataFrame(new_links)

        _idx_c = RoadwayNetwork.UNIQUE_LINK_KEY + "_idx"
        new_links_df[_idx_c] = new_links_df[RoadwayNetwork.UNIQUE_LINK_KEY]
        new_links_df.set_index(_idx_c, inplace=True)
        new_links_df = RoadwayNetwork.coerce_types(new_links_df)

        new_links_df = self._create_new_link_geometry(new_links_df)

        WranglerLogger.debug(
            f"New Links:\n{new_links_df[[RoadwayNetwork.UNIQUE_LINK_KEY,'name']]}"
        )
        assert self.new_links_valid(new_links_df)
        return new_links_df

    def _create_nodes(self, new_nodes: Collection[dict] = []):

        new_nodes_df = pd.DataFrame(new_nodes)

        _idx_c = RoadwayNetwork.UNIQUE_NODE_KEY + "_idx"
        new_nodes_df[_idx_c] = new_nodes_df[RoadwayNetwork.UNIQUE_NODE_KEY]
        new_nodes_df.set_index(_idx_c, inplace=True)
        new_nodes_df = RoadwayNetwork.coerce_types(new_nodes_df)

        new_nodes_df["geometry"] = new_nodes_df.apply(
            lambda x: point_from_xy(
                x["X"],
                x["Y"],
                xy_crs=RoadwayNetwork.CRS,
                point_crs=RoadwayNetwork.CRS,
            ),
            axis=1,
        )

        new_nodes_df = gpd.GeoDataFrame(new_nodes_df)
        WranglerLogger.debug(f"New Nodes:\n{new_nodes_df}")

        assert self.new_nodes_valid(new_nodes_df)
        return new_nodes_df

    def add_new_roadway_feature_change(
        self, add_links: Collection[dict] = [], add_nodes: Collection[dict] = []
    ) -> None:
        """
        Add the new roadway features defined in the project card.

        New shapes are also added for the new roadway links.

        New nodes are added first so that links can refer to any added nodes.

        args:
            add_links: list of dictionaries
            add_nodes: list of dictionaries

        returns: updated network with new links and nodes and associated geometries

        .. todo:: validate links and nodes dictionary
        """
        WranglerLogger.debug(
            f"Adding New Roadway Features:\n-Links:\n{add_links}\n-Nodes:\n{add_nodes}"
        )
        if add_nodes:
            _new_nodes_df = self._create_nodes(add_nodes)
            self.nodes_df = pd.concat([self.nodes_df, _new_nodes_df])

        if add_links:
            _new_links_df = self._create_links(add_links)
            self.links_df = pd.concat([self.links_df, _new_links_df])

            _new_shapes_df = RoadwayNetwork._create_new_shapes_from_links(_new_links_df)
            self.shapes_df = pd.concat([self.shapes_df, _new_shapes_df])
        return self

    def new_nodes_valid(self, new_nodes_df: pd.DataFrame) -> bool:

        # Check to see if same node is already in the network
        _existing_nodes = new_nodes_df[RoadwayNetwork.NODE_FOREIGN_KEY_TO_LINK].apply(
            self.has_node
        )
        if _existing_nodes.any():
            msg = f"Node already exists between nodes:\n {new_nodes_df[_existing_nodes,RoadwayNetwork.NODE_FOREIGN_KEY_TO_LINK]}."
            raise ValueError(msg)

        # Check to see if there are missing required columns
        _missing_cols = [
            c
            for c in RoadwayNetwork.MIN_NODE_REQUIRED_PROPS_DEFAULT
            if c not in new_nodes_df.columns
        ]
        if _missing_cols:
            msg = f"Missing required link properties:{_missing_cols}"
            raise ValueError(msg)

        # Check to see if there are missing required values
        _missing_values = new_nodes_df[
            RoadwayNetwork.MIN_NODE_REQUIRED_PROPS_DEFAULT
        ].isna()
        if _missing_values.any().any():
            msg = f"Missing values for required node properties:\n{new_nodes_df.loc[_missing_values]}"
            WranglerLogger.Warning(msg)

        return True

    def new_links_valid(self, new_links_df: pd.DataFrame) -> bool:
        """Assesses if a set of links are valid for adding to self.links_df.

        Will produce a ValueError if new_links_df:
        1. A-B combinations are not unique within new_links_df
        2. UNIQUE_LINK_KEY is not unique within new_links_df
        3. A-B combinations overlap with an existing A-B link in self.links_df
        4. UNIQUE_LINK_KEY overlaps with an existing UNIQUE_LINK_ID in self.links_df
        5. A and B nodes are not in self.nodes_df
        6. Doesn't contain columns for MIN_LINK_REQUIRED_PROPS_DEFAULT

        Will produce a warning if there are NA values for any MIN_LINK_REQUIRED_PROPS_DEFAULT

        Args:
            new_links_df: dataframe of links being considered for addition to self.links_df

        Returns:
            bool: Returns a True if passes various validation tests.
        """

        # A-B combinations are unique within new_links_df
        _new_fk_id = pd.Series(
            zip(*[new_links_df[c] for c in RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE])
        )
        if not _new_fk_id.is_unique:
            msg = f"Duplicate ABs in new links."
            raise ValueError(msg)

        # UNIQUE_LINK_ID is unique within new_links_df
        if not new_links_df[RoadwayNetwork.UNIQUE_LINK_KEY].is_unique:
            msg = f"Duplicate link IDs in new links."
            raise ValueError(msg)

        # Doesn't overlap with an existing A-B link in self.links_df
        _existing_links_ab = _new_fk_id.apply(self.has_link)
        if _existing_links_ab.any():
            msg = f"Link already exists between nodes:\n {_new_fk_id[_existing_links_ab]}."
            raise ValueError(msg)

        # Doesn't overlap with an existing UNIQUE_LINK_ID in self.links_df
        _ids = pd.concat(
            [
                self.links_df[RoadwayNetwork.UNIQUE_LINK_KEY],
                new_links_df[RoadwayNetwork.UNIQUE_LINK_KEY],
            ]
        )
        if not _ids.is_unique:
            msg = f"Link ID already exists:\n{_ids.loc[_ids.duplicated()]}."
            raise ValueError(msg)

        # A and B nodes are in self.nodes_df
        for fk_prop in RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE:
            _has_node = new_links_df[fk_prop].apply(self.has_node)
            if not _has_node.all():
                if len(self.nodes) < 25:
                    WranglerLogger.debug(f"self.nodes_df:\n{self.nodes_df}")
                msg = f"New link specifies non existant node {fk_prop} = {new_links_df.loc[_has_node,fk_prop]}."
                raise ValueError(msg)

        # Check to see if there are missing required columns
        _missing_cols = [
            c
            for c in RoadwayNetwork.MIN_LINK_REQUIRED_PROPS_DEFAULT
            if c not in new_links_df.columns
        ]
        if _missing_cols:
            msg = f"Missing required link properties:{_missing_cols}"
            raise ValueError(msg)

        # Check to see if there are missing required values
        _missing_values = new_links_df[
            RoadwayNetwork.MIN_LINK_REQUIRED_PROPS_DEFAULT
        ].isna()
        if _missing_values.any().any():
            msg = f"Missing values for required link properties:\n{new_links_df.loc[_missing_values]}"
            WranglerLogger.Warning(msg)

        return True

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

    def delete_links(self, del_links, dict, ignore_missing=True) -> None:
        """
        Delete the roadway links based on del_links dictionary selecting links by properties.

        Also deletes shapes which no longer have links associated with them.

        Args:
            del_links: Dictionary identified shapes to delete by properties.  Links will be selected
                if *any* of the properties are equal to *any* of the values.
            ignore_missing: If True, will only warn if try to delete a links that isn't in network.
                If False, it will fail on missing links. Defaults to True.
        """
        # if RoadwayNetwork.UNIQUE_LINK_ID is used to select, flag links that weren't in network.
        if RoadwayNetwork.UNIQUE_LINK_KEY in del_links:
            _del_link_ids = pd.Series(del_links[RoadwayNetwork.UNIQUE_LINK_KEY])
            _missing_links = _del_link_ids[
                ~_del_link_ids.isin(self.links_df[RoadwayNetwork.UNIQUE_LINK_KEY])
            ]
            msg = f"Following links cannot be deleted because they are not in the network: {_missing_links}"
            if len(_missing_links) and ignore_missing:
                WranglerLogger.warning(msg)
            elif len(_missing_links):
                raise ValueError(msg)

        _del_links_mask = self.links_df.isin(del_links).any(axis=1)
        if not _del_links_mask.any():
            WranglerLogger.warning("No links found matching criteria to delete.")
            return
        WranglerLogger.debug(
            f"Deleting following links:\n{self.links_df.loc[_del_links_mask][['A','B','model_link_id']]}"
        )
        self.links_df = self.links_df.loc[~_del_links_mask]

        # Delete shapes which no longer have links associated with them
        _shapes_without_links = self._shapes_without_links()
        if len(_shapes_without_links):
            WranglerLogger.debug(f"Shapes without links:\n {_shapes_without_links}")
            self.shapes_df = self.shapes_df.loc[~_shapes_without_links]
            WranglerLogger.debug(f"self.shapes_df reduced to:\n {self.shapes_df}")

    def delete_nodes(self, del_nodes: dict, ignore_missing: bool = True) -> None:
        """
        Delete the roadway nodes based on del_nodes dictionary selecting nodes by properties.

        Will fail if try to delete node that is currently being used by a link.

        Args:
            del_nodes : Dictionary identified nodes to delete by properties.  Nodes will be selected
                if *any* of the properties are equal to *any* of the values.
            ignore_missing: If True, will only warn if try to delete a node that isn't in network.
                If False, it will fail on missing nodes. Defaults to True.
        """
        _del_nodes_mask = self.nodes_df.isin(del_nodes).any(axis=1)
        _del_nodes_df = self.nodes_df.loc[_del_nodes_mask]

        if not _del_nodes_mask.any():
            WranglerLogger.warning("No nodes found matching criteria to delete.")
            return

        WranglerLogger.debug(f"Deleting Nodes:\n{_del_nodes_df}")
        # Check if node used in an existing link
        _links_with_nodes = RoadwayNetwork.links_with_nodes(
            self.links_df,
            _del_nodes_df[RoadwayNetwork.NODE_FOREIGN_KEY_TO_LINK].tolist(),
        )
        if len(_links_with_nodes):
            WranglerLogger.error(
                f"Node deletion failed because being used in following links:\n{_links_with_nodes[RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE]}"
            )
            raise ValueError
        
        # Check if node is in network
        if RoadwayNetwork.UNIQUE_NODE_KEY in del_nodes:
            _del_node_ids = pd.Series(del_nodes[RoadwayNetwork.UNIQUE_NODE_KEY])
            _missing_nodes = _del_node_ids[
                ~_del_node_ids.isin(self.nodes_df[RoadwayNetwork.UNIQUE_NODE_KEY])
            ]
            msg = f"Following nodes cannot be deleted because they are not in the network: {_missing_nodes}"
            if len(_missing_nodes) and ignore_missing:
                WranglerLogger.warning(msg)
            elif len(_missing_nodes):
                raise ValueError(msg)
        self.nodes_df = self.nodes_df.loc[~_del_nodes_mask]

    def delete_roadway_feature_change(
        self,
        del_links: dict = None,
        del_nodes: dict = None,
        ignore_missing=True,
    ) -> "RoadwayNetwork":
        """
        Delete the roadway links or nodes defined in the project card.

        Corresponding shapes to the deleted links are also deleted if they are not used elsewhere.

        Args:
            del_links : dictionary of identified links to delete
            del_nodes : dictionary of identified nodes to delete
            ignore_missing: bool
                If True, will only warn about links/nodes that are missing from
                network but specified to "delete" in project card
                If False, will fail.
        """

        WranglerLogger.debug(
            f"Deleting Roadway Features:\n-Links:\n{del_links}\n-Nodes:\n{del_nodes}"
        )

        if del_links:
            self.delete_links(del_links, ignore_missing)

        if del_nodes:
            self.delete_nodes(del_nodes, ignore_missing)

        return self

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

    @staticmethod
    def get_modal_links_nodes(
        links_df: DataFrame, nodes_df: DataFrame, modes: list[str] = None
    ) -> tuple(DataFrame, DataFrame):
        """Returns nodes and link dataframes for specific mode.

        Args:
            links_df: DataFrame of standard network links
            nodes_df: DataFrame of standard network nodes
            modes: list of the modes of the network to be kept, must be in
                `drive`,`transit`,`rail`,`bus`,`walk`, `bike`.
                For example, if bike and walk are selected, both bike and walk links will be kept.

        Returns: tuple of DataFrames for links, nodes filtered by mode

        .. todo:: Right now we don't filter the nodes because transit-only
        links with walk access are not marked as having walk access
        Issue discussed in https://github.com/wsp-sag/network_wrangler/issues/145
        modal_nodes_df = nodes_df[nodes_df[mode_node_variable] == 1]
        """
        for mode in modes:
            if mode not in RoadwayNetwork.MODES_TO_NETWORK_LINK_VARIABLES.keys():
                msg = "mode value should be one of {}, got {}".format(
                    list(RoadwayNetwork.MODES_TO_NETWORK_LINK_VARIABLES.keys()),
                    mode,
                )
                WranglerLogger.error(msg)
                raise ValueError(msg)

        mode_link_variables = list(
            set(
                [
                    mode
                    for mode in modes
                    for mode in RoadwayNetwork.MODES_TO_NETWORK_LINK_VARIABLES[mode]
                ]
            )
        )
        mode_node_variables = list(
            set(
                [
                    mode
                    for mode in modes
                    for mode in RoadwayNetwork.MODES_TO_NETWORK_NODE_VARIABLES[mode]
                ]
            )
        )

        if not set(mode_link_variables).issubset(set(links_df.columns)):
            msg = f"""{set(mode_link_variables) - set(links_df.columns)} not in provided links_df \
                list of columns. Available columns are:
                {links_df.columns}"""
            WranglerLogger.error(msg)

        if not set(mode_node_variables).issubset(set(nodes_df.columns)):
            msg = f"""{set(mode_node_variables) - set(nodes_df.columns)} not in provided nodes_df \
                list of columns. Available columns are:
                {nodes_df.columns}"""
            WranglerLogger.error(msg)

        modal_links_df = links_df.loc[links_df[mode_link_variables].any(axis=1)]

        # TODO right now we don't filter the nodes because transit-only
        # links with walk access are not marked as having walk access
        # Issue discussed in https://github.com/wsp-sag/network_wrangler/issues/145
        # modal_nodes_df = nodes_df[nodes_df[mode_node_variable] == 1]
        modal_nodes_df = nodes_df

        return modal_links_df, modal_nodes_df

    @staticmethod
    def get_modal_graph(links_df: DataFrame, nodes_df: DataFrame, mode: str = None):
        """Determines if the network graph is "strongly" connected
        A graph is strongly connected if each vertex is reachable from every other vertex.

        Args:
            links_df: DataFrame of standard network links
            nodes_df: DataFrame of standard network nodes
            mode: mode of the network, one of `drive`,`transit`,
                `walk`, `bike`

        Returns: networkx: osmnx: DiGraph  of network
        """
        if mode not in RoadwayNetwork.MODES_TO_NETWORK_LINK_VARIABLES.keys():
            msg = "mode value should be one of {}.".format(
                list(RoadwayNetwork.MODES_TO_NETWORK_LINK_VARIABLES.keys())
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

        _links_df, _nodes_df = RoadwayNetwork.get_modal_links_nodes(
            links_df,
            nodes_df,
            modes=[mode],
        )
        G = RoadwayNetwork.ox_graph(_nodes_df, _links_df)

        return G

    def is_network_connected(
        self, mode: str = None, links_df: DataFrame = None, nodes_df: DataFrame = None
    ):
        """
        Determines if the network graph is "strongly" connected
        A graph is strongly connected if each vertex is reachable from every other vertex.

        Args:
            mode:  mode of the network, one of `drive`,`transit`,
                `walk`, `bike`
            links_df: DataFrame of standard network links
            nodes_df: DataFrame of standard network nodes

        Returns: boolean

        .. todo:: Consider caching graphs if they take a long time.
        """

        _nodes_df = nodes_df if nodes_df else self.nodes_df
        _links_df = links_df if links_df else self.links_df

        if mode:
            _links_df, _nodes_df = RoadwayNetwork.get_modal_links_nodes(
                _links_df,
                _nodes_df,
                modes=[mode],
            )
        else:
            WranglerLogger.info(
                "Assessing connectivity without a mode\
                specified. This may have limited value in interpretation.\
                To add mode specificity, add the keyword `mode =` to calling\
                this method"
            )

        # TODO: consider caching graphs if they start to take forever
        #      and we are calling them more than once.
        G = RoadwayNetwork.ox_graph(_nodes_df, _links_df)
        is_connected = nx.is_strongly_connected(G)

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

    def identify_segment_endpoints(
        self,
        mode: str = "",
        links_df: DataFrame = None,
        nodes_df: DataFrame = None,
        min_connecting_links: int = 10,
        min_distance: float = None,
        max_link_deviation: int = 2,
    ):
        """

        Args:
            mode:  list of modes of the network, one of `drive`,`transit`,
                `walk`, `bike`
            links_df: if specified, will assess connectivity of this
                links list rather than self.links_df
            nodes_df: if specified, will assess connectivity of this
                nodes list rather than self.nodes_df

        """
        SEGMENT_IDENTIFIERS = ["name", "ref"]

        NAME_PER_NODE = 4
        REF_PER_NODE = 2

        _nodes_df = nodes_df if nodes_df else self.nodes_df
        _links_df = links_df if links_df else self.links_df

        if mode:
            _links_df, _nodes_df = RoadwayNetwork.get_modal_links_nodes(
                _links_df,
                _nodes_df,
                modes=[mode],
            )
        else:
            WranglerLogger.warning(
                "Assessing connectivity without a mode\
                specified. This may have limited value in interpretation.\
                To add mode specificity, add the keyword `mode =` to calling\
                this method"
            )

        _nodes_df = RoadwayNetwork.add_incident_link_data_to_nodes(
            links_df=_links_df,
            nodes_df=_nodes_df,
            link_variables=SEGMENT_IDENTIFIERS + ["distance"],
        )
        WranglerLogger.debug("Node/Link table elements: {}".format(len(_nodes_df)))

        # Screen out segments that have blank name AND refs
        _nodes_df = _nodes_df.replace(r"^\s*$", np.nan, regex=True).dropna(
            subset=["name", "ref"]
        )

        WranglerLogger.debug(
            "Node/Link table elements after dropping empty name AND ref : {}".format(
                len(_nodes_df)
            )
        )

        # Screen out segments that aren't likely to be long enough
        # Minus 1 in case ref or name is missing on an intermediate link
        _min_ref_in_table = REF_PER_NODE * (min_connecting_links - max_link_deviation)
        _min_name_in_table = NAME_PER_NODE * (min_connecting_links - max_link_deviation)

        _nodes_df["ref_freq"] = _nodes_df["ref"].map(_nodes_df["ref"].value_counts())
        _nodes_df["name_freq"] = _nodes_df["name"].map(_nodes_df["name"].value_counts())

        _nodes_df = _nodes_df.loc[
            (_nodes_df["ref_freq"] >= _min_ref_in_table)
            & (_nodes_df["name_freq"] >= _min_name_in_table)
        ]

        WranglerLogger.debug(
            "Node/Link table has n = {} after screening segments for min length:\n{}".format(
                len(_nodes_df),
                _nodes_df[
                    [
                        RoadwayNetwork.UNIQUE_NODE_KEY,
                        "name",
                        "ref",
                        "distance",
                        "ref_freq",
                        "name_freq",
                    ]
                ],
            )
        )
        # ----------------------------------------
        # Find nodes that are likely endpoints
        # ----------------------------------------

        # - Likely have one incident link and one outgoing link
        _max_ref_endpoints = REF_PER_NODE / 2
        _max_name_endpoints = NAME_PER_NODE / 2
        # - Attach frequency  of node/ref
        _nodes_df = _nodes_df.merge(
            _nodes_df.groupby(by=[RoadwayNetwork.UNIQUE_NODE_KEY, "ref"])
            .size()
            .rename("ref_N_freq"),
            on=[RoadwayNetwork.UNIQUE_NODE_KEY, "ref"],
        )
        # WranglerLogger.debug("_ref_count+_nodes:\n{}".format(_nodes_df[["model_node_id","ref","name","ref_N_freq"]]))
        # - Attach frequency  of node/name
        _nodes_df = _nodes_df.merge(
            _nodes_df.groupby(by=[RoadwayNetwork.UNIQUE_NODE_KEY, "name"])
            .size()
            .rename("name_N_freq"),
            on=[RoadwayNetwork.UNIQUE_NODE_KEY, "name"],
        )
        # WranglerLogger.debug("_name_count+_nodes:\n{}".format(_nodes_df[["model_node_id","ref","name","name_N_freq"]]))

        WranglerLogger.debug(
            "Possible segment endpoints:\n{}".format(
                _nodes_df[
                    [
                        RoadwayNetwork.UNIQUE_NODE_KEY,
                        "name",
                        "ref",
                        "distance",
                        "ref_N_freq",
                        "name_N_freq",
                    ]
                ]
            )
        )
        # - Filter possible endpoint list based on node/name node/ref frequency
        _nodes_df = _nodes_df.loc[
            (_nodes_df["ref_N_freq"] <= _max_ref_endpoints)
            | (_nodes_df["name_N_freq"] <= _max_name_endpoints)
        ]
        WranglerLogger.debug(
            "{} Likely segment endpoints with req_ref<= {} or freq_name<={} \n{}".format(
                len(_nodes_df),
                _max_ref_endpoints,
                _max_name_endpoints,
                _nodes_df[
                    [
                        RoadwayNetwork.UNIQUE_NODE_KEY,
                        "name",
                        "ref",
                        "ref_N_freq",
                        "name_N_freq",
                    ]
                ],
            )
        )
        # ----------------------------------------
        # Assign a segment id
        # ----------------------------------------
        _nodes_df["segment_id"], _segments = pd.factorize(
            _nodes_df.name + _nodes_df.ref
        )
        WranglerLogger.debug("{} Segments:\n{}".format(len(_segments), _segments))

        # ----------------------------------------
        # Drop segments without at least two nodes
        # ----------------------------------------

        # https://stackoverflow.com/questions/13446480/python-pandas-remove-entries-based-on-the-number-of-occurrences
        _nodes_df = _nodes_df[
            _nodes_df.groupby(["segment_id", RoadwayNetwork.UNIQUE_NODE_KEY])[
                RoadwayNetwork.UNIQUE_NODE_KEY
            ].transform(len)
            > 1
        ]

        WranglerLogger.debug(
            "{} Segments with at least nodes:\n{}".format(
                len(_nodes_df),
                _nodes_df[
                    [RoadwayNetwork.UNIQUE_NODE_KEY, "name", "ref", "segment_id"]
                ],
            )
        )

        # ----------------------------------------
        # For segments with more than two nodes, find farthest apart pairs
        # ----------------------------------------

        def _max_segment_distance(row):
            _segment_nodes = _nodes_df.loc[_nodes_df["segment_id"] == row["segment_id"]]
            dist = _segment_nodes.geometry.distance(row.geometry)
            return max(dist.dropna())

        _nodes_df["seg_distance"] = _nodes_df.apply(_max_segment_distance, axis=1)
        _nodes_df = _nodes_df.merge(
            _nodes_df.groupby("segment_id")
            .seg_distance.agg(max)
            .rename("max_seg_distance"),
            on="segment_id",
        )

        _nodes_df = _nodes_df.loc[
            (_nodes_df["max_seg_distance"] == _nodes_df["seg_distance"])
            & (_nodes_df["seg_distance"] > 0)
        ].drop_duplicates(subset=[RoadwayNetwork.UNIQUE_NODE_KEY, "segment_id"])

        # ----------------------------------------
        # Reassign segment id for final segments
        # ----------------------------------------
        _nodes_df["segment_id"], _segments = pd.factorize(
            _nodes_df.name + _nodes_df.ref
        )

        WranglerLogger.debug(
            "{} Segments:\n{}".format(
                len(_segments),
                _nodes_df[
                    [
                        RoadwayNetwork.UNIQUE_NODE_KEY,
                        "name",
                        "ref",
                        "segment_id",
                        "seg_distance",
                    ]
                ],
            )
        )

        return _nodes_df[
            ["segment_id", RoadwayNetwork.UNIQUE_NODE_KEY, "geometry", "name", "ref"]
        ]

    def identify_segment(
        self,
        O_id,
        D_id,
        selection_dict: dict = {},
        mode=None,
        nodes_df=None,
        links_df=None,
    ):
        """
        Args:
            endpoints: list of length of two unique keys of nodes making up endpoints of segment
            selection_dict: dictionary of link variables to select candidate links from, otherwise
                will create a graph of ALL links which will be both a RAM hog and could result in
                odd shortest paths.
            segment_variables: list of variables to keep
        """
        _nodes_df = nodes_df if nodes_df else self.nodes_df
        _links_df = links_df if links_df else self.links_df

        if mode:
            _links_df, _nodes_df = RoadwayNetwork.get_modal_links_nodes(
                _links_df,
                _nodes_df,
                modes=[mode],
            )
        else:
            WranglerLogger.warning(
                "Assessing connectivity without a mode\
                specified. This may have limited value in interpretation.\
                To add mode specificity, add the keyword `mode =` to calling\
                this method"
            )

        if selection_dict:
            _query = " or ".join(
                [f"{k} == {repr(v)}" for k, v in selection_dict.items()]
            )
            _candidate_links = _links_df.query(_query)
            WranglerLogger.debug(
                "Found {} candidate links from {} total links using following query:\n{}".format(
                    len(_candidate_links), len(_links_df), _query
                )
            )
        else:
            _candidate_links = _links_df

            WranglerLogger.warning(
                "Not pre-selecting links using selection_dict can use up a lot of RAM and \
                    also result in odd segment paths."
            )

        WranglerLogger.debug(
            "_candidate links for segment: \n{}".format(
                _candidate_links[["u", "v", "A", "B", "name", "ref"]]
            )
        )

        try:
            (G, candidate_links, sp_route, sp_links) = self.path_search(
                _candidate_links, O_id, D_id
            )
        except NoPathFound:
            msg = "Route not found from {} to {} using selection candidates {}".format(
                O_id, D_id, selection_dict
            )
            WranglerLogger.warning(msg)
            sp_links = pd.DataFrame()

        return sp_links

    def assess_connectivity(
        self,
        mode: str = "",
        ignore_end_nodes: bool = True,
        links_df: DataFrame = None,
        nodes_df: DataFrame = None,
    ):
        """Returns a network graph and list of disconnected subgraphs
        as described by a list of their member nodes.

        Args:
            mode:  list of modes of the network, one of `drive`,`transit`,
                `walk`, `bike`
            ignore_end_nodes: if True, ignores stray singleton nodes
            links_df: if specified, will assess connectivity of this
                links list rather than self.links_df
            nodes_df: if specified, will assess connectivity of this
                nodes list rather than self.nodes_df

        Returns: Tuple of
            Network Graph (osmnx flavored networkX DiGraph)
            List of disconnected subgraphs described by the list of their
                member nodes (as described by their `model_node_id`)
        """
        _nodes_df = nodes_df if nodes_df else self.nodes_df
        _links_df = links_df if links_df else self.links_df

        if mode:
            _links_df, _nodes_df = RoadwayNetwork.get_modal_links_nodes(
                _links_df,
                _nodes_df,
                modes=[mode],
            )
        else:
            WranglerLogger.warning(
                "Assessing connectivity without a mode\
                specified. This may have limited value in interpretation.\
                To add mode specificity, add the keyword `mode =` to calling\
                this method"
            )

        G = RoadwayNetwork.ox_graph(_nodes_df, _links_df)

        sub_graph_nodes = [
            list(s)
            for s in sorted(nx.strongly_connected_components(G), key=len, reverse=True)
        ]

        # sorted on decreasing length, dropping the main sub-graph
        disconnected_sub_graph_nodes = sub_graph_nodes[1:]

        # dropping the sub-graphs with only 1 node
        if ignore_end_nodes:
            disconnected_sub_graph_nodes = [
                list(s) for s in disconnected_sub_graph_nodes if len(s) > 1
            ]

        WranglerLogger.info(
            "{} for disconnected networks for mode = {}:\n{}".format(
                RoadwayNetwork.UNIQUE_NODE_KEY,
                mode,
                "\n".join(list(map(str, disconnected_sub_graph_nodes))),
            )
        )
        return G, disconnected_sub_graph_nodes

    @staticmethod
    def network_connection_plot(G, disconnected_subgraph_nodes: list):
        """Plot a graph to check for network connection.

        Args:
            G: OSMNX flavored networkX graph.
            disconnected_subgraph_nodes: List of disconnected subgraphs described by the list
                of their member nodes (as described by their `model_node_id`).

        returns: fig, ax : tuple
        """

        colors = []
        for i in range(len(disconnected_subgraph_nodes)):
            colors.append("#%06X" % randint(0, 0xFFFFFF))

        fig, ax = ox.plot_graph(
            G,
            figsize=(16, 16),
            show=False,
            close=True,
            edge_color="black",
            edge_alpha=0.1,
            node_color="black",
            node_alpha=0.5,
            node_size=10,
        )
        i = 0
        for nodes in disconnected_subgraph_nodes:
            for n in nodes:
                size = 100
                ax.scatter(G.nodes[n]["X"], G.nodes[n]["Y"], c=colors[i], s=size)
            i = i + 1

        return fig, ax

    def selection_map(
        self,
        selected_link_idx: list,
        A: Optional[Any] = None,
        B: Optional[Any] = None,
        candidate_link_idx: Optional[List] = [],
    ):
        """
        Shows which links are selected for roadway property change or parallel
        managed lanes category of roadway projects.

        Args:
            selected_links_idx: list of selected link indices
            candidate_links_idx: optional list of candidate link indices to also include in map
            A: optional foreign key of starting node of a route selection
            B: optional foreign key of ending node of a route selection
        """
        WranglerLogger.debug(
            "Selected Links: {}\nCandidate Links: {}\n".format(
                selected_link_idx, candidate_link_idx
            )
        )

        graph_link_idx = list(set(selected_link_idx + candidate_link_idx))
        graph_links = self.links_df.loc[graph_link_idx]

        node_list_foreign_keys = list(
            set(
                [
                    i
                    for fk in RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE
                    for i in list(graph_links[fk])
                ]
            )
        )

        graph_nodes = self.nodes_df.loc[node_list_foreign_keys]

        G = RoadwayNetwork.ox_graph(graph_nodes, graph_links)

        # base map plot with whole graph
        m = ox.plot_graph_folium(
            G, edge_color=None, tiles="cartodbpositron", width="300px", height="250px"
        )

        # plot selection
        selected_links = self.links_df.loc[selected_link_idx]

        for _, row in selected_links.iterrows():
            pl = ox.folium._make_folium_polyline(
                geom=row["geometry"],
                edge=row,
                edge_color="blue",
                edge_width=5,
                edge_opacity=0.8,
            )
            pl.add_to(m)

        # if have A and B node add them to base map
        def _folium_node(node_row, color="white", icon=""):
            node_marker = folium.Marker(
                location=[node_row["Y"], node_row["X"]],
                icon=folium.Icon(icon=icon, color=color),
            )
            return node_marker

        if A:
            msg = f"A: {A}\n{self.nodes_df[self.nodes_df[RoadwayNetwork.UNIQUE_NODE_KEY] == A]}"
            # WranglerLogger.debug(msg)
            _folium_node(
                self.nodes_df[self.nodes_df[RoadwayNetwork.UNIQUE_NODE_KEY] == A],
                color="green",
                icon="play",
            ).add_to(m)

        if B:
            _folium_node(
                self.nodes_df[self.nodes_df[RoadwayNetwork.UNIQUE_NODE_KEY] == B],
                color="red",
                icon="star",
            ).add_to(m)

        return m

    def deletion_map(self, links: dict, nodes: dict):
        """
        Shows which links and nodes are deleted from the roadway network
        """

        if links is not None:
            for key, val in links.items():
                deleted_links = self.links_df[self.links_df[key].isin(val)]

                node_list_foreign_keys = list(
                    set(
                        [
                            i
                            for fk in RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE
                            for i in list(deleted_links[fk])
                        ]
                    )
                )
                candidate_nodes = self.nodes_df.loc[node_list_foreign_keys]
        else:
            deleted_links = None

        if nodes is not None:
            for key, val in nodes.items():
                deleted_nodes = self.nodes_df[self.nodes_df[key].isin(val)]
        else:
            deleted_nodes = None

        G = RoadwayNetwork.ox_graph(candidate_nodes, deleted_links)

        m = ox.plot_graph_folium(G, edge_color="red", tiles="cartodbpositron")

        def _folium_node(node, color="white", icon=""):
            node_circle = folium.Circle(
                location=[node["Y"], node["X"]],
                radius=2,
                fill=True,
                color=color,
                fill_opacity=0.8,
            )
            return node_circle

        if deleted_nodes is not None:
            for _, row in deleted_nodes.iterrows():
                _folium_node(row, color="red").add_to(m)

        return m

    def addition_map(self, links: dict, nodes: dict):
        """
        Shows which links and nodes are added to the roadway network
        """

        if links is not None:
            link_ids = []
            for link in links:
                link_ids.append(link.get(RoadwayNetwork.UNIQUE_LINK_KEY))

            added_links = self.links_df[
                self.links_df[RoadwayNetwork.UNIQUE_LINK_KEY].isin(link_ids)
            ]
            node_list_foreign_keys = list(
                set(
                    [
                        i
                        for fk in RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE
                        for i in list(added_links[fk])
                    ]
                )
            )
            try:
                candidate_nodes = self.nodes_df.loc[node_list_foreign_keys]
            except:
                return None

        if nodes is not None:
            node_ids = []
            for node in nodes:
                node_ids.append(node.get(RoadwayNetwork.UNIQUE_NODE_KEY))

            added_nodes = self.nodes_df[
                self.nodes_df[RoadwayNetwork.UNIQUE_NODE_KEY].isin(node_ids)
            ]
        else:
            added_nodes = None

        G = RoadwayNetwork.ox_graph(candidate_nodes, added_links)

        m = ox.plot_graph_folium(G, edge_color="green", tiles="cartodbpositron")

        def _folium_node(node, color="white", icon=""):
            node_circle = folium.Circle(
                location=[node["Y"], node["X"]],
                radius=2,
                fill=True,
                color=color,
                fill_opacity=0.8,
            )
            return node_circle

        if added_nodes is not None:
            for _, row in added_nodes.iterrows():
                _folium_node(row, color="green").add_to(m)

        return m

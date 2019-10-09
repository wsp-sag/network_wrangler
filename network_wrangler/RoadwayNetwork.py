#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import os, sys
import copy

import yaml
import pandas as pd
import geojson
import geopandas as gpd
import json
import networkx as nx
import numpy as np

from pandas.core.frame import DataFrame
from geopandas.geodataframe import GeoDataFrame

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError

import osmnx as ox

from shapely.geometry import Point, LineString

from .Logger import WranglerLogger
from .Utils import point_df_to_geojson, link_df_to_json, parse_time_spans
from .ProjectCard import ProjectCard


class RoadwayNetwork(object):
    """
    Representation of a Roadway Network.
    """

    CRS = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"

    NODE_FOREIGN_KEY = "osmNodeId"

    SEARCH_BREADTH = 5
    MAX_SEARCH_BREADTH = 10
    SP_WEIGHT_FACTOR = 100

    SELECTION_REQUIRES = ["A", "B", "link"]

    def __init__(self, nodes: GeoDataFrame, links: DataFrame, shapes: GeoDataFrame):
        """
        Constructor
        """

        if not RoadwayNetwork.validate_object_types(nodes, links, shapes):
            sys.exit("RoadwayNetwork: Invalid constructor data type")

        self.nodes_df = nodes
        self.links_df = links
        self.shapes_df = shapes

        # Add non-required fields if they aren't there.
        # for field, default_value in RoadwayNetwork.OPTIONAL_FIELDS:
        #    if field not in self.links_df.columns:
        #        self.links_df[field] = default_value

        self.selections = {}

    @staticmethod
    def read(
        link_file: str, node_file: str, shape_file: str, fast: bool = False
    ) -> RoadwayNetwork:
        """
        Reads a network from the roadway network standard
        Validates that it conforms to the schema

        args:
        link_file: full path to the link file
        node_file: full path to the node file
        shape_file: full path to the shape file
        fast: boolean that will skip validation to speed up read time
        """
        if not fast:
            if not (
                RoadwayNetwork.validate_node_schema(node_file)
                and RoadwayNetwork.validate_link_schema(link_file)
                and RoadwayNetwork.validate_shape_schema(shape_file)
            ):

                sys.exit("RoadwayNetwork: Data doesn't conform to schema")

        with open(link_file) as f:
            link_json = json.load(f)

        link_properties = pd.DataFrame(link_json["features"])
        link_geometries = [
            LineString(
                [
                    g["locationReferences"][0]["point"],
                    g["locationReferences"][1]["point"],
                ]
            )
            for g in link_json["features"]
        ]
        links_df = gpd.GeoDataFrame(link_properties, geometry=link_geometries)

        shapes_df = gpd.read_file(shape_file)

        # geopandas uses fiona OGR drivers, which doesn't let you have
        # a list as a property type. Therefore, must read in node_properties
        # separately in a vanilla dataframe and then convert to geopandas

        with open(node_file) as f:
            node_geojson = json.load(f)

        node_properties = pd.DataFrame(
            [g["properties"] for g in node_geojson["features"]]
        )
        node_geometries = [
            Point(g["geometry"]["coordinates"]) for g in node_geojson["features"]
        ]

        nodes_df = gpd.GeoDataFrame(node_properties, geometry=node_geometries)

        nodes_df.gdf_name = "network_nodes"

        nodes_df.set_index(RoadwayNetwork.NODE_FOREIGN_KEY, inplace=True)
        nodes_df.crs = RoadwayNetwork.CRS
        nodes_df["x"] = nodes_df["geometry"].apply(lambda g: g.x)
        nodes_df["y"] = nodes_df["geometry"].apply(lambda g: g.y)
        ## todo: flatten json

        WranglerLogger.info("Read %s links from %s" % (links_df.size, link_file))
        WranglerLogger.info("Read %s nodes from %s" % (nodes_df.size, node_file))
        WranglerLogger.info("Read %s shapes from %s" % (shapes_df.size, shape_file))

        roadway_network = RoadwayNetwork(
            nodes=nodes_df, links=links_df, shapes=shapes_df
        )

        return roadway_network

    def write(self, filename: str, path: str = ".") -> None:
        """
        Writes a network in the roadway network standard

        args:
        path: the path were the output will be saved
        filename: the name prefix of the roadway files that will be generated
        """

        if not os.path.exists(path):
            WranglerLogger.debug("\nPath [%s] doesn't exist; creating." % path)
            os.mkdir(path)

        links_file = os.path.join(path, filename + "_link.json")
        link_property_columns = self.links_df.columns.values.tolist()
        link_property_columns.remove("geometry")
        links_json = link_df_to_json(self.links_df, link_property_columns)
        with open(links_file, "w") as f:
            json.dump(links_json, f)

        nodes_file = os.path.join(path, filename + "_node.geojson")
        # geopandas wont let you write to geojson because
        # it uses fiona, which doesn't accept a list as one of the properties
        # so need to convert the df to geojson manually first
        property_columns = self.nodes_df.columns.values.tolist()
        property_columns.remove("geometry")

        nodes_geojson = point_df_to_geojson(self.nodes_df, property_columns)

        with open(nodes_file, "w") as f:
            json.dump(nodes_geojson, f)

        shapes_file = os.path.join(path, filename + "_shape.geojson")
        self.shapes_df.to_file(shapes_file, driver="GeoJSON")

    @staticmethod
    def validate_object_types(
        nodes: GeoDataFrame, links: DataFrame, shapes: GeoDataFrame
    ):
        """
        Determines if the roadway network is being built with the right object types.
        Returns: boolean

        Does not validate schemas.
        """

        errors = ""

        if not isinstance(nodes, GeoDataFrame):
            error_message = "Incompatible nodes type:{}. Must provide a GeoDataFrame.  ".format(
                type(nodes)
            )
            WranglerLogger.error(error_message)
            errors.append(error_message)
        if not isinstance(links, GeoDataFrame):
            error_message = "Incompatible links type:{}. Must provide a GeoDataFrame.  ".format(
                type(links)
            )
            WranglerLogger.error(error_message)
            errors.append(error_message)
        if not isinstance(shapes, GeoDataFrame):
            error_message = "Incompatible shapes type:{}. Must provide a GeoDataFrame.  ".format(
                type(shapes)
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
            WranglerLogger.error("Failed Node schema validation: Schema Error")
            WranglerLogger.error("Node Schema Loc:{}".format(schema_location))
            WranglerLogger.error(exc.message)

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
            WranglerLogger.error("Failed Link schema validation: Schema Error")
            WranglerLogger.error("Link Schema Loc: {}".format(schema_location))
            WranglerLogger.error(exc.message)

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
            WranglerLogger.error("Failed Shape schema validation: Schema Error")
            WranglerLogger.error("Shape Schema Loc: {}".format(schema_location))
            WranglerLogger.error(exc.message)

        return False

    def validate_selection(self, selection: dict) -> Bool:
        """
        Evaluate whetther the selection dictionary contains the
        minimum required values.

        Parameters
        -----------
        selection : dict
            selection dictionary to be evaluated

        Returns
        -------
        boolean value as to whether the selection dictonary is valid.

        """
        if not set(RoadwayNetwork.SELECTION_REQUIRES).issubset(selection):
            err_msg = "Project Card Selection requires: {}".format(
                ",".join(RoadwayNetwork.SELECTION_REQUIRES)
            )
            err_msg += ", but selection only contains: {}".format(",".join(selection))
            WranglerLogger.error(err_msg)
            raise KeyError(err_msg)

        err = []
        for l in selection["link"]:
            for k, v in l.items():
                if k not in self.links_df.columns:
                    err.append(
                        "{} specified in link selection but not an attribute in network\n".format(
                            k
                        )
                    )
        for k, v in selection["A"].items():
            if k not in self.nodes_df.columns and k != RoadwayNetwork.NODE_FOREIGN_KEY:
                err.append(
                    "{} specified in A node selection but not an attribute in network\n".format(
                        k
                    )
                )
        for k, v in selection["B"].items():
            if k not in self.nodes_df.columns and k != RoadwayNetwork.NODE_FOREIGN_KEY:
                err.append(
                    "{} specified in B node selection but not an attribute in network\n".format(
                        k
                    )
                )
        if err:
            WranglerLogger.error(
                "ERROR: Selection variables in project card not found in network"
            )
            WranglerLogger.error("\n".join(err))
            WranglerLogger.error(
                "--existing node columns:{}".format(" ".join(self.nodes_df.columns))
            )
            WranglerLogger.error(
                "--existing link columns:{}".format(" ".join(self.links_df.columns))
            )
            raise ValueError()
            return False
        else:
            return True

    def orig_dest_nodes_foreign_key(
        self, selection: dict, node_foreign_key: str = ""
    ) -> tuple:
        """
        Returns the foreign key id (whatever is used in the u and v
        variables in the links file) for the AB nodes as a tuple.

        Parameters
        -----------
        selection : dict
            selection dictionary with A and B keys
        node_foreign_key: str
            variable name for whatever is used by the u and v variable
            in the links_df file.  If nothing is specified, assume whatever
            default is (usually osmNodeId)
        """

        if not node_foreign_key:
            node_foreign_key = RoadwayNetwork.NODE_FOREIGN_KEY
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
    def ox_graph(nodes_df, links_df):
        """
        create an osmnx-flavored network graph

        osmnx doesn't like values that are arrays, so remove the variables
        that have arrays.  osmnx also requires that certain variables
        be filled in, so do that too.

        Parameters
        ----------
        nodes_df : GeoDataFrame
        link_df : GeoDataFrame

        Returns
        -------
        networkx multidigraph
        """

        try:
            graph_nodes = nodes_df.drop(
                ["inboundReferenceId", "outboundReferenceId"], axis=1
            )
        except:
            graph_nodes = nodes_df

        graph_nodes.gdf_name = "network_nodes"

        G = ox.gdfs_to_graph(graph_nodes, links_df)

        return G

    def build_selection_key(self, selection_dict):
        sel_query = ProjectCard.build_link_selection_query(selection_dict)

        A_id, B_id = self.orig_dest_nodes_foreign_key(selection_dict)
        sel_key = (sel_query, A_id, B_id)

        return sel_key

    def select_roadway_features(
        self, selection: dict, search_mode="drive"
    ) -> GeoDataFrame:
        """
        Selects roadway features that satisfy selection criteria

        Example usage:
            net.select_roadway_features(
              selection = [ {
                #   a match condition for the from node using osm,
                #   shared streets, or model node number
                'from': {'osmid': '1234'},
                #   a match for the to-node..
                'to': {'shstid': '4321'},
                #   a regex or match for facility condition
                #   could be # of lanes, facility type, etc.
                'facility': {'name':'Main St'},
                }, ... ])

        Parameters
        ------------
        selection : dictionary
            With keys for:
             A - from node
             B - to node
             link - which includes at least a variable for `name`

        Returns
        -------
        shortest path node route : list
           list of foreign IDs of nodes in the selection route
        """

        self.validate_selection(selection)

        # build a selection query based on the selection dictionary
        modes_to_network_variables = {
            "drive": "isDriveLink",
            "transit": "isTransitLink",
            "walk": "isWalkLink",
            "bike": "isBikeLink",
        }

        sel_query = ProjectCard.build_link_selection_query(
            selection, mode=modes_to_network_variables[search_mode]
        )

        # create a unique key for the selection so that we can cache it
        A_id, B_id = self.orig_dest_nodes_foreign_key(selection)
        sel_key = (sel_query, A_id, B_id)

        # if this selection has been queried before, just return the
        # previously selected links

        if sel_key in self.selections:
            if self.selections[sel_key]["selection_found"]:
                return self.selections[sel_key]["selected_links"].index.tolist()
        else:
            self.selections[sel_key] = {}
            self.selections[sel_key]["selection_found"] = False

        # identify candidate links which match the initial query
        # assign them as iteration = 0
        # subsequent iterations that didn't match the query will be
        # assigned a heigher weight in the shortest path
        try:
            self.selections[sel_key]["candidate_links"] = self.links_df.query(
                sel_query, engine="python"
            )
            candidate_links = self.selections[sel_key][
                "candidate_links"
            ]  # b/c too long to keep that way
            candidate_links["i"] = 0

            if len(candidate_links.index) == 0:
                raise Exception("search query did not return anything")
        except:
            selection_has_name_key = any("name" in d for d in selection["link"])

            # if the query doesn't come back with something from 'name'
            # try it again with 'ref' instead
            if selection_has_name_key:
                sel_query = sel_query.replace("name", "ref")

                self.selections[sel_key]["candidate_links"] = self.links_df.query(
                    sel_query, engine="python"
                )
                candidate_links = self.selections[sel_key][
                    "candidate_links"
                ]  # b/c too long to keep that way
                candidate_links["i"] = 0
            else:
                return False

        def _add_breadth(candidate_links, nodes, links, i):
            """
            Add outbound and inbound reference IDs to candidate links
            from existing nodes

            Parameters
            -----------
            candidate_links : GeoDataFrame
                df with the links from the previous iteration that we
                want to add on to

            nodes : GeoDataFrame
                df of all nodes in the full network

            links : GeoDataFrame
                df of all links in the full network

            i : int
                iteration of adding breadth

            Returns
            -------
            candidate_links : GeoDataFrame
                updated df with one more degree of added breadth

            node_list_foreign_keys : list
                list of foreign key ids for nodes in the updated candidate links
                to test if the A and B nodes are in there.
            """
            print("-Adding Breadth-")
            node_list_foreign_keys = list(
                set(list(candidate_links["u"]) + list(candidate_links["v"]))
            )
            candidate_nodes = nodes.loc[node_list_foreign_keys]
            print("Candidate Nodes: {}".format(len(candidate_nodes)))
            links_id_to_add = list(
                set(
                    sum(candidate_nodes["outboundReferenceId"].tolist(), [])
                    + sum(candidate_nodes["inboundReferenceId"].tolist(), [])
                )
                - set(candidate_links["id"].tolist())
                - set([""])
            )
            print("Link IDs to add: {}".format(len(links_id_to_add)))
            # print("Links: ", links_id_to_add)
            links_to_add = links[links.id.isin(links_id_to_add)]
            print("Adding {} links.".format(links_to_add.shape[0]))
            links[links.id.isin(links_id_to_add)]["i"] = i
            candidate_links = candidate_links.append(links_to_add)
            node_list_foreign_keys = list(
                set(list(candidate_links["u"]) + list(candidate_links["v"]))
            )

            return candidate_links, node_list_foreign_keys

        def _shortest_path():
            candidate_links["weight"] = 1 + (
                candidate_links["i"] * RoadwayNetwork.SP_WEIGHT_FACTOR
            )
            candidate_nodes = self.nodes_df.loc[
                list(candidate_links["u"]) + list(candidate_links["v"])
            ]

            G = RoadwayNetwork.ox_graph(candidate_nodes, candidate_links)

            try:
                sp_route = nx.shortest_path(G, A_id, B_id, weight="weight")
                self.selections[sel_key]["candidate_links"] = candidate_links
                sp_links = candidate_links[
                    candidate_links["u"].isin(sp_route)
                    & candidate_links["v"].isin(sp_route)
                ]
                self.selections[sel_key] = {
                    "route": sp_route,
                    "links": sp_links,
                    "graph": G,
                }
                return True
            except:
                return False

        # find the node ids for the candidate links
        node_list_foreign_keys = list(candidate_links["u"]) + list(candidate_links["v"])
        i = 0

        max_i = RoadwayNetwork.SEARCH_BREADTH
        while (
            A_id not in node_list_foreign_keys
            and B_id not in node_list_foreign_keys
            and i <= max_i
        ):
            print("Adding breadth, no shortest path. i:", i, " Max i:", max_i)
            i += 1
            candidate_links, node_list_foreign_keys = _add_breadth(
                candidate_links, self.nodes_df, self.links_df, i
            )

        sp_found = _shortest_path()
        if not sp_found:
            print(
                "No shortest path found with {}, trying greater breadth until SP found".format(
                    i
                )
            )
        while not sp_found and i <= RoadwayNetwork.MAX_SEARCH_BREADTH:
            print(
                "Adding breadth, with shortest path iteration. i:", i, " Max i:", max_i
            )
            i += 1
            candidate_links, node_list_foreign_keys = _add_breadth(
                candidate_links, self.nodes_df, self.links_df, i
            )
            sp_found = _shortest_path()

        if sp_found:
            # reselect from the links in the shortest path, the ones with
            # the desired values....ignoring name.
            if len(selection["link"]) > 1:
                resel_query = ProjectCard.build_link_selection_query(
                    selection,
                    mode=modes_to_network_variables[search_mode],
                    ignore=["name"],
                )
                print("Reselecting features:\n{}".format(resel_query))
                self.selections[sel_key]["selected_links"] = self.selections[sel_key][
                    "links"
                ].query(resel_query, engine="python")
            else:
                self.selections[sel_key]["selected_links"] = self.selections[sel_key][
                    "links"
                ]

            self.selections[sel_key]["selection_found"] = True
            # Return pandas.Series of links_ids
            return self.selections[sel_key]["selected_links"].index.tolist()
        else:
            WranglerLogger.error("Couldn't find path from {} to {}".format(A_id, B_id))
            raise ValueError

    def validate_and_update_properties(
        self,
        properties: dict,
        ignore_existing: bool = False,
        require_existing_for_change: bool = False,
    ) -> bool:
        """
        If there are change or existing commands, make sure that that
        property exists in the network.

        Parameters
        -----------
        properties : dict
            properties dictionary to be evaluated
        ignore_existing: bool
            If True, will only warn about properties that specify an "existing"
            value.  If False, will fail.
        require_existing_for_change: bool
            If True, will fail if there isn't a specified value in the
            project card for existing when a change is specified.
        Returns
        -------
        boolean value as to whether the properties dictonary is valid.
        """

        validation_error_message = []

        for p in properties:
            if p["property"] not in self.links_df.columns:
                if p.get("change"):
                    validation_error_message.append(
                        '"Change" is specified for attribute {}, but doesn\'t exist in base network\n'.format(
                            p["property"]
                        )
                    )

                if p.get("existing") and not ignore_existing:
                    validation_error_message.append(
                        '"Existing" is specified for attribute {}, but doesn\'t exist in base network\n'.format(
                            p["property"]
                        )
                    )
                elif p.get("existing"):
                    WranglerLogger.warning(
                        '"Existing" is specified for attribute {}, but doesn\'t exist in base network\n'.format(
                            p["property"]
                        )
                    )

            if p.get("change") and not p.get("existing"):
                if require_existing_for_change:
                    validation_error_message.append(
                        '"Change" is specified for attribute {}, but there isn\'t a value for existing.\nTo proceed, run with the setting require_existing_for_change=False'.format(
                            p["property"]
                        )
                    )
                else:
                    WranglerLogger.warning(
                        '"Change" is specified for attribute {}, but there isn\'t a value for existing.\n'.format(
                            p["property"]
                        )
                    )

        if validation_error_message:
            WranglerLogger.error(" ".join(validation_error_message))
            raise ValueError()

    def apply(self, project_card_dictionary: dict):
        """
        Wrapper method to apply a project to a roadway network.

        args
        ------
        project_card_dictionary: dict
          a dictionary of the project card object

        """

        WranglerLogger.info(
            "Applying Project to Roadway Network: {}".format(
                project_card_dictionary["project"]
            )
        )

        def _apply_individual_change(project_dictionary: dict):

            if project_dictionary["category"].lower() == "roadway property change":
                self.apply_roadway_feature_change(
                    self.select_roadway_features(project_dictionary["facility"]),
                    project_dictionary["properties"],
                )
            elif project_dictionary["category"].lower() == "parallel managed lanes":
                self.apply_managed_lane_feature_change(
                    self.select_roadway_features(project_dictionary["facility"]),
                    project_dictionary["properties"],
                )
            else:
                raise (BaseException)

        if project_card_dictionary.get("changes"):
            for project_dictionary in project_card_dictionary["changes"]:
                _apply_individual_change(project_dictionary)
        else:
            _apply_individual_change(project_card_dictionary)

    def apply_roadway_feature_change(
        self, link_idx: list, properties: dict, in_place: bool = True
    ) -> Union(None, RoadwayNetwork):
        """
        Changes the roadway attributes for the selected features based on the
        project card information passed

        args:
        link_idx : list
            lndices of all links to apply change to
        properties : list of dictionarys
            roadway properties to change
        in_place: boolean
            update self or return a new roadway network object
        """

        # check if there are change or existing commands that that property
        #   exists in the network
        # if there is a set command, add that property to network
        self.validate_and_update_properties(properties)

        for i, p in enumerate(properties):
            attribute = p["property"]

            # if project card specifies an existing value in the network
            #   check and see if the existing value in the network matches
            if p.get("existing"):
                network_values = self.links_df.loc[link_idx, attribute].tolist()
                if not set(network_values).issubset([p.get("existing")]):
                    WranglerLogger.warning(
                        "Existing value defined for {} in project card does "
                        "not match the value in the roadway network for the "
                        "selected links".format(attribute)
                    )

            if in_place:
                if "set" in p.keys():
                    self.links_df.loc[link_idx, attribute] = p["set"]
                else:
                    self.links_df.loc[link_idx, attribute] = (
                        self.links_df.loc[link_idx, attribute] + p["change"]
                    )
            else:
                if i == 0:
                    updated_network = copy.deepcopy(self)

                if "set" in p.keys():
                    updated_network.links_df.loc[link_idx, attribute] = p["set"]
                else:
                    updated_network.links_df.loc[link_idx, attribute] = (
                        updated_network.links_df.loc[link_idx, attribute] + p["change"]
                    )

                if i == len(properties) - 1:
                    return updated_network

    def apply_managed_lane_feature_change(
        self, link_idx: list, properties: dict, in_place: bool = True
    ) -> Union(None, RoadwayNetwork):
        """
        Apply the managed lane feature changes to the roadway network

        link_idx : list
            lndices of all links to apply change to
        properties : list of dictionarys
            roadway properties to change
        in_place: boolean
            update self or return a new roadway network object
        """

        for p in properties:
            attribute = p["property"]

            if "group" in p.keys():
                attr_value = {}
                attr_value["default"] = p["set"]
                attr_value["timeofday"] = []
                for g in p["group"]:
                    category = g["category"]
                    for tod in g["timeofday"]:
                        attr_value["timeofday"].append(
                            {
                                "category": category,
                                "time": parse_time_spans(tod["time"]),
                                "value": tod["set"],
                            }
                        )

            elif "timeofday" in p.keys():
                attr_value = {}
                attr_value["default"] = p["set"]
                attr_value["timeofday"] = []
                for tod in p["timeofday"]:
                    attr_value["timeofday"].append(
                        {"time": parse_time_spans(tod["time"]), "value": tod["set"]}
                    )

            elif "set" in p.keys():
                attr_value = p["set"]

            else:
                attr_value = ""

            # TODO: decide on connectors info when they are more specific in project card
            if attribute == "ML_ACCESS" and attr_value == "all":
                attr_value = 1

            if attribute == "ML_EGRESS" and attr_value == "all":
                attr_value = 1

            if in_place:
                self.links_df.loc[link_idx, attribute] = attr_value
            else:
                if i == 0:
                    updated_network = copy.deepcopy(self)

                updated_network.links_df.loc[link_idx, attribute] = attr_value

                if i == len(properties) - 1:
                    return updated_network

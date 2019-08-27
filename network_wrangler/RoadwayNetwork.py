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
from .Utils import point_df_to_geojson, link_df_to_json
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
                os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "schemas"
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
                os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "schemas"
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
                os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "schemas"
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
        for k, v in selection["link"].items():
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
                return self.selections[sel_key]["selected_links"]
        else:
            self.selections[sel_key] = {}
            self.selections[sel_key]["selection_found"] = False

        # identify candidate links which match the initial query
        # assign them as iteration = 0
        # subsequent iterations that didn't match the query will be
        # assigned a heigher weight in the shortest path
        self.selections[sel_key]["candidate_links"] = self.links_df.query(
            sel_query, engine="python"
        )
        candidate_links = self.selections[sel_key][
            "candidate_links"
        ]  # b/c too long to keep that way
        candidate_links["i"] = 0

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

        # reselect from the links in the shortest path, the ones with
        # the desired values....ignoring name.

        if len(selection["link"]) > 1:
            resel_query = ProjectCard.build_link_selection_query(
                selection, mode=modes_to_network_variables[search_mode], ignore=["name"]
            )
            print("Reselecting features:\n{}".format(resel_query))
            self.selections[sel_key]["selected_links"] = self.selections[sel_key][
                "links"
            ].query(resel_query, engine="python")
        else:
            self.selections[sel_key]["selected_links"] = self.selections[sel_key][
                "links"
            ]

        if sp_found:
            self.selections[sel_key]["selection_found"] = True
            return self.selections[sel_key]["selected_links"]
        else:
            return False

    def validate_properties(self, properties: dict) -> Bool:
        """
        Evaluate whether the properties dictionary contains the
        attributes that exists on the network

        Parameters
        -----------
        properties : dict
            properties dictionary to be evaluated

        Returns
        -------
        boolean value as to whether the properties dictonary is valid.

        """
        attr_err = []
        req_err = []

        for k, v in properties.items():
            if k not in self.links_df.columns:
                attr_err.append(
                    "{} specified as attribute to change but not an attribute in network\n".format(
                        k
                    )
                )

            # either 'set' OR 'change' should be specified, not both
            if "set" in v.keys() and "change" in v.keys():
                req_err.append(
                    "Both Set and Change should not be specified for the attribute {}\n".format(
                        k
                    )
                )

            # if 'change' is specified, then 'existing' is required
            if "change" in v.keys() and "existing" not in v.keys():
                req_err.append(
                    'Since "Change" is specified for attribute {}, "Existing" value is also required\n'.format(
                        k
                    )
                )

        if attr_err:
            WranglerLogger.error(
                "ERROR: Properties to change in project card not found in network"
            )
            WranglerLogger.error("\n".join(attr_err))
            raise ValueError()
            return False

        if req_err:
            WranglerLogger.error(
                "ERROR: Properties not specified correctly in the project card"
            )
            WranglerLogger.error("\n".join(req_err))
            raise ValueError()
            return False

        return True

    def apply_roadway_feature_change(self, properties: dict) -> RoadwayNetwork:
        """
        Changes the roadway attributes for the selected features based on the project card information passed

        args:
        properties: dictionary with roadway properties to change

        returns:
        updated roadway network
        """

        self.validate_properties(properties)

        # if the input network does not have selected links flag
        if "selected_links" not in self.links_df.columns:
            WranglerLogger.error("ERROR: selected_links flag not found in the network")
            raise ValueError()
            return False

        # shallow (copy.copy(self)) doesn't work as it will still use the references to links_df etc from the original net
        updated_network = copy.deepcopy(self)

        for attribute, values in properties.items():
            existing_value = None

            if "existing" in values.keys():
                existing_value = values["existing"]

                # if existing value in project card is not same in the network
                network_values = updated_network.links_df[
                    updated_network.links_df["selected_links"] == 1
                ][attribute].tolist()
                if not set(network_values).issubset([existing_value]):
                    WranglerLogger.warning(
                        "WARNING: Existing value defined for {} in project card does not match the value in the roadway network for the selected links".format(
                            attribute
                        )
                    )

            if "set" in values.keys():
                build_value = values["set"]
            else:
                build_value = values["existing"] + values["change"]

            updated_network.links_df[attribute] = np.where(
                updated_network.links_df["selected_links"] == 1,
                build_value,
                updated_network.links_df[attribute],
            )

        updated_network.links_df.drop(["selected_links"], axis=1, inplace=True)

        return updated_network

    def add_roadway_attributes(self, properties: dict) -> RoadwayNetwork:
        """
        Add the new attributes to the roadway network

        args:
        properties: dictionary with roadway properties to add

        returns:
        new roadway network
        """

        updated_network = copy.deepcopy(self)

        for attribute, values in properties.items():

            if "existing" in values.keys():
                WranglerLogger.warn("WARNING: Properties not specified correctly in the project card")
                WranglerLogger.warn("'Existing' should not be defined for a new attribute to add!")

            if "set" not in values.keys():
                WranglerLogger.error("ERROR: Properties not specified correctly in the project card")
                WranglerLogger.error("'Set' should be specified for the new attibutes!")
                raise ValueError()
                return False
            else:
                value = values["set"]

                if 'selected_links' in self.links_df.columns:
                    # if the input network has a selected_links flags to indicate selection set
                    updated_network.links_df[attribute] = np.where(
                        updated_network.links_df['selected_links'] == 1,
                        value,
                        ""
                    )
                else:
                    # else add the attribute to all the links
                    updated_network.links_df[attribute] = value

        return updated_network

    def add_managed_lane_connectors(self, connectors: dict) -> RoadwayNetwork:
        """
        Method to specifiy access/egress connectors to managed lane facility
        Simple 'all' for now, update later for more detailed access/egress

        args:
        properties: dictionary with connectors information

        returns:
        new roadway network
        """

        updated_network = copy.deepcopy(self)

        if "ML_Access" not in connectors.keys():
            WranglerLogger.error("Access connectors not defined for managed lane facility")
            raise ValueError()
            return False

        if "ML_Egress" not in connectors.keys():
            WranglerLogger.error("Egress connectors not defined for managed lane facility")
            raise ValueError()
            return False

        if connectors['ML_Access'] == 'all':
            if 'selected_links' in self.links_df.columns:
                # if the input network has a selected_links flags to indicate selection set
                updated_network.links_df['ML_Access'] = np.where(updated_network.links_df['selected_links'] == 1, 1, "")
            else:
                # else add the attribute to all the links
                updated_network.links_df['ML_Access'] = 1

        if connectors['ML_Egress'] == 'all':
            if 'selected_links' in self.links_df.columns:
                # if the input network has a selected_links flags to indicate selection set
                updated_network.links_df['ML_Egress'] = np.where(updated_network.links_df['selected_links'] == 1, 1, "")
            else:
                # else add the attribute to all the links
                updated_network.links_df['ML_Egress'] = 1

        return updated_network

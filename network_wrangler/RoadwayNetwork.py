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
from .Utils import point_df_to_geojson,link_df_to_json
from .ProjectCard import ProjectCard

class RoadwayNetwork(object):
    '''
    Representation of a Roadway Network.
    '''

    CRS = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'
    NODE_FOREIGN_KEY = 'osmNodeId'
    SEARCH_BREADTH = 5
    MAX_SEARCH_BREADTH = 10
    SP_WEIGHT_FACTOR = 100
    SELECTION_REQUIRES = ['A','B','link']

    def __init__(self, nodes: GeoDataFrame, links: DataFrame, shapes: GeoDataFrame):
        '''
        Constructor
        '''

        if not RoadwayNetwork.validate_object_types(nodes, links, shapes):
            sys.exit("RoadwayNetwork: Invalid constructor data type")

        self.nodes_df  = nodes
        self.links_df  = links
        self.shapes_df = shapes

        self.selections = {}

    @staticmethod
    def read(link_file: str, node_file: str, shape_file: str, fast: bool = False) -> RoadwayNetwork:
        '''
        Reads a network from the roadway network standard
        Validates that it conforms to the schema

        args:
        link_file: full path to the link file
        node_file: full path to the node file
        shape_file: full path to the shape file
        fast: boolean that will skip validation to speed up read time
        '''
        if not fast:
            if not ( \
                RoadwayNetwork.validate_node_schema(node_file) and \
                RoadwayNetwork.validate_link_schema(link_file) and \
                RoadwayNetwork.validate_shape_schema(shape_file) \
                ):

                sys.exit("RoadwayNetwork: Data doesn't conform to schema")

        with open(link_file) as f:
            link_json = json.load(f)

        link_properties = pd.DataFrame(link_json['features'])
        link_geometries = [  LineString( [g["locationReferences"][0]["point"],g["locationReferences"][1]["point"]]) for g in link_json["features"]  ]
        links_df  = gpd.GeoDataFrame(link_properties, geometry=link_geometries)

        shapes_df = gpd.read_file(shape_file)

        # geopandas uses fiona OGR drivers, which doesn't let you have
        # a list as a property type. Therefore, must read in node_properties
        # separately in a vanilla dataframe and then convert to geopandas

        with open(node_file) as f:
            node_geojson = json.load(f)

        node_properties = pd.DataFrame([g['properties'] for g in node_geojson['features']])
        node_geometries = [Point(g['geometry']['coordinates']) for g in node_geojson['features']]

        nodes_df = gpd.GeoDataFrame(node_properties, geometry=node_geometries)

        nodes_df.gdf_name = 'network_nodes'

        nodes_df.set_index(RoadwayNetwork.NODE_FOREIGN_KEY, inplace = True)
        nodes_df.crs = RoadwayNetwork.CRS
        nodes_df['x'] = nodes_df['geometry'].apply(lambda g: g.x)
        nodes_df['y'] = nodes_df['geometry'].apply(lambda g: g.y)
        ## todo: flatten json

        WranglerLogger.info('Read %s links from %s' % (links_df.size, link_file))
        WranglerLogger.info('Read %s nodes from %s' % (nodes_df.size, node_file))
        WranglerLogger.info('Read %s shapes from %s' % (shapes_df.size, shape_file))

        roadway_network = RoadwayNetwork(nodes = nodes_df, links = links_df, shapes = shapes_df)

        return roadway_network

    def write(self, filename: str, path: str = '.') -> None:
        '''
        Writes a network in the roadway network standard

        args:
        path: the path were the output will be saved
        filename: the name prefix of the roadway files that will be generated
        '''

        if not os.path.exists(path):
            WranglerLogger.debug("\nPath [%s] doesn't exist; creating." % path)
            os.mkdir(path)

        links_file = os.path.join(path, filename + "_link.json")
        link_property_columns = self.links_df.columns.values.tolist()
        link_property_columns.remove('geometry')
        links_json = link_df_to_json(self.links_df, link_property_columns)
        with open(links_file,'w') as f:
            json.dump(links_json, f)

        nodes_file = os.path.join(path, filename + "_node.geojson")
        # geopandas wont let you write to geojson because
        # it uses fiona, which doesn't accept a list as one of the properties
        # so need to convert the df to geojson manually first
        property_columns = self.nodes_df.columns.values.tolist()
        property_columns.remove('geometry')
        nodes_geojson = point_df_to_geojson(self.nodes_df, property_columns )
        with open(nodes_file,'w') as f:
            json.dump(nodes_geojson, f)

        shapes_file = os.path.join(path, filename + "_shape.geojson")
        self.shapes_df.to_file(shapes_file, driver='GeoJSON')

    @staticmethod
    def validate_object_types(nodes: GeoDataFrame, links: DataFrame, shapes: GeoDataFrame):
        '''
        Determines if the roadway network is being built with the right object types.
        Returns: boolean

        Does not validate schemas.
        '''

        errors = ''

        if not isinstance(nodes, GeoDataFrame):
            error_message = "Incompatible nodes type:{}. Must provide a GeoDataFrame.  ".format(type(nodes))
            WranglerLogger.error(error_message)
            errors.append(error_message)
        if not isinstance(links, GeoDataFrame):
            error_message = "Incompatible links type:{}. Must provide a GeoDataFrame.  ".format(type(links))
            WranglerLogger.error(error_message)
            errors.append(error_message)
        if not isinstance(shapes, GeoDataFrame):
            error_message = "Incompatible shapes type:{}. Must provide a GeoDataFrame.  ".format(type(shapes))
            WranglerLogger.error(error_message)
            errors.append(error_message)

        if errors: return False
        return True

    @staticmethod
    def validate_node_schema(node_file, schema_location: str = 'roadway_network_node.json'):
        '''
        Validate roadway network data node schema and output a boolean
        '''
        if not os.path.exists(schema_location):
            base_path    = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'schemas')
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
            WranglerLogger.error(excmessage)

        except SchemaError as exc:
            WranglerLogger.error("Failed Node schema validation: Schema Error")
            WranglerLogger.error("Node Schema Loc:{}".format(schema_location))
            WranglerLogger.error(exc.message)

        return False

    @staticmethod
    def validate_link_schema(link_file, schema_location: str = 'roadway_network_link.json'):
        '''
        Validate roadway network data link schema and output a boolean
        '''

        if not os.path.exists(schema_location):
            base_path    = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'schemas')
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
    def validate_shape_schema(shape_file, schema_location: str = 'roadway_network_shape.json'):
        '''
        Validate roadway network data shape schema and output a boolean
        '''

        if not os.path.exists(schema_location):
            base_path    = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'schemas')
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

    def validate_selection(self, selection:dict) -> Bool:
        if not set(RoadwayNetwork.SELECTION_REQUIRES).issubset(selection):
            err_msg = 'Project Card Selection requires: {}'.format(",".join(RoadwayNetwork.SELECTION_REQUIRES))
            err_msg +=', but selection only contains: {}'.format(",".join(selection))
            WranglerLogger.error(err_msg)
            raise KeyError(err_msg)

        err = []

        for d in selection['link']:
            for k,v in d.items():
                if k not in self.links_df.columns:
                    err.append('{} specified in link selection but not an attribute in network\n'.format(k))
        for k,v in selection['A'].items():
            if k not in self.nodes_df.columns and k != RoadwayNetwork.NODE_FOREIGN_KEY:
                err.append('{} specified in A node selection but not an attribute in network\n'.format(k))
        for k,v in selection['B'].items():
            if k not in self.nodes_df.columns and k != RoadwayNetwork.NODE_FOREIGN_KEY:
                err.append('{} specified in B node selection but not an attribute in network\n'.format(k))
        if err:
            WranglerLogger.error("ERROR: Selection variables in project card not found in network")
            WranglerLogger.error("\n".join(err))
            WranglerLogger.error("--existing node columns:{}".format(" ".join(self.nodes_df.columns)))
            WranglerLogger.error("--existing link columns:{}".format(" ".join(self.links_df.columns)))
            raise ValueError()
            return False
        else:
            return True

    def orig_dest_nodes_foreign_key(self, selection: dict, node_foreign_key: str = '') -> tuple:
        '''
        returns the foreign key id for the AB nodes as a tuple
        '''

        if not node_foreign_key:
            node_foreign_key = RoadwayNetwork.NODE_FOREIGN_KEY
        if len(selection['A'])>1:
            raise ("Selection A node dictionary should be of length 1")
        if len(selection['B'])>1:
            raise ("Selection B node dictionary should be of length 1")

        A_node_key, A_id = next(iter(selection['A'].items()))
        B_node_key, B_id = next(iter(selection['B'].items()))

        if A_node_key != node_foreign_key:
            A_id = self.nodes_df[self.nodes_df[A_node_key]==A_id][node_foreign_key].values[0]
        if B_node_key != node_foreign_key:
            B_id = self.nodes_df[self.nodes_df[B_node_key]==B_id][node_foreign_key].values[0]

        return (A_id, B_id)

    @staticmethod
    def ox_graph(nodes_df, links_df):
        '''
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
        '''

        try:
            graph_nodes = nodes_df.drop(['inboundReferenceId', 'outboundReferenceId'], axis=1)
        except:
            graph_nodes = nodes_df

        graph_nodes.gdf_name = "network_nodes"

        G = ox.gdfs_to_graph(graph_nodes,links_df)

        return G

    def build_selection_key(self,selection_dict):
        sel_query = ProjectCard.build_link_selection_query(selection_dict)

        A_id, B_id = self.orig_dest_nodes_foreign_key(selection_dict)
        sel_key = (sel_query, A_id, B_id)

        return sel_key

    def select_roadway_features(self, selection: dict) -> RoadwayNetwork:
        '''
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

        args:
        card_dict: dictionary with facility information
        '''

        self.validate_selection(selection)

        sel_query = ProjectCard.build_link_selection_query(selection)

        A_id, B_id = self.orig_dest_nodes_foreign_key(selection)
        sel_key = (sel_query, A_id, B_id)

        if sel_key in self.selections:
            if 'route' in self.selections[sel_key]:
                # we have to use the string version of the query b/c dicts can't use dicts as keys
                return self.selections[sel_key]['route']
        else:
            self.selections[sel_key]={}

        self.selections[sel_key]['candidate_links'] = self.links_df.query(sel_query, engine='python')
        candidate_links = self.selections[sel_key]['candidate_links'] #b/c too long to keep that way
        candidate_links['i'] = 0
        node_list_foreign_keys = list(candidate_links['u']) + list(candidate_links['v'])

        def add_breadth(candidate_links, nodes, links, i):
            '''
            add outbound and inbound reference IDs from existing nodes
            '''
            print("-Adding Breadth-")
            node_list_foreign_keys = list(set(list(candidate_links['u']) + list(candidate_links['v'])))
            candidate_nodes = nodes.loc[node_list_foreign_keys]
            print("Candidate Nodes: {}".format(len(candidate_nodes)))
            links_id_to_add = list(
                                  set(
                                       sum(candidate_nodes['outboundReferenceId'].tolist(),[]) \
                                       + sum(candidate_nodes['inboundReferenceId'].tolist(),[]) \
                                      )\
                                       - set(candidate_links['id'].tolist()) \
                                       - set(['']) \
                                    )
            print("Link IDs to add: {}".format(len(links_id_to_add)))
            #print("Links: ", links_id_to_add)
            links_to_add = links[links.id.isin(links_id_to_add)]
            print("Adding {} links.".format(links_to_add.shape[0]))
            links_to_add['i'] = i
            candidate_links = candidate_links.append(links_to_add)
            node_list_foreign_keys = list(set(list(candidate_links['u']) + list(candidate_links['v'])))

            return candidate_links, node_list_foreign_keys

        def shortest_path():
            candidate_links['weight'] = candidate_links['i']+(candidate_links['i']*RoadwayNetwork.SP_WEIGHT_FACTOR)
            candidate_nodes = self.nodes_df.loc[list(candidate_links['u']) + list(candidate_links['v'])]

            G = RoadwayNetwork.ox_graph(candidate_nodes, candidate_links)

            try:
                sp_route = nx.shortest_path(G, A_id, B_id, weight = 'weight')
                self.selections[sel_key]['candidate_links'] = candidate_links
                sp_links = candidate_links[candidate_links['u'].isin(sp_route) & candidate_links['v'].isin(sp_route)]
                self.selections[sel_key] = {'route':sp_route,'links':sp_links, 'graph':G}
                return True
            except:
                return False

        i = 0

        max_i = RoadwayNetwork.SEARCH_BREADTH
        while A_id not in node_list_foreign_keys and B_id not in node_list_foreign_keys and i <= max_i:
           print("Adding breadth, no shortest path. i:",i, " Max i:", max_i)
           i += 1
           candidate_links, node_list_foreign_keys = add_breadth(candidate_links, self.nodes_df, self.links_df, i)

        sp_found = shortest_path()
        print("No shortest path found with {}, trying greater breadth until SP found".format(i))
        while not sp_found and i <= RoadwayNetwork.MAX_SEARCH_BREADTH:
            print("Adding breadth, with shortest path iteration. i:",i, " Max i:", max_i)
            i += 1
            candidate_links, node_list_foreign_keys = add_breadth(candidate_links, self.nodes_df, self.links_df, i)
            sp_found = shortest_path()

        if sp_found:
            return self.selections[sel_key]['route']
        else:
            return False

    def apply_roadway_feature_change(net: RoadwayNetwork, properties_dict: dict) -> bool:
        '''
        Changes the roadway attributes for the selected features based on the project card information passed

        args:
        net: RoadwayNetwork with selected links flag
        properties_dict: dictionary with roadway properties to change

        returns:
        bool: True if successful.
        '''

        roadway_network = copy.copy(net)
        error = False
        for d in properties_dict:
            for attribute, value in d.items():
                if isinstance(value, list):
                    existing_value = value[0]          # set to fail, if existing value is not same to start with
                    build_value = value[1]             # account for -/+ sign later
                else:
                    build_value = value                # account for -/+ sign later

                # check if the attribute to be updated exists on the network links
                if attribute not in list(roadway_network.links_df.columns):
                    WranglerLogger.error('%s is not an valid network attribute!' % (attribute))
                    error = True
                else:
                    roadway_network.links_df[attribute] = np.where(roadway_network.links_df['sel_links'] == 1, build_value, roadway_network.links_df[attribute])


        roadway_network.links_df.drop(['sel_links'], axis = 1, inplace = True)

        return error, roadway_network

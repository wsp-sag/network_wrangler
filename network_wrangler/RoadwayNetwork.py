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
import numpy as np
from pandas.core.frame import DataFrame

from geopandas.geodataframe import GeoDataFrame

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError

from shapely.geometry import Point, LineString

from .Logger import WranglerLogger
from .Utils import point_df_to_geojson,link_df_to_json

class RoadwayNetwork(object):
    '''
    Representation of a Roadway Network.
    '''

    def __init__(self, nodes: GeoDataFrame, links: DataFrame, shapes: GeoDataFrame):
        '''
        Constructor
        '''

        if not RoadwayNetwork.validate_object_types(nodes, links, shapes):
            sys.exit("RoadwayNetwork: Invalid constructor data type")

        self.nodes_df  = nodes
        self.links_df  = links
        self.shapes_df = shapes

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

    def select_roadway_features(self, card_dict: dict) -> RoadwayNetwork:
        '''
        Selects roadway features that satisfy selection criteria and
        return another RoadwayNetwork object with selected links flag

        args:
        card_dict: dictionary with project card information
        '''

        roadway_network = copy.copy(self)

        # build selection query
        sel_query = ''
        count = 1
        for d in card_dict['facility']['link']:
            for key, value in d.items():
                if isinstance(value, list):
                    sel_query = sel_query + '('
                    v = 1
                    for i in value:   # building an OR query with each element in list
                        sel_query = sel_query + key + '.str.contains("' + i + '")'
                        if v!= len(value):
                            sel_query = sel_query + ' or '
                        v = v + 1
                    sel_query = sel_query + ')'
                else:
                    sel_query = sel_query + key + ' == ' + '"' + str(value) + '"'

                if count != len(card_dict['facility']['link']):
                    sel_query = sel_query + ' and '

                count = count + 1

        #print(sel_query)

        sel_data = roadway_network.links_df.query(sel_query, engine='python')
        sel_indices = sel_data.index.tolist()

        roadway_network.links_df['sel_links'] = np.where(roadway_network.links_df.index.isin(sel_indices), 1, 0)
        return roadway_network

    def apply_roadway_feature_change(net: RoadwayNetwork, card_dict: dict) -> bool:
        '''
        Changes the roadway attributes for the selected features based to the project card information passed

        args:
        net: RoadwayNetwork with selected links flag
        card_dict: dictionary with project card information

        returns:
        bool: True if successful.
        '''

        for d in card_dict['properties']:
            for attribute, value in d.items():
                if isinstance(value, list):
                    existing_value = value[0]          # set to fail, if existing value is not same to start with
                    build_value = value[1]             # account for -/+ sign later
                else:
                    build_value = value                # account for -/+ sign later

                # check if the attribute to be updated exists on the network links
                if attribute not in list(net.links_df.columns):
                    WranglerLogger.error('%s is not an valid network attribute!' % (attribute))
                    return False
                else:
                    net.links_df[attribute] = np.where(net.links_df['sel_links'] == 1, build_value, net.links_df[attribute])


        net.links_df.drop(['sel_links'], axis = 1, inplace = True)

        return True

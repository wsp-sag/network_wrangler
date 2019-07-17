#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import os, sys

import yaml
import pandas as pd
import geojson
import geopandas as gpd
import json
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

    def select_roadway_features(self, selection: list(str or dict)) -> RoadwayNetwork:
        '''
        Selects roadway features that satisfy selection criteria and
        return another RoadwayNetwork object.

        Example usage:
           net.select_roadway_links(
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
        '''
        ##TODO just a placeholder

        pass

    def apply_roadway_feature_change(self, card_dict: dict) -> bool:
        '''
        Changes the road network according to the project card information passed

        args:
        card_dict: dictionary with project card information

        returns:
        bool: True if successful.
        '''

        road_dict = card_dict.get("Road")
        road_id = road_dict.get("Name").split("=")[1]

        attribute = card_dict.get("Attribute").upper()
        change_dict = card_dict.get("Change")
        existing_value = change_dict.get("Existing")
        build_value = change_dict.get("Build")

        # identify the network link with same id as project card road id
        # check if the attribute to be updated exists on the network links
        # get the current attribute value for the link from the network
        # check for current attribute value to be same as defined in the project card
        # update the link attribute value

        # TODO:
        # change desired logger level for the different checks

        found = False
        for link in self.links_df['features']:
            if link['id'] == road_id:
                found = True

                # if projetc card attribute to be updated is not found on the network link
                if attribute not in link.keys():
                    WranglerLogger.error('%s is not an valid network attribute!' % (attribute))
                    return False
                else:
                    attr_value = link[attribute]

                    # if network attribute value is not same as existing value info from project card
                    # log it but still update the attribute
                    if attr_value != existing_value:
                        WranglerLogger.warn('Current value for %s is not same as existing value defined in the project card!' % (attribute))

                    # update to build value
                    link[attribute] = build_value
                    break

        # if link with project card id is not found in the network
        if not found:
            WranglerLogger.warn('Project card link with id %s not found in the network!'% (road_id))

        return True

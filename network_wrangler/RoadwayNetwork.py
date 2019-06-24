#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import os, sys

import yaml
import pandas as pd
import geopandas as gpd

from pandas.core.frame import DataFrame
import geojson
from geopandas.geodataframe import GeoDataFrame
import json
from shapely.geometry import Point

from .Logger import WranglerLogger
from .Utils import point_df_to_geojson

class RoadwayNetwork(object):
    '''
    Representation of a Roadway Network.
    '''

    def __init__(self, nodes: GeoDataFrame, links: DataFrame, shapes: GeoDataFrame):
        '''
        Constructor
        '''

        errors = []

        if isinstance(nodes, GeoDataFrame):
            self.nodes_df = nodes
        else:
            error_message = "Incompatible nodes type. Must provide a GeoDataFrame."
            WranglerLogger.error(error_message)
            errors.append(error_message)

        if isinstance(links, DataFrame):
            self.links_df = links
        else:
            error_message = "Incompatible links type. Must provide a DataFrame."
            WranglerLogger.error(error_message)
            errors.append(error_message)

        if isinstance(shapes, GeoDataFrame):
            self.shapes_df = shapes
        else:
            error_message = "Incompatible shapes type. Must provide a GeoDataFrame."
            WranglerLogger.error(error_message)
            errors.append(error_message)

        if len(errors) > 0:
            sys.exit("RoadwayNetwork: Invalid constructor data type")


    @staticmethod
    def read(link_file: str, node_file: str, shape_file: str) -> RoadwayNetwork:
        '''
        Reads a network from the roadway network standard

        args:
        link_file: full path to the link file
        node_file: full path to the node file
        shape_file: full path to the shape file
        '''

        links_df  = pd.read_json(link_file)

        # geopandas uses fiona OGR drivers, which doesn't let you have
        # a list as a property type. Therefore, must read in node_properties
        # separately in a vanilla dataframe and then convert to geopandas

        with open(node_file) as f:
            node_geojson = json.load(f)

        node_properties = pd.DataFrame([g['properties'] for g in node_geojson['features']])
        node_geometries = [Point(g['geometry']['coordinates']) for g in node_geojson['features']]

        nodes_df = gpd.GeoDataFrame(node_properties, geometry=node_geometries)

        shapes_df = gpd.read_file(shape_file)

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
        self.links_df.to_json(path_or_buf = links_file, orient = 'records', lines = True)

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

    def apply_roadway_feauture_change(self, card_dict: dict) -> bool:
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

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



    def apply_roadway_feauture_change(self, project_card: str) -> bool:
        '''
        Changes the road network according to the project card passed
        
        args:
        project_card: the path to the project card
        
        returns:
        bool: True if successful.
        '''
        #import pdb; pdb.set_trace()
        with open(project_card, 'r') as card:
            card_dict = yaml.safe_load(card)
            
            road_dict = card_dict.get("Road")            
            name_str = road_dict.get(list(road_dict)[0])
            id = name_str.split('=')[1]
            
            change_dict = card_dict.get("Change")
            existing_value = change_dict.get(list(change_dict)[0])
            build_value = change_dict.get(list(change_dict)[1])
            
            attribute = card_dict.get("Attribute")
            
            print(id)
            print(attribute)
            print(existing_value)
            print(build_value)
            
            
            #check existing value that it matches the attribute field in self.links_df
            #TODO: how do i manipulate the data in the dataframe to apply the changes?
            
            self.links_df
            
        return True

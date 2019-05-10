#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import os, sys

import pandas as pd
import geopandas as gpd

from pandas.core.frame import DataFrame
from geopandas.geodataframe import GeoDataFrame

from Logger import WranglerLogger


class RoadwayNetwork(object):
    '''
    Representation of a Roadway Network.
    '''


    def __init__(self, nodes: GeoDataFrame, links: DataFrame, shapes: GeoDataFrame):
        '''
        Constructor
        '''
        
        if isinstance(nodes, DataFrame):
            self.nodes = nodes
        else:
            error_message = "Incompatible nodes type. Must provide a GeoDataFrame."
            WranglerLogger.error(error_message)
            sys.exit(error_message)
        
        if isinstance(links, DataFrame):  
            self.links = links
        else:
            error_message = "Incompatible links type. Must provide a DataFrame."
            WranglerLogger.error(error_message)
            sys.exit(error_message)
        
        if isinstance(shapes, DataFrame): 
            self.shapes = shapes
        else:
            error_message = "Incompatible shapes type. Must provide a GeoDataFrame."
            WranglerLogger.error(error_message)
            sys.exit(error_message)
    
    
    
    def read(self, link_file: str, node_file: str, shape_file: str) -> RoadwayNetwork:
        '''
        Reads a network from the roadway network standard
        
        args:
        link_file: full path to the link file
        node_file: full path to the node file
        shape_file: full path to the shape file 
        '''
            
        links_df = pd.read_json(link_file)
        nodes_df = gpd.read_file(node_file)
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
           
        links_file = os.path.join(path, filename + "_links.json")
        self.links_df.to_json(path_or_buf = links_file, orient = 'records', lines = True)
            
        nodes_file = os.path.join(path, filename + "_nodes.geojson")
        self.nodes_df.to_file(nodes_file, driver='GeoJSON')
        
        shapes_file = os.path.join(path, filename + "_shapes.geojson")
        self.shapes_df.to_file(shapes_file, driver='GeoJSON')
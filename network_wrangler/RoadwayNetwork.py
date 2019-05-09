#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import os

import pandas as pd
import geopandas as gpd

from pandas.core.frame import DataFrame

from Logger import WranglerLogger


class RoadwayNetwork(object):
    '''
    Representation of a Roadway Network.
    '''


    def __init__(self, nodes: DataFrame, links: DataFrame, shapes: DataFrame):
        '''
        Constructor
        '''
        
        if isinstance(nodes, DataFrame):
            self.nodes = nodes
        else:
            WranglerLogger.error("Incompatible nodes type. Must provide a DataFrame.")
        
        if isinstance(links, DataFrame):  
            self.links = links
        else:
            WranglerLogger.error("Incompatible links type. Must provide a GeoDataFrame.")
        
        if isinstance(shapes, DataFrame): 
            self.shapes = shapes
        else:
            WranglerLogger.error("Incompatible shapes type. Must provide a GeoDataFrame.")
    
    
    
    def read(self, linkFile: str, nodeFile: str, shapeFile: str) -> RoadwayNetwork:
        '''
        Reads a network from the roadway network standard
        
        args:
        linkFile: full path to the link file
        nodeFile: full path to the node file
        shapeFile: full path to the shape file 
        '''
            
        links_df = pd.read_json(linkFile)
        nodes_df = gpd.read_file(nodeFile)
        shapes_df = gpd.read_file(shapeFile)
        
        WranglerLogger.info('Read %s links from %s' % (links_df.size, linkFile))
        WranglerLogger.info('Read %s nodes from %s' % (nodes_df.size, nodeFile))
        WranglerLogger.info('Read %s shapes from %s' % (shapes_df.size, shapeFile))
        
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
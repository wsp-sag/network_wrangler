#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import pandas as pd
import geopandas as gpd

from Logger import WranglerLogger


class RoadwayNetwork(object):
    '''
    Representation of a Roadway Network.
    '''


    def __init__(self):
        '''
        Constructor
        '''
    
    def read(self, network):
        '''
        Reads a network from the roadway network standard
        
        args:
        network (not sure of type): 
        '''
            
        #df_link = pd.read_json("../example/stpaul/link.json", lines = True, orient = 'records')
        #print(df_link.head(24))
        
        
        df_node = gpd.read_file("../example/stpaul/node.geojson")
        print(df_node.head())
        df_shape = gpd.read_file("../example/stpaul/shape.geojson")
        print(df_shape.head())
        
        #TODO: now what???
    
    def write(self, path = '.', fileName = None, fileFormat = ""):
        '''
        Writes a network in the roadway network standard 
        
        args:
        path (string): the path were the output will be saved
        filename (string): the name prefix of the roadway file that is generated
        fileFormat (string): the network format that is written out
        '''
        
        #TODO: how are we writing this? Will it be one single geojson file or several like the input?
        # Will there be any json files? 
        if not os.path.exists(path):
            WranglerLogger.debug("\nPath [%s] doesn't exist; creating." % path)
            os.mkdir(path)
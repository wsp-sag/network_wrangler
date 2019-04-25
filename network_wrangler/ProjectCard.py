#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yaml

from Logger import WranglerLogger

class ProjectCard(object):
    '''
    Representation of a Project Card
    '''


    def __init__(self):
        '''
        Constructor
        '''
        pass
        
        
        
    def read(self, path_to_card):
        '''
        Reads a Project card.
        
    args:
        path_to_card (string): the path to the project card 
        
        '''
        method_lookup = {'Roadway Attribute Change': self.roadway_attribute_change, 
                         'New Roadway': self.new_roadway,
                         'Transit Service Attribute Change': self.transit_attribute_change,
                         'New Transit Dedicated Right of Way': self.new_transit_right_of_way,
                         'Parallel Managed Lanes': self.parallel_managed_lanes}
        
        with open (path_to_card, 'r') as card:
            try:
                dictionary_card = yaml.safe_load(card)
                
                try:
                    method_lookup[dictionary_card.get('Category')](dictionary_card)
                    
                except KeyError as e:
                    WranglerLogger.error(e.message())
                    raise NotImplementedError('Invalid Project Card Category') from e
                
                
            except yaml.YAMLError as exc:
                print(exc)
    
    
    
    def roadway_attribute_change(self, card):
        '''
        Reads a Roadway Attribute Change card.
        
    args:
        card (dictionary): the project card stored in a dictionary  
        
        '''
        WranglerLogger.info(card.get('Category'))
    
    
    
    def new_roadway(self, card):
        '''
        Reads a New Roadway card.
        
    args:
        card (dictionary): the project card stored in a dictionary  
        
        '''
        WranglerLogger.info(card.get('Category'))
    
    
    def transit_attribute_change(self, card):
        '''
        Reads a Transit Service Attribute Change card.
        
    args:
        card (dictionary): the project card stored in a dictionary  
        
        '''
        WranglerLogger.info(card.get('Category'))
    
    
    def new_transit_right_of_way(self, card):
        '''
        Reads a New Transit Dedicated Right of Way card.
        
    args:
        card (dictionary): the project card stored in a dictionary  
        
        '''
        WranglerLogger.info(card.get('Category'))
    
    
    def parallel_managed_lanes(self, card):
        '''
        Reads a Parallel Managed Lanes card.
        
    args:
        card (dictionary): the project card stored in a dictionary  
        
        '''
        WranglerLogger.info(card.get('Category'))
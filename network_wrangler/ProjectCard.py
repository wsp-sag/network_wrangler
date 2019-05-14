#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yaml

from Logger import WranglerLogger

class ProjectCard(object):
    '''
    Representation of a Project Card
    '''


    def __init__(self, filename: str):
        '''
        Constructor
        
        args:
        filename: the full path to project card file in YML format
        '''
        
        self.dictionary = None
        
        if not filename.endswith(".yml") and  not filename.endswith(".yaml"):
            error_message = "Incompatible file extension for Project Card. Must provide a YML file"
            WranglerLogger.error(error_message)
            return None
        
        if not self.validate(filename):
            return None
        
        with open (filename, 'r') as file:
            try:
                self.dictionary = yaml.safe_load(file)
            except yaml.YAMLError as exc:
                print(exc)
    
    
    
    def validate(self, filename: str) -> bool:
        '''
        Validates a project card.
        
        args:
        filename: the full path of the YML file
        '''
        return True
    
    
    
    def get_tags(self):
        '''
        Returns the project card's 'Tags' field
        '''
        if self.dictionary != None:
            return self.dictionary.get('Tags')
        
        return None
    
    
    
    def read(self, path_to_card: str):
        '''
        Reads a Project card.
        
        args:
        path_to_card: the full path to the project card in YML format
        
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
    
    
    
    def roadway_attribute_change(self, card: dict):
        '''
        Reads a Roadway Attribute Change card.
        
        args:
        card: the project card stored in a dictionary  
        
        '''
        WranglerLogger.info(card.get('Category'))
    
    
    
    def new_roadway(self, card: dict):
        '''
        Reads a New Roadway card.
        
        args:
        card: the project card stored in a dictionary  
        
        '''
        WranglerLogger.info(card.get('Category'))
    
    
    def transit_attribute_change(self, card: dict):
        '''
        Reads a Transit Service Attribute Change card.
        
        args:
        card: the project card stored in a dictionary  
        
        '''
        WranglerLogger.info(card.get('Category'))
    
    
    def new_transit_right_of_way(self, card: dict):
        '''
        Reads a New Transit Dedicated Right of Way card.
        
        args:
        card: the project card stored in a dictionary  
        
        '''
        WranglerLogger.info(card.get('Category'))
    
    
    def parallel_managed_lanes(self, card: dict):
        '''
        Reads a Parallel Managed Lanes card.
        
        args:
        card: the project card stored in a dictionary  
        
        '''
        WranglerLogger.info(card.get('Category'))
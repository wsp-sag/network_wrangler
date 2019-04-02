'''
Created on Apr 2, 2019

@author: chryssac
'''

import yaml

class ProjectCard(object):
    '''
    Representation of a Project Card
    '''


    def __init__(self):
        '''
        Constructor
        '''
        
        
        
    def read(self, path_to_card):
        with open (path_to_card, 'r') as card:
            try:
                dict_card = yaml.safe_load(card)
                print(dict_card)
                
                if dict_card.get('Category') == 'Roadway Attribute Change':
                    self.rdwy_attr_change(dict_card)
                    
                elif dict_card.get('Category') == 'New Roadway':
                    self.new_rdwy(dict_card)
                    
                elif dict_card.get('Category') == 'Transit Service Attribute Change':
                    self.trns_attr_change(dict_card)
                    
                elif dict_card.get('Category') == 'New Transit Dedicated Right of Way':
                    self.new_trans(dict_card)
                    
                elif dict_card.get('Category') == 'Parallel Managed Lanes':
                    self.paral_lanes(dict_card)
                
                
            except yaml.YAMLError as exc:
                print(exc)
    
    
    
    def rdwy_attr_change(self, card):
        '''
        Reads a Roadway Attribute Change card.
        
    args:
        card (dictionary): the project card stored in a dictionary  
        
        '''
        print(card.get('Category'))
    
    
    
    def new_rdwy(self, card):
        '''
        Reads a New Roadway card.
        
    args:
        card (dictionary): the project card stored in a dictionary  
        
        '''
        print(card.get('Category'))
    
    
    def trns_attr_change(self, card):
        '''
        Reads a Transit Service Attribute Change card.
        
    args:
        card (dictionary): the project card stored in a dictionary  
        
        '''
        print(card.get('Category'))
    
    
    def new_trans(self, card):
        '''
        Reads a New Transit Dedicated Right of Way card.
        
    args:
        card (dictionary): the project card stored in a dictionary  
        
        '''
        print(card.get('Category'))
    
    
    def paral_lanes(self, card):
        '''
        Reads a Parallel Managed Lanes card.
        
    args:
        card (dictionary): the project card stored in a dictionary  
        
        '''
        print(card.get('Category'))
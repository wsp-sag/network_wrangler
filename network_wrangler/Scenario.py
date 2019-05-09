#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import yaml

from ProjectCard import ProjectCard

class Scenario(object):
    '''
    Holds information about a scenario
    '''

    #TODO: can a scenario have more than one project card? 
    def __init__(self, base_scenario: str, project_cards: [ProjectCard]):
        '''
        Constructor
        
        args:
        base_scenario: the base scenario
        project_card: this scenario's project card
        '''
        
        self.base_scenario = base_scenario
        self.project_cards = project_cards
    
    #TODO: what will the base_scenario field be used for?
    def create_scenario(self, base_scenario: str, tags: [str], folder: str): 
        '''
        Validates project cards with a specific tag from the specified folder and 
        creates a scenario object with the valid project card.
        
        args:
        base_scenario: the base scenario
        tags: only project cards with these tags will be read/validated 
        folder: the folder location where the project cards will be
        '''
        project_cards_list = []
        
        for file in os.listdir(folder):
            if file.endswith(".yml"):
                with open (os.path.join(folder, file), 'r') as card:
                    try:
                        card_dict = yaml.safe_load(card)
                        card_tags = card_dict.get('Tags')
                    
                        if len(tags) == len(card_tags):
                            count = 0
                            
                            for tag in tags:
                                if tag in card_tags:
                                    count = count + 1
                                    if count == len(tags):
                                        #TODO validate project card.... What does "validate" mean?
                                        project_cards_list.append(ProjectCard())
                                else:
                                    break
                            
                    except yaml.YAMLError as exc:
                        print(exc)
        
        
        scenario = Scenario(base_scenario, project_cards_list)
        return scenario
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import os

from ProjectCard import ProjectCard

class Scenario(object):
    '''
    Holds information about a scenario
    '''

    def __init__(self, base_scenario: dict, project_cards: [ProjectCard] = None):
        '''
        Constructor
        
        args:
        base_scenario: the base scenario
        project_card: this scenario's project card
        '''
        
        self.base_scenario = base_scenario
        self.project_cards = project_cards
        
    
    def create_scenario(self, base_scenario: dict, folder: str, tags: [str] = None) -> Scenario: 
        '''
        Validates project cards with a specific tag from the specified folder and 
        creates a scenario object with the valid project card.
        
        args:
        base_scenario: the base scenario
        tags: only project cards with these tags will be read/validated 
        folder: the folder location where the project cards will be
        '''
        
        project_cards_list = self.add_project_cards(folder, tags, [])
        
        
        scenario = Scenario(base_scenario, project_cards_list)
        return scenario
    
    
    
    def add_project_cards(self, folder: str, tags: [str], project_cards_list: []) -> [ProjectCard]:
        '''
        Adds projects cards to a list. A folder is provided to look for project cards that have a tag matching
        the tag that is passed to the method. 
        
        args:
        tags: only project cards with these tags will be validated and added to the returning list
        folder: the folder location where the project cards will be
        project_cards_list: the list that the validated cards are added to
        '''
        
        for file in os.listdir(folder):
            if file.endswith(".yml") or file.endswith(".yaml"):
                project_card = ProjectCard(os.path.join(folder, file))
                
                if project_card != None:
                    card_tags = project_card.get_tags()
                
                if not set(tags).isdisjoint(card_tags):
                    project_cards_list.append(project_card)
        
        return project_cards_list
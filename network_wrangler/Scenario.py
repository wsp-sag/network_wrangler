#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys
from .ProjectCard import ProjectCard
from collections import OrderedDict
from .Logger import WranglerLogger

class Scenario(object):
    '''
    Holds information about a scenario
    '''

    def __init__(self, base_scenario: dict, project_cards: [ProjectCard] = None):
        '''
        Constructor

        args:
        base_scenario: the base scenario
        project_cards: this scenario's project cards
        '''

        self.base_scenario = base_scenario
        self.project_cards = project_cards

        self.prerequisites = {}
        self.corequisites  = {}
        self.conflicts     = {}

        for card in self.project_cards:
            self.prerequisites.update( {card.name : card.dependencies['prerequisite']} )
            self.corequisites.update( {card.name : card.dependencies['corequisite']} )
            self.conflicts.update( {card.name : card.dependencies['conflicts']} )

    @staticmethod
    def create_scenario(base_scenario: dict, card_directory: str = '', tags: [str] = None, project_cards_list = []) -> Scenario:
        '''
        Validates project cards with a specific tag from the specified folder or
        list of user specified project cards and
        creates a scenario object with the valid project card.

        args:
        base_scenario: the base scenario
        tags: only project cards with these tags will be read/validated
        folder: the folder location where the project cards will be
        project_cards_list: list of project cards to be applied
        '''

        scenario = Scenario(base_scenario, project_cards = project_cards_list)

        if card_directory:
            scenario.add_project_cards(card_directory, tags = tags)

        return scenario

    def add_project_cards(self, folder: str, tags: [str] = []):
        '''
        Adds projects cards to the scenario.
        A folder is provided to look for project cards that have a matching tag that is passed to the method.

        args:
        folder: the folder location where the project cards will be
        tags: only project cards with these tags will be validated and added to the returning scenario
        '''

        for file in os.listdir(folder):
            if file.endswith(".yml") or file.endswith(".yaml"):
                project_card = ProjectCard.read(os.path.join(folder, file))

                if project_card != None:
                    card_tags = project_card.tags

                    if not set(tags).isdisjoint(card_tags):
                        self.project_cards.append(project_card)
                        self.prerequisites.update( {project_card.name : project_card.dependencies['prerequisite']} )
                        self.corequisites.update( {project_card.name : project_card.dependencies['corequisite']} )
                        self.conflicts.update( {project_card.name : project_card.dependencies['conflicts']} )

    def check_scenario_conflicts(self):
        '''
        Checks if there are any conflicting projects in the scenario
        Fail if the project A specifies that project B is a conflict and project B is included in the scenario

        Returns: boolean
        '''

        conflict_dict = self.conflicts
        scenario_projects = list(conflict_dict.keys())

        error = False
        for project, conflicts in conflict_dict.items():
            if not conflicts == 'None':
                for name in conflicts:
                    if name in scenario_projects:
                        WranglerLogger.error('Projects %s has %s as conflicting project' % (project, name))
                        error = True

        if error:
            sys.exit('Conflicting project found for scenario!')

        return True

    def check_scenario_requisites(self):
        '''
        Checks if there are any missing pre- or co-requisite projects in the scenario
        Fail if the project A specifies that project B is a pre- or co-requisite and project B is not included in the scenario

        Returns: boolean
        '''

        corequisite_dict = self.corequisites
        prerequisite_dict = self.prerequisites

        scenario_projects = list(corequisite_dict.keys())

        error = False

        for project, coreq in corequisite_dict.items():
            if not coreq == 'None':
                for name in coreq:
                    if name not in scenario_projects:
                        WranglerLogger.error('Projects %s has %s as corequisite project which is missing for the scenario' % (project, name))
                        error = True

        for project, prereq in prerequisite_dict.items():
            if not prereq == 'None':
                for name in prereq:
                    if name not in scenario_projects:
                        WranglerLogger.error('Projects %s has %s as prerequisite project which is missing for the scenario' % (project, name))
                        error = True

        if error:
            sys.exit('Missing pre- or co-requisite project found for scenario!')

        return True

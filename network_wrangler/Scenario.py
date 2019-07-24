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

    def __init__(self, base_scenario: dict, project_cards: [ProjectCard] = None, prerequisite_dict: dict = None, corequisite_dict: dict = None, conflict_dict: dict = None):
        '''
        Constructor

        args:
        base_scenario: the base scenario
        project_cards: this scenario's project cards
        '''

        self.base_scenario = base_scenario
        self.project_cards = project_cards

        self.prerequisites = prerequisite_dict
        self.corequisites  = corequisite_dict
        self.conflicts     = conflict_dict

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

        prereq_dict   = {}
        coreq_dict    = {}
        conflict_dict = {}

        for project_card in project_cards_list:
            prereq_dict[project_card.name] = project_card.dependencies['prerequisite']
            coreq_dict[project_card.name] = project_card.dependencies['corequisite']
            conflict_dict[project_card.name] = project_card.dependencies['conflicts']

        scenario = Scenario(base_scenario, project_cards = project_cards_list, prerequisite_dict = prereq_dict, corequisite_dict = coreq_dict, conflict_dict = conflict_dict)

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

    def check_scenario_corequisites(self):
        '''
        Checks if there are any missing corequisite projects in the scenario
        Fail if the project A specifies that project B is a corequisite and project B is not included in the scenario

        Returns: boolean
        '''

        corequisite_dict = self.corequisites
        scenario_projects = list(corequisite_dict.keys())

        error = False
        for project, coreq in corequisite_dict.items():
            if not coreq == 'None':
                for name in coreq:
                    if name not in scenario_projects:
                        WranglerLogger.error('Projects %s has %s as corequisite project which is missing for the scenario' % (project, name))
                        error = True

        if error:
            sys.exit('Missing corequisite project found for scenario!')

        return True

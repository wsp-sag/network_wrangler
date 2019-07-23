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

    def __init__(self, base_scenario: dict, project_cards: [ProjectCard] = None, prerequisite_dict: OrderedDict = None, corequisite_dict: OrderedDict = None, conflict_dict: OrderedDict = None):
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
    def create_scenario(base_scenario: dict, card_directory: str, project_card_names = [], tags: [str] = None) -> Scenario:
        '''
        Validates project cards with a specific tag from the specified folder and
        creates a scenario object with the valid project card.

        args:
        base_scenario: the base scenario
        tags: only project cards with these tags will be read/validated
        folder: the folder location where the project cards will be
        project_cards_names: project names
        '''

        project_cards = []

        prereq_dict   = OrderedDict()
        coreq_dict    = OrderedDict()
        conflict_dict = OrderedDict()

        for project_name in project_card_names:
            card = ProjectCard.read(os.path.join(card_directory, project_name + '.yml'))
            project_cards.append(card)

            prereq_dict[project_name] = card.get_dependency('prerequisite')
            coreq_dict[project_name] = card.get_dependency('corequisite')
            conflict_dict[project_name] = card.get_dependency('conflicts')

        scenario = Scenario(base_scenario, project_cards = project_cards, prerequisite_dict = prereq_dict, corequisite_dict = coreq_dict, conflict_dict = conflict_dict)

        # if card_directory:
        #     scenario.add_project_cards(card_directory, tags = tags)

        return scenario

    def add_project_cards(self, folder: str, tags: [str] = []) -> [ProjectCard]:
        '''
        Adds projects cards to a list. A folder is provided to look for project cards that have a tag matching
        the tag that is passed to the method.

        args:
        tags: only project cards with these tags will be validated and added to the returning list
        folder: the folder location where the project cards will be
        '''

        for file in os.listdir(folder):
            if file.endswith(".yml") or file.endswith(".yaml"):
                project_card = ProjectCard(os.path.join(folder, file))

                if project_card != None:
                    card_tags = project_card.get_tags()

                    if not set(tags).isdisjoint(card_tags):
                        project_cards_list.append(project_card)

        return project_cards_list

    def check_scenario_conflicts(self):
        '''
        Checks if there are any conflicting projects in the scenario
        Fail if the project A specifies that project B is a conflict and project B is included in the scenario

        Returns: boolean
        '''

        conflict_dict = self.conflicts
        scenario_projects = list(conflict_dict.keys())

        errorFound = False
        for project, conflicts in conflict_dict.items():
            if not conflicts == 'None':
                for name in conflicts:
                    if name in scenario_projects:
                        WranglerLogger.error('Projects %s has %s as conflicting project' % (project, name))
                        errorFound = True

        if errorFound:
            sys.exit('Conflicting project found for scenario!')

        return True

#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys
from .ProjectCard import ProjectCard
from collections import OrderedDict
from .Logger import WranglerLogger
from collections import defaultdict
from .Utils import topological_sort

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

        self.requisite_checks_done = False
        self.conflicts_checks_done = False

        self.has_requisite_error = False
        self.has_conflict_error = False

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

    def __str__(self):
        return "\n"

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

    def __str__(self):
        projects = ["{}\n\tPrerequisites: {}\n\tCoRequisites: {}\n\tConflicts: {}".format(p.name, p.dependencies['prerequisite'],p.dependencies['corequisite'],p.dependencies['conflicts']) for p in self.project_cards]
        s = ["Base Scenario: {}".format(self.base_scenario)]
        s += projects
        return '\n'.join(s)

    def check_scenario_conflicts(self) -> bool:
        '''
        Checks if there are any conflicting projects in the scenario
        Fail if the project A specifies that project B is a conflict and project B is included in the scenario

        Returns: boolean indicating if the check was successful or returned an error
        '''

        conflict_dict = self.conflicts
        scenario_projects = [p.name for p in self.project_cards]

        for project, conflicts in conflict_dict.items():
            if not conflicts == 'None':
                for name in conflicts:
                    if name in scenario_projects:
                        self.project_cards
                        WranglerLogger.error('Projects %s has %s as conflicting project' % (project, name))
                        self.has_conflict_error = True

        self.conflicts_checks_done = True

        return self.has_conflict_error

    def check_scenario_requisites(self) -> bool:
        '''
        Checks if there are any missing pre- or co-requisite projects in the scenario
        Fail if the project A specifies that project B is a pre- or co-requisite and project B is not included in the scenario

        Returns: boolean indicating if the checks were successful or returned an error
        '''

        corequisite_dict = self.corequisites
        prerequisite_dict = self.prerequisites

        scenario_projects = [p.name for p in self.project_cards]

        for project, coreq in corequisite_dict.items():
            if not coreq == 'None':
                for name in coreq:
                    if name not in scenario_projects:
                        WranglerLogger.error('Projects %s has %s as corequisite project which is missing for the scenario' % (project, name))
                        self.has_requisite_error = True

        for project, prereq in prerequisite_dict.items():
            if not prereq == 'None':
                for name in prereq:
                    if name not in scenario_projects:
                        WranglerLogger.error('Projects %s has %s as prerequisite project which is missing for the scenario' % (project, name))
                        self.has_requisite_error = True

        self.requisite_checks_done = True

        return self.has_requisite_error

    def create_ordered_project_cards(self):
        '''
        create a list of project cards such that they are in order based on pre-requisites

        Returns: ordered list of project cards to be applied to scenario
        '''

        scenario_projects = [p.name for p in self.project_cards]

        # build prereq (adjacency) list for topological sort
        adjacency_list = defaultdict(list)
        visited_list = defaultdict()

        for project in scenario_projects:
            visited_list[project] = False
            if not self.prerequisites[project] == "None":
                for prereq in self.prerequisites[project]:
                    if prereq in scenario_projects:         # this will always be true, else would have been flagged in missing prerequsite check, but just in case
                        adjacency_list[prereq] = [project]

        # sorted_project_names is topological sorted project card names (based on prerequsiite)
        sorted_project_names = topological_sort(adjacency_list = adjacency_list, visited_list = visited_list)

        # get the project card objects for these sorted project names
        project_card_and_name_dict = {}
        for project_card in self.project_cards:
            project_card_and_name_dict[project_card.name] = project_card

        sorted_project_cards = [project_card_and_name_dict[project_name] for project_name in sorted_project_names]

        return sorted_project_cards

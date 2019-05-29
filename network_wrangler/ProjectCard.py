#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yaml
import json

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError
from .Logger import WranglerLogger

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

        with open (filename, 'r') as card:
            card_dict = yaml.safe_load(card)

            try:
                with open("../schemas/project_card.json") as json_file:
                    schema = json.load(json_file)

                #validate project card
                validate(card_dict, schema)
                self.dictionary = card_dict

            except ValidationError as exc:
                WranglerLogger.error(exc)

            except SchemaError as exc:
                WranglerLogger.error(exc)

            except yaml.YAMLError as exc:
                WranglerLogger.error(exc)


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
        path_to_card (string): the path to the project card
        '''
        method_lookup = {'Roadway Attribute Change': self.roadway_attribute_change,
                         'New Roadway': self.new_roadway,
                         'Transit Service Attribute Change': self.transit_attribute_change,
                         'New Transit Dedicated Right of Way': self.new_transit_right_of_way,
                         'Parallel Managed Lanes': self.parallel_managed_lanes}

        try:
            method_lookup[self.dictionary.get('Category')](self.dictionary)

        except KeyError as e:
            WranglerLogger.error(e.message())
            raise NotImplementedError('Invalid Project Card Category') from e


    def roadway_attribute_change(self, card: dict):
        '''
        Reads a Roadway Attribute Change card.

        args:
        card (dictionary): the project card stored in a dictionary
        '''
        WranglerLogger.info(card.get('Category'))



    def new_roadway(self, card: dict):
        '''
        Reads a New Roadway card.

        args:
        card (dictionary): the project card stored in a dictionary
        '''
        WranglerLogger.info(card.get('Category'))


    def transit_attribute_change(self, card: dict):
        '''
        Reads a Transit Service Attribute Change card.

        args:
        card (dictionary): the project card stored in a dictionary
        '''
        WranglerLogger.info(card.get('Category'))


    def new_transit_right_of_way(self, card: dict):
        '''
        Reads a New Transit Dedicated Right of Way card.

        args:
        card (dictionary): the project card stored in a dictionary
        '''
        WranglerLogger.info(card.get('Category'))


    def parallel_managed_lanes(self, card: dict):
        '''
        Reads a Parallel Managed Lanes card.

        args:
        card (dictionary): the project card stored in a dictionary
        '''
        WranglerLogger.info(card.get('Category'))

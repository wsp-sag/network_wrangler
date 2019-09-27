#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import yaml
import json

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError
from .Logger import WranglerLogger


class ProjectCard(object):
    """
    Representation of a Project Card
    """

    def __init__(self, attribute_dictonary: dict):
        """
        Constructor

        args:
        attribute_dictonary: a nested dictionary of attributes
        """
        self.__dict__.update(attribute_dictonary)
        self.valid = False

        ##todo more unstructuring of project card yaml

    def __str__(self):
        s = ["{}: {}".format(key, value) for key, value in self.__dict__.items()]
        return "\n".join(s)

    @staticmethod
    def read(path_to_card: str):
        """
        Reads and validates a Project card

        args:
        path_to_card: the path to the project card

        Returns a Project Card object
        """

        with open(path_to_card, "r") as cardfile:
            attribute_dictionary = yaml.safe_load(cardfile)
            attribute_dictionary["file"] = path_to_card
            card = ProjectCard(attribute_dictionary)

        card.valid = ProjectCard.validate_project_card_schema(path_to_card)

        return card

    def write(self, filename: str = None):
        """
        Writes project card dictionary to YAML file
        """
        if not filename:
            from network_wrangler.Utils import make_slug

            filename = make_slug(self.project) + ".yml"
        with open(filename, "w") as outfile:
            yaml.dump(self.__dict__, outfile, default_flow_style=False)

    @staticmethod
    def validate_project_card_schema(
        card_file, card_schema_file: str = "project_card.json"
    ) -> bool:
        """
        Tests project card schema validity by evaluating if it conforms to the schemas
        returns: boolean
        """
        if not os.path.exists(card_schema_file):
            base_path = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "schemas"
            )
            card_schema_file = os.path.join(base_path, card_schema_file)

        with open(card_schema_file) as schema_json_file:
            schema = json.load(schema_json_file)

        with open(card_file, "r") as card:
            card_json = yaml.safe_load(card)

        try:
            validate(card_json, schema)
            return True

        except ValidationError as exc:
            WranglerLogger.error("Failed Project Card validation: Validation Error")
            WranglerLogger.error("Project Card File Loc:{}".format(card_file))
            WranglerLogger.error("Project Card Schema Loc:{}".format(card_schema_file))
            WranglerLogger.error(exc.message)

        except SchemaError as exc:
            WranglerLogger.error("Failed Project Card schema validation: Schema Error")
            WranglerLogger.error("Project Card Schema Loc:{}".format(card_schema_file))
            WranglerLogger.error(exc.message)

        except yaml.YAMLError as exc:
            WranglerLogger.error(exc.message)

    @staticmethod
    def build_link_selection_query(selection: dict, mode="isDriveLink", ignore=[]):
        sel_query = "("
        count = 1

        ## i think we can remove this b/c we are validating it ahead of time
        # if 'link' not in selection.keys():
        #    return sel_query

        for l in selection["link"]:
            for key, value in l.items():
                if key in ignore:
                    count = count + 1
                    continue

                if isinstance(value, list):
                    sel_query = sel_query + "("
                    v = 1
                    for i in value:  # building an OR query with each element in list
                        if isinstance(i, str):
                            sel_query = sel_query + key + '.str.contains("' + i + '")'
                        else:
                            sel_query = sel_query + key + "==" + str(i)
                        if v != len(value):
                            sel_query = sel_query + " or "
                            v = v + 1
                    sel_query = sel_query + ")"
                else:
                    sel_query = sel_query + key + " == " + '"' + str(value) + '"'

                if count != len(selection["link"]):
                    sel_query = sel_query + " and "
                count = count + 1

        if count > (1 + len(ignore)):
            sel_query = sel_query + " and "
        sel_query = sel_query + mode + " == 1"
        sel_query = sel_query + ")"
        return sel_query

    def roadway_attribute_change(self, card: dict):
        """
        Probably delete.
        Reads a Roadway Attribute Change card.

        args:
        card: the project card stored in a dictionary
        """
        WranglerLogger.info(card.get("Category"))

    def new_roadway(self, card: dict):
        """
        Probably delete.
        Reads a New Roadway card.

        args:
        card: the project card stored in a dictionary
        """
        WranglerLogger.info(card.get("Category"))

    def transit_attribute_change(self, card: dict):
        """
        Probably delete.
        Reads a Transit Service Attribute Change card.

        args:
        card: the project card stored in a dictionary
        """
        WranglerLogger.info(card.get("Category"))

    def new_transit_right_of_way(self, card: dict):
        """
        Probably delete.
        Reads a New Transit Dedicated Right of Way card.

        args:
        card: the project card stored in a dictionary
        """
        WranglerLogger.info(card.get("Category"))

    def parallel_managed_lanes(self, card: dict):
        """
        Probably delete.
        Reads a Parallel Managed Lanes card.

        args:
        card: the project card stored in a dictionary
        """
        WranglerLogger.info(card.get("Category"))

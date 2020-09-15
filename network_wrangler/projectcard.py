#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import yaml
import json
from typing import Optional, List

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError
from .logger import WranglerLogger


class ProjectCard(object):
    """
    Representation of a Project Card

    Attributes:
        __dict__: Dictionary of project card attributes
        valid: Boolean indicating if data conforms to project card data schema
    """

    TRANSIT_CATEGORIES = ["Transit Service Property Change"]

    # categories that may affect transit, but only as a secondary
    # effect of changing roadways
    SECONDARY_TRANSIT_CATEGORIES = ["Roadway Deletion", "Parallel Managed Lanes"]

    ROADWAY_CATEGORIES = [
        "Roadway Property Change",
        "Roadway Deletion",
        "Parallel Managed lanes",
        "Add New Roadway",
        "Calculated Roadway",
    ]

    UNSPECIFIED_PROJECT_NAMES = ["", "TO DO User Define", "USER TO define"]

    def __init__(self, attribute_dictonary: dict):
        """
        Constructor

        args:
        attribute_dictonary: a nested dictionary of attributes
        """
        # add these first so they are first on write out
        self.project = None
        self.tags = ""
        self.dependencies = ""

        self.__dict__.update(attribute_dictonary)
        self.valid = False

        # todo more unstructuring of project card yaml

    def __str__(self):
        s = ["{}: {}".format(key, value) for key, value in self.__dict__.items()]
        return "\n".join(s)

    @staticmethod
    def read(path_to_card: str, validate: bool = True):
        """
        Reads and validates a Project card

        args:
        path_to_card: the path to the project card

        Returns a Project Card object
        """
        card_suffix = path_to_card.split(".")[-1].lower()

        if card_suffix in ["yaml",'yml']:
            attribute_dictionary = ProjectCard.read_yml(path_to_card)
        elif card_suffix in ["wrangler",'wr']:
            attribute_dictionary = ProjectCard.read_wrangler_card(path_to_card)
        else:
            msg = "Card should have a suffix of yaml, yml, wrangler, or wr. Found suffix: {}".format(card_suffix)
            raise ValueError(msg)

        card = ProjectCard(attribute_dictionary)

        if card.project in ProjectCard.UNSPECIFIED_PROJECT_NAMES:
            msg = "Card must have valid project name: {}".format(path_to_card)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        card.valid = False
        if validate:
            card.valid = ProjectCard.validate_project_card_schema(path_to_card)

        return card

    @staticmethod
    def read_wrangler_card(path_to_card: str) -> dict:
        """
        Reads wrangler project cards with YAML front matter and then python code.

        Args:
            path_to_card: where the project card is

        Returns: Attribute Dictionary for Project Card
        """
        WranglerLogger.debug("Reading Wrangler-Style Project Card")

        with open(path_to_card, "r") as cardfile:
            delim = cardfile.readline()
            WranglerLogger.debug("Using delimiter: {}".format(delim))
            _yaml,_pycode = cardfile.read().split(delim)
            WranglerLogger.debug("_yaml: {}\n_pycode: {}".format(_yaml, _pycode))

        attribute_dictionary = yaml.load(_yaml)
        attribute_dictionary["file"] = path_to_card
        attribute_dictionary["pycode"] = _pycode.lstrip("\n")

        return attribute_dictionary


    @staticmethod
    def read_yml(path_to_card: str) -> dict:
        """
        Reads "normal" wrangler project cards defined in YAML.

        Args:
            path_to_card: where the project card is

        Returns: Attribute Dictionary for Project Card
        """
        WranglerLogger.debug("Reading YAML-Style Project Card")

        with open(path_to_card, "r") as cardfile:
            attribute_dictionary = yaml.safe_load(cardfile)
            attribute_dictionary["file"] = path_to_card

        return attribute_dictionary

    def write(self, filename: str = None):
        """
        Writes project card dictionary to YAML file
        """
        if not filename:
            from network_wrangler.utils import make_slug

            filename = make_slug(self.project) + ".yml"

        # import collections
        # out_dict = collections.OrderedDict()
        out_dict = {}
        out_dict["project"] = None
        out_dict["tags"] = ""
        out_dict["dependencies"] = ""
        out_dict.update(self.__dict__)

        with open(filename, "w") as outfile:
            yaml.dump(out_dict, outfile, default_flow_style=False, sort_keys=False)

        WranglerLogger.info("Wrote project card to: {}".format(filename))

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
    def build_link_selection_query(
        selection: dict,
        unique_model_link_identifiers: [],
        mode: List[str] = ["drive_access"],
        ignore=[],
    ):
        sel_query = "("
        count = 0

        selection_keys = [k for l in selection["link"] for k, v in l.items()]
        num_unique_model_link_identifiers = len(
            set(unique_model_link_identifiers).intersection(selection_keys)
        )
        unique_model_link_identifer_exist = num_unique_model_link_identifiers > 0

        for l in selection["link"]:
            for key, value in l.items():

                if key in ignore:
                    continue

                if (
                    unique_model_link_identifer_exist
                    and key not in unique_model_link_identifiers
                ):
                    continue

                count = count + 1

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
                    sel_query = sel_query + key + "==" + '"' + str(value) + '"'

                if not unique_model_link_identifer_exist and count != (
                    len(selection["link"]) - len(ignore)
                ):
                    sel_query = sel_query + " and "

                if (
                    unique_model_link_identifer_exist
                    and count != num_unique_model_link_identifiers
                ):
                    sel_query = sel_query + " and "

        if not unique_model_link_identifer_exist:
            if count > 0:
                sel_query = sel_query + " and "

            # add mode query
            mode_sel = "(" + " or ".join(m + "==1" for m in mode) + ")"
            sel_query = sel_query + mode_sel

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
        Reads a Parallel Managed lanes card.

        args:
        card: the project card stored in a dictionary
        """
        WranglerLogger.info(card.get("Category"))

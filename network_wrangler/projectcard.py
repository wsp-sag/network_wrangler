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

UNSPECIFIED_PROJECT_NAMES = ["", "TO DO User Define", "USER TO define"]


class ProjectCard(object):
    """
    Representation of a Project Card

    Attributes:
        __dict__: Dictionary of project card attributes
        valid: Boolean indicating if data conforms to project card data schema
    """

    TRANSIT_CATEGORIES = ["Transit Service Property Change", "Add Transit"]

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

    def __init__(self, attribute_dictonary: dict):
        """
        Constructor for Project Card object.

        args:
            attribute_dictonary: a nested dictionary of project card attributes.
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
    def read(card_filename: str, validate: bool = True):
        """
        Reads and validates a Project card

        args:
            card_filename: The path to the project card file.
            validate: Boolean indicating if the project card should be validated. Defaults to True.

        Returns a Project Card object
        """
        card_suffix = card_filename.split(".")[-1].lower()

        if card_suffix in ["yaml", "yml"]:
            attribute_dictionary = ProjectCard.read_yml(card_filename)
        elif card_suffix in ["wrangler", "wr"]:
            attribute_dictionary = ProjectCard.read_wrangler_card(card_filename)
        else:
            msg = "Card should have a suffix of yaml, yml, wrangler, or wr. Found suffix: {}".format(
                card_suffix
            )
            raise ValueError(msg)

        card = ProjectCard(attribute_dictionary)

        if card.project in UNSPECIFIED_PROJECT_NAMES:
            msg = "Card must have valid project name: {}".format(card_filename)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        card.valid = False
        if validate:
            card.valid = ProjectCard.validate_project_card_schema(card_filename)

        return card

    @staticmethod
    def read_wrangler_card(w_card_filename: str) -> dict:
        """
        Reads wrangler project cards with YAML front matter and then python code.

        Args:
            w_card_filename: where the project card is

        Returns: Attribute Dictionary for Project Card
        """
        WranglerLogger.debug("Reading Wrangler-Style Project Card")

        with open(w_card_filename, "r") as cardfile:
            delim = cardfile.readline()
            WranglerLogger.debug("Using delimiter: {}".format(delim))
            _yaml, _pycode = cardfile.read().split(delim)
            WranglerLogger.debug("_yaml: {}\n_pycode: {}".format(_yaml, _pycode))

        attribute_dictionary = yaml.load(_yaml)
        attribute_dictionary["file"] = w_card_filename
        attribute_dictionary["pycode"] = _pycode.lstrip("\n")

        return attribute_dictionary

    @staticmethod
    def read_yml(card_filename: str) -> dict:
        """
        Reads "normal" wrangler project cards defined in YAML.

        Args:
            card_filename: file location where the project card is.

        Returns: Attribute Dictionary for Project Card
        """
        WranglerLogger.debug("Reading YAML-Style Project Card")

        with open(card_filename, "r") as cardfile:
            attribute_dictionary = yaml.safe_load(cardfile)
            attribute_dictionary["file"] = card_filename

        return attribute_dictionary

    def write(self, out_filename: str = None):
        """
        Writes project card dictionary to YAML file.

        args:
            out_filename: file location to write the project card object as yml.
                If not provided, will write to current directory using the project name as the filename.
        """
        if not out_filename:
            from network_wrangler.utils import make_slug

            out_filename = make_slug(self.project) + ".yml"

        # import collections
        # out_dict = collections.OrderedDict()
        out_dict = {}
        out_dict["project"] = None
        out_dict["tags"] = ""
        out_dict["dependencies"] = ""
        out_dict.update(self.__dict__)

        with open(out_filename, "w") as outfile:
            yaml.dump(out_dict, outfile, default_flow_style=False, sort_keys=False)

        WranglerLogger.info("Wrote project card to: {}".format(out_filename))

    @staticmethod
    def validate_project_card_schema(
        card_filename: str,
        card_schema_filename: str = "project_card.json"
    ) -> bool:
        """
        Tests project card schema validity by evaluating if it conforms to the schemas

        args:
            card_filename: location of project card .yml file
            card_schema_filename: location of project card schema to validate against. Defaults to project_card.json.

        returns: boolean
        """
        if not os.path.exists(card_schema_filename):
            base_path = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "schemas"
            )
            card_schema_filename = os.path.join(base_path, card_schema_filename)

        with open(card_schema_filename) as schema_json_file:
            schema = json.load(schema_json_file)

        with open(card_filename, "r") as card:
            card_json = yaml.safe_load(card)

        try:
            validate(card_json, schema)
            return True

        except ValidationError as exc:
            WranglerLogger.error("Failed Project Card validation: Validation Error")
            WranglerLogger.error("Project Card File Loc:{}".format(card_filename))
            WranglerLogger.error("Project Card Schema Loc:{}".format(card_schema_filename))
            WranglerLogger.error(exc.message)

        except SchemaError as exc:
            WranglerLogger.error("Failed Project Card schema validation: Schema Error")
            WranglerLogger.error("Project Card Schema Loc:{}".format(card_schema_filename))
            WranglerLogger.error(exc.message)

        except yaml.YAMLError as exc:
            WranglerLogger.error(exc.message)

    @staticmethod
    def build_link_selection_query(
        selection: dict,
        unique_link_ids: [],
        mode: List[str] = ["drive_access"],
        ignore: List[str] = [],
    ):
        """
        One line description
        #todo #239 #238

        args:
            selection:
            unique_link_ids:
            mode:
            ignore:

        returns:

        usage

        """
        sel_query = "("
        count = 0

        selection_keys = [k for l in selection["link"] for k, v in l.items()]

        unique_link_ids_sel = list( set(unique_link_ids) & set(selection_keys) )

        for l in selection["link"]:
            for key, value in l.items():

                if key in ignore:
                    continue

                if (
                    unique_link_ids_sel
                    and key not in unique_link_ids
                ):
                    continue

                count = count + 1

                if isinstance(value, list):
                    sel_query = sel_query + "("
                    v = 1
                    for i in value:
                        if isinstance(i, str): # building an OR query with each element in list
                            sel_query = sel_query + key + '.str.contains("' + i + '")'
                            if v != len(value):
                                sel_query = sel_query + " or "
                            else:
                                sel_query = sel_query + ")"
                        else:  # building an isin query with each element in list
                            if v == 1:
                                sel_query = sel_query + key + '.isin(['

                            sel_query = sel_query + str(i)

                            if v != len(value):
                                sel_query = sel_query + ","
                            else:
                                sel_query = sel_query + "]))"

                        v = v + 1
                else:
                    sel_query = sel_query + key + "==" + '"' + str(value) + '"'

                if not unique_link_ids_sel and count != (
                    len(selection["link"]) - len(ignore)
                ):
                    sel_query = sel_query + " and "

                if (
                    unique_link_ids_sel
                    and count != len(unique_link_ids_sel)
                ):
                    sel_query = sel_query + " and "

        if not unique_link_ids_sel:
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

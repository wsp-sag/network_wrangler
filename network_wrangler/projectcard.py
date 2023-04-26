#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import yaml
import json
from typing import List, Collection, Mapping, Any
from pathlib import Path
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

    FILE_TYPES = ["wr", "wrangler", "yml", "yaml"]

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

    UNSPECIFIED_PROJECT_NAMES = ["", "TO DO User Define", "USER TO define"]

    def __init__(self, attribute_dictonary: dict):
        """
        Constructor

        args:
        attribute_dictonary: a nested dictionary of attributes
        """
        # add these first so they are first on write out
        self.project = None
        self.tags = []
        self.dependencies = {}

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

        if card_suffix in ["yaml", "yml"]:
            attribute_dictionary = ProjectCard.read_yml(path_to_card)
        elif card_suffix in ["wrangler", "wr"]:
            attribute_dictionary = ProjectCard.read_wrangler_card(path_to_card)
        else:
            msg = f"Card should have a suffix of yaml, yml, wrangler, or wr. \
                Found suffix: {card_suffix}"
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
        WranglerLogger.debug(f"Reading Wrangler-Style Project Card {path_to_card}")

        with open(path_to_card, "r") as cardfile:
            delim = cardfile.readline()
            WranglerLogger.debug("Using delimiter: {}".format(delim))
            _yaml, _pycode = cardfile.read().split(delim)
            WranglerLogger.debug("_yaml: {}\n_pycode: {}".format(_yaml, _pycode))

        attribute_dictionary = yaml.safe_load(_yaml.lower())
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
        WranglerLogger.debug(f"Reading YAML-Style Project Card {path_to_card}")

        with open(path_to_card, "r") as cardfile:
            attribute_dictionary = yaml.safe_load(cardfile.read())
            attribute_dictionary["file"] = path_to_card

        return attribute_dictionary

    def write(self, filename: str = None):
        """
        Writes project card dictionary to YAML file
        """
        if not filename:
            from network_wrangler.utils.geo import make_slug

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
            msg = "Failed Project Card validation: Validation Error\n"
            msg += f"   Project Card File Loc:{card_file}\n"
            msg += f"   Project Card Schema Loc:{card_schema_file}\n"
            msg += f"      {exc.message}\n"
            WranglerLogger.error(msg)

        except SchemaError as exc:
            msg = "Failed Project Card schema validation: Schema Error"
            msg += f"   Project Card Schema Loc:{card_schema_file}"
            msg += f"      {exc.message}\n"
            WranglerLogger.error(msg)

        except yaml.YAMLError as exc:
            WranglerLogger.error(exc.message)

    def has_any_tags(self, tags: Collection[str]) -> bool:
        """Returns true if ProjectCard has at lest one tag in tags list.

        args:
            tags: list of tags to search for
        """
        if tags and set(tags).isdisjoint(self.tags):
            WranglerLogger.debug(
                f"Project card tags: {self.tags} don't match search tags: {tags}"
            )
            return False
        return True

    @staticmethod
    def build_selection_query(
        selection: Mapping[str, Any],
        type: str = "links",
        unique_ids: Collection[str] = [],
        mode: Collection[str] = ["drive_access"],
        ignore: Collection[str] = [],
    ):
        """Generates the query for selecting links within links_df.

        Args:
            selection: Selection dictionary from project card.
            type: one of "links" or "nodes"
            unique_ids: Properties which are unique in network and can be used
                for selecting individual links or nodes without other properties.
            mode: Limits selection to certain modes.
                Defaults to ["drive_access"].
            ignore: _description_. Defaults to [].

        Returns:
            _type_: _description_
        """
        sel_query = "("
        count = 0

        selection_keys = [k for li in selection[type] for k, v in li.items()]

        unique_ids_sel = list(set(unique_ids) & set(selection_keys))

        for li in selection[type]:
            for key, value in li.items():

                if key in ignore:
                    continue

                if unique_ids_sel and key not in unique_ids:
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
                elif isinstance(value, str):
                    sel_query = sel_query + key + "==" + '"' + str(value) + '"'
                else:
                    sel_query = sel_query + key + "==" + str(value)

                if not unique_ids_sel and count != (len(selection[type]) - len(ignore)):
                    sel_query = sel_query + " and "

                if unique_ids_sel and count != len(unique_ids_sel):
                    sel_query = sel_query + " and "

        if not unique_ids_sel:
            if count > 0:
                sel_query = sel_query + " and "

            # add mode query
            mode_sel = "(" + " or ".join(m + "==1" for m in mode) + ")"
            sel_query = sel_query + mode_sel

        sel_query = sel_query + ")"

        return sel_query

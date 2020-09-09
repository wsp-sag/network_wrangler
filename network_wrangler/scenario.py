#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations
import os
import sys
import glob
import copy
import pprint
from datetime import datetime
from typing import Union

import pandas as pd
import geopandas as gpd

from .projectcard import ProjectCard
from collections import OrderedDict
from .logger import WranglerLogger
from collections import defaultdict
from .utils import topological_sort
from .roadwaynetwork import RoadwayNetwork
from .transitnetwork import TransitNetwork


class Scenario(object):
    """
    Holds information about a scenario.

    .. highlight:: python

    Typical usage example:
    ::
        my_base_scenario = {
            "road_net": RoadwayNetwork.read(
                link_file=STPAUL_LINK_FILE,
                node_file=STPAUL_NODE_FILE,
                shape_file=STPAUL_SHAPE_FILE,
                fast=True,
            ),
            "transit_net": TransitNetwork.read(STPAUL_DIR),
        }

        card_filenames = [
            "3_multiple_roadway_attribute_change.yml",
            "multiple_changes.yml",
            "4_simple_managed_lane.yml",
        ]

        project_card_directory = os.path.join(STPAUL_DIR, "project_cards")

        project_cards_list = [
            ProjectCard.read(os.path.join(project_card_directory, filename), validate=False)
            for filename in card_filenames
        ]

        my_scenario = Scenario.create_scenario(
          base_scenario=my_base_scenario,
          project_cards_list=project_cards_list,
        )
        my_scenario.check_scenario_requisites()

        my_scenario.apply_all_projects()

        my_scenario.scenario_summary()

    Attributes:
        base_scenario: dictionary representation of a scenario
        project_cards (Optional): list of Project Card Instances
        road_net: instance of RoadwayNetwork for the scenario
        transit_net: instance of TransitNetwork for the scenario
        applied_projects: list of project names that have been applied
        project_cards: list of project card instances
        ordered_project_cards:
        prerequisites:  dictionary storing prerequiste information
        corequisites:  dictionary storing corequisite information
        conflicts: dictionary storing conflict information
        requisites_checked: boolean indicating if the co- and pre-requisites
            have been checked in the project cards
        conflicts_checked: boolean indicating if the project conflicts have been checked
        has_requisite_error: boolean indicating if there is a conflict in the pre- or
            co-requisites of project cards
        has_conflict_error: boolean indicating if there is are conflicting project cards
        prerequisites_sorted: boolean indicating if the project cards have
            been sorted to make sure cards that are pre-requisites are applied first
    """

    def __init__(self, base_scenario: dict, project_cards: [ProjectCard] = None):
        """
        Constructor

        args:
        base_scenario: dict the base scenario
        project_cards: list this scenario's project cards
        """

        self.road_net = None
        self.transit_net = None

        self.base_scenario = base_scenario

        # if the base scenario had roadway or transit networks, use them as the basis.
        if self.base_scenario.get("road_net"):
            self.road_net = copy.deepcopy(self.base_scenario["road_net"])
        if self.base_scenario.get("transit_net"):
            self.transit_net = copy.deepcopy(self.base_scenario["transit_net"])

        # if the base scenario had applied projects, add them to the list of applied
        self.applied_projects = []
        if self.base_scenario.get("applied_projects"):
            self.applied_projects = base_scenario["applied_projects"]

        self.project_cards = project_cards
        self.ordered_project_cards = OrderedDict()

        self.prerequisites = {}
        self.corequisites = {}
        self.conflicts = {}

        self.requisites_checked = False
        self.conflicts_checked = False

        self.has_requisite_error = False
        self.has_conflict_error = False

        self.prerequisites_sorted = False

        for card in self.project_cards:
            if not card.__dict__.get("dependencies"):
                continue

            if card.dependencies.get("prerequisites"):
                self.prerequisites.update(
                    {card.project: card.dependencies["prerequisites"]}
                )
            if card.dependencies.get("corequisites"):
                self.corequisites.update(
                    {card.project: card.dependencies["corequisites"]}
                )

    @staticmethod
    def create_base_scenario(
        base_shape_name: str,
        base_link_name: str,
        base_node_name: str,
        roadway_dir: str = "",
        transit_dir: str = "",
        validate: bool = True,
    ) -> Scenario:
        """
        args
        -----
        roadway_dir: optional
          path to the base scenario roadway network files
        base_shape_name:
          filename of the base network shape
        base_link_name:
          filename of the base network link
        base_node_name:
          filename of the base network node
        transit_dir: optional
           path to base scenario transit files
        validate:
          boolean indicating whether to validate the base network or not
        """
        if roadway_dir:
            base_network_shape_file = os.path.join(roadway_dir, base_shape_name)
            base_network_link_file = os.path.join(roadway_dir, base_link_name)
            base_network_node_file = os.path.join(roadway_dir, base_node_name)
        else:
            base_network_shape_file = base_shape_name
            base_network_link_file = base_link_name
            base_network_node_file = base_node_name

        road_net = RoadwayNetwork.read(
            link_file=base_network_link_file,
            node_file=base_network_node_file,
            shape_file=base_network_shape_file,
            fast=not validate,
        )

        if transit_dir:
            transit_net = TransitNetwork.read(transit_dir)
        else:
            transit_net = None
            WranglerLogger.info(
                "No transit directory specified, base scenario will have empty transit network."
            )

        transit_net.set_roadnet(road_net, validate_consistency=validate)
        base_scenario = {"road_net": road_net, "transit_net": transit_net}

        return base_scenario

    @staticmethod
    def create_scenario(
        base_scenario: dict = {},
        card_directory: str = "",
        tags: [str] = None,
        project_cards_list=[],
        glob_search="",
        validate_project_cards=True,
    ) -> Scenario:
        """
        Validates project cards with a specific tag from the specified folder or
        list of user specified project cards and
        creates a scenario object with the valid project card.

        args
        -----
        base_scenario:
          object dictionary for the base scenario (i.e. my_base_scenario.__dict__)
        tags:
          only project cards with these tags will be read/validated
        folder:
          the folder location where the project cards will be
        project_cards_list:
          list of project cards to be applied
        glob_search:

        """
        WranglerLogger.info("Creating Scenario")

        if project_cards_list:
            WranglerLogger.debug(
                "Adding project cards from List.\n{}".format(
                    ",".join([p.project for p in project_cards_list])
                )
            )
        scenario = Scenario(base_scenario, project_cards=project_cards_list)

        if card_directory and tags:
            WranglerLogger.debug(
                "Adding project cards from directory and tags.\nDir: {}\nTags: {}".format(
                    card_directory, ",".join(tags)
                )
            )
            scenario.add_project_cards_from_tags(
                card_directory,
                tags=tags,
                glob_search=glob_search,
                validate=validate_project_cards,
            )
        elif card_directory:
            WranglerLogger.debug(
                "Adding project cards from directory.\nDir: {}".format(card_directory)
            )
            scenario.add_project_cards_from_directory(
                card_directory, glob_search=glob_search, validate=validate_project_cards
            )
        return scenario

    def add_project_card_from_file(
        self, project_card_file: str, validate: bool = True, tags: list = []
    ):

        WranglerLogger.debug(
            "Trying to add project card from file: {}".format(project_card_file)
        )
        project_card = ProjectCard.read(project_card_file, validate=validate)

        if project_card == None:
            msg = "project card not read: {}".format(project_card_file)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        if tags and set(tags).isdisjoint(project_card.tags):
            WranglerLogger.debug(
                "Project card tags: {} don't match search tags: {}".format(
                    ",".join(project_card.tags), ",".join(tags)
                )
            )
            return

        if project_card.project in self.get_project_names():
            msg = "project card with name '{}' already in Scenario. Project names must be unique".format(
                project_card.project
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

        self.requisites_checked = False
        self.conflicts_checked = False
        self.prerequisites_sorted = False

        WranglerLogger.debug(
            "Adding project card to scenario: {}".format(project_card.project)
        )
        self.project_cards.append(project_card)

        if not project_card.__dict__.get("dependencies"):
            return

        WranglerLogger.debug("Adding project card dependencies")
        if project_card.dependencies.get("prerequisites"):
            self.prerequisites.update(
                {project_card.project: project_card.dependencies["prerequisites"]}
            )
        if project_card.dependencies.get("corequisites"):
            self.corequisites.update(
                {project_card.project: project_card.dependencies["corequisites"]}
            )
        if project_card.dependencies.get("conflicts"):
            self.conflicts.update(
                {project_card.project: project_card.dependencies["conflicts"]}
            )

    def add_project_cards_from_directory(
        self, folder: str, glob_search="", validate=True
    ):
        """
        Adds projects cards to the scenario.
        A folder is provided to look for project cards and if applicable, a glob-style search.

        i.e. glob_search = 'road*.yml'

        args:
        folder: the folder location where the project cards will be
        glob_search: https://docs.python.org/2/library/glob.html
        """

        if not os.path.exists(folder):
            msg = "Cannot find specified directory to add project cards: {}".format(
                folder
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

        if glob_search:
            WranglerLogger.debug(
                "Adding project cards using glob search: {}".format(glob_search)
            )
            for file in glob.iglob(os.path.join(folder, glob_search)):
                if not file.endswith(".yml") or file.endswith(".yaml"):
                    continue
                else:
                    self.add_project_card_from_file(file, validate=validate)
        else:
            for file in os.listdir(folder):
                if not file.endswith(".yml") or file.endswith(".yaml"):
                    continue
                else:
                    self.add_project_card_from_file(
                        os.path.join(folder, file), validate=validate
                    )

    def add_project_cards_from_tags(
        self, folder: str, tags: [str] = [], glob_search="", validate=True
    ):
        """
        Adds projects cards to the scenario.
        A folder is provided to look for project cards that have a matching tag that is passed to the method.

        args:
        folder: the folder location where the project cards will be
        tags: only project cards with these tags will be validated and added to the returning scenario
        """

        if glob_search:
            WranglerLogger.debug(
                "Adding project cards using \n-tags: {} and \nglob search: {}".format(
                    tags, glob_search
                )
            )
            for file in glob.iglob(os.path.join(folder, glob_search)):
                self.add_project_card_from_file(file, tags=tags, validate=validate)
        else:
            WranglerLogger.debug("Adding project cards using \n-tags: {}".format(tags))
            for file in os.listdir(folder):

                self.add_project_card_from_file(file, tags=tags, validate=validate)

    def __str__(self):
        s = ["{}: {}".format(key, value) for key, value in self.__dict__.items()]
        return "\n".join(s)

    def get_project_names(self) -> list:
        """
        Returns a list of project names
        """
        return [project_card.project for project_card in self.project_cards]

    def check_scenario_conflicts(self) -> bool:
        """
        Checks if there are any conflicting projects in the scenario
        Fail if the project A specifies that project B is a conflict and project B is included in the scenario

        Returns: boolean indicating if the check was successful or returned an error
        """

        conflict_dict = self.conflicts
        scenario_projects = [p.project for p in self.project_cards]

        for project, conflicts in conflict_dict.items():
            if conflicts:
                for name in conflicts:
                    if name in scenario_projects:
                        self.project_cards
                        WranglerLogger.error(
                            "Projects %s has %s as conflicting project"
                            % (project, name)
                        )
                        self.has_conflict_error = True

        self.conflicts_checked = True

        return self.has_conflict_error

    def check_scenario_requisites(self) -> bool:
        """
        Checks if there are any missing pre- or co-requisite projects in the scenario
        Fail if the project A specifies that project B is a pre- or co-requisite and project B is not included in the scenario

        Returns: boolean indicating if the checks were successful or returned an error
        """

        corequisite_dict = self.corequisites
        prerequisite_dict = self.prerequisites

        scenario_projects = [p.project for p in self.project_cards]

        for project, coreq in corequisite_dict.items():
            if coreq:
                for name in coreq:
                    if name not in scenario_projects:
                        WranglerLogger.error(
                            "Projects %s has %s as corequisite project which is missing for the scenario"
                            % (project, name)
                        )
                        self.has_requisite_error = True

        for project, prereq in prerequisite_dict.items():
            if prereq:
                for name in prereq:
                    if name not in scenario_projects:
                        WranglerLogger.error(
                            "Projects %s has %s as prerequisite project which is missing for the scenario"
                            % (project, name)
                        )
                        self.has_requisite_error = True

        self.requisites_checked = True

        return self.has_requisite_error

    def order_project_cards(self):
        """
        create a list of project cards such that they are in order based on pre-requisites

        Returns: ordered list of project cards to be applied to scenario
        """

        scenario_projects = [p.project.lower() for p in self.project_cards]

        # build prereq (adjacency) list for topological sort
        adjacency_list = defaultdict(list)
        visited_list = defaultdict()

        for project in scenario_projects:
            visited_list[project] = False
            if not self.prerequisites.get(project):
                continue
            for prereq in self.prerequisites[project]:
                if (
                    prereq.lower() in scenario_projects
                ):  # this will always be true, else would have been flagged in missing prerequsite check, but just in case
                    adjacency_list[prereq.lower()] = [project]

        # sorted_project_names is topological sorted project card names (based on prerequsiite)
        sorted_project_names = topological_sort(
            adjacency_list=adjacency_list, visited_list=visited_list
        )

        # get the project card objects for these sorted project names
        project_card_and_name_dict = {}
        for project_card in self.project_cards:
            project_card_and_name_dict[project_card.project.lower()] = project_card

        sorted_project_cards = [
            project_card_and_name_dict[project_name]
            for project_name in sorted_project_names
        ]

        try:
            assert len(sorted_project_cards) == len(self.project_cards)
        except:
            msg = "Sorted project cards ({}) are not of same number as unsorted ({}).".format(
                len(sorted_project_cards), len(self.project_cards)
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

        self.prerequisites_sorted = True
        self.ordered_project_cards = {
            project_name: project_card_and_name_dict[project_name]
            for project_name in sorted_project_names
        }

        WranglerLogger.debug(
            "Ordered Project Cards: {}".format(self.ordered_project_cards)
        )
        self.project_cards = sorted_project_cards

        WranglerLogger.debug("Project Cards: {}".format(self.project_cards))

        return sorted_project_cards

    def apply_all_projects(self):

        # Get everything in order

        if not self.requisites_checked:
            self.check_scenario_requisites()
        if not self.conflicts_checked:
            self.check_scenario_conflicts()
        if not self.prerequisites_sorted:
            self.order_project_cards()

        for p in self.project_cards:
            self.apply_project(p.__dict__)

    def apply_project(self, p):
        if isinstance(p, ProjectCard):
            p = p.__dict__

        if p.get("project"):
            WranglerLogger.info("Applying {}".format(p["project"]))

        if p.get("changes"):
            part = 1
            for pc in p["changes"]:
                pc["project"] = p["project"]
                self.apply_project(pc)

        else:
            if p["category"] in ProjectCard.ROADWAY_CATEGORIES:
                if not self.road_net:
                    raise ("Missing Roadway Network")
                self.road_net.apply(p)
            if p["category"] in ProjectCard.TRANSIT_CATEGORIES:
                if not self.transit_net:
                    raise ("Missing Transit Network")
                self.transit_net.apply(p)
            if (
                p["category"] in ProjectCard.SECONDARY_TRANSIT_CATEGORIES
                and self.transit_net
            ):
                self.transit_net.apply(p)

            if p["project"] not in self.applied_projects:
                self.applied_projects.append(p["project"])

    def remove_all_projects(self):
        self.project_cards = []

    def applied_project_card_summary(self, project_card_dictionary: dict) -> dict:
        """
        Create a summary of applied project card and what they changed for the scenario.

        Args:
            project_card_dictionary: dictionary representation of the values of a project card (i.e. ProjectCard.__dict__ )

        Returns:
            A dict of project summary change dictionaries for each change
        """
        changes = project_card_dictionary.get("changes", [project_card_dictionary])

        summary = {
            "project_card": project_card_dictionary["file"],
            "total_parts": len(changes),
        }

        def _summarize_change_roadway(change: dict, change_summary: dict):

            sel_key = RoadwayNetwork.build_selection_key(
                self.road_net, change["facility"]
            )

            change_summary["sel_idx"] = self.road_net.selections[sel_key][
                "selected_links"
            ].index.tolist()

            change_summary["attributes"] = [p["property"] for p in change["properties"]]

            if type(sel_key) == tuple:
                _, A_id, B_id = sel_key
            else:
                A_id, B_id = (None, None)

            change_summary["map"] = self.road_net.selection_map(
                change_summary["sel_idx"],
                A=A_id,
                B=B_id,
                candidate_link_idx=self.road_net.selections[sel_key]
                .get("candidate_links", pd.DataFrame([]))
                .index.tolist(),
            )
            return change_summary

        def _summarize_add_roadway(change: dict, change_summary: dict):
            change_summary["added_links"] = pd.DataFrame(change.get("links"))
            change_summary["added_nodes"] = pd.DataFrame(change.get("nodes"))
            change_summary["map"] = RoadwayNetwork.addition_map(
                self.road_net, change.get("links"), change.get("nodes"),
            )
            return change_summary

        def _summarize_deletion(change: dict, change_summary: dict):
            change_summary["deleted_links"] = change.get("links")
            change_summary["deleted_nodes"] = change.get("nodes")
            change_summary["map"] = RoadwayNetwork.deletion_map(
                self.base_scenario["road_net"],
                change.get("links"),
                change.get("nodes"),
            )
            return change_summary

        for i, change in enumerate(changes):
            WranglerLogger.debug(
                "Summarizing {} Part: {}".format(
                    project_card_dictionary["project"], i + 1
                )
            )
            change_summary = {
                "project": project_card_dictionary["project"] + " – Part " + str(i + 1),
                "category": change["category"].lower(),
            }

            if change["category"].lower() == "roadway deletion":
                change_summary = _summarize_deletion(change, change_summary)
            elif change["category"].lower() == "add new roadway":
                change_summary = _summarize_add_roadway(change, change_summary)
            elif change["category"].lower() in [
                "roadway property change",
                "parallel managed lanes",
            ]:
                change_summary = _summarize_change_roadway(change, change_summary)

            summary["Part " + str(i + 1)] = change_summary

        return summary

    def scenario_summary(
        self, project_detail: bool = True, outfile: str = "", mode: str = "a"
    ) -> str:
        """
        A high level summary of the created scenario.

        Args:
            project_detail: If True (default), will write out project card summaries.
            outfile: If specified, will write scenario summary to text file.
            mode: Outfile open mode. 'a' to append 'w' to overwrite.

        Returns:
            string of summary

        """

        WranglerLogger.info("Summarizing Scenario")
        report_str = "------------------------------\n"
        report_str += "Scenario created on {}\n".format(datetime.now())

        report_str += "Base Scenario:\n"
        report_str += "--Road Network:\n"
        report_str += "----Link File: {}\n".format(
            self.base_scenario["road_net"].link_file
        )
        report_str += "----Node File: {}\n".format(
            self.base_scenario["road_net"].node_file
        )
        report_str += "----Shape File: {}\n".format(
            self.base_scenario["road_net"].shape_file
        )
        report_str += "--Transit Network:\n"
        report_str += "----Feed Path: {}\n".format(
            self.base_scenario["transit_net"].feed_path
        )

        report_str += "\nProject Cards:\n -"
        report_str += "\n-".join(p.file for p in self.project_cards)

        report_str += "\nApplied Projects:\n-"
        report_str += "\n-".join(self.applied_projects)

        if project_detail:
            report_str += "\n---Project Card Details---\n"
            for p in self.project_cards:
                report_str += "\n{}".format(
                    pprint.pformat(self.applied_project_card_summary(p.__dict__))
                )

        if outfile:
            with open(outfile, mode) as f:
                f.write(report_str)
            WranglerLogger.info("Wrote Scenario Report to: {}".format(outfile))

        return report_str


def net_to_mapbox(
    roadway: Union[RoadwayNetwork, gpd.GeoDataFrame] = gpd.GeoDataFrame(),
    transit: Union[TransitNetwork, gpd.GeoDataFrame] = gpd.GeoDataFrame(),
    roadway_geojson: str = "roadway_shapes.geojson",
    transit_geojson: str = "transit_shapes.geojson",
    mbtiles_out: str = "network.mbtiles",
    overwrite: bool = True,
    port: str = "9000",
):
    """

    Args:
        roadway: a RoadwayNetwork instance or a geodataframe with roadway linetrings
        transit: a TransitNetwork instance or a geodataframe with transit linetrings
    """
    import subprocess

    # test for mapbox token
    try:
        token = os.getenv("MAPBOX_ACCESS_TOKEN")
    except:
        raise (
            "NEED TO SET MAPBOX ACCESS TOKEN IN ENVIRONMENT VARIABLES/n In command line: >>export MAPBOX_ACCESS_TOKEN='pk.0000.1111' # replace value with your mapbox public access token"
        )

    if type(transit) != gpd.GeoDataFrame:
        transit = TransitNetwork.transit_net_to_gdf(transit)
    if type(roadway) != gpd.GeoDataFrame:
        roadway = RoadwayNetwork.roadway_net_to_gdf(roadway)

    transit.to_file(transit_geojson, driver="GeoJSON")
    roadway.to_file(roadway_geojson, driver="GeoJSON")

    tippe_options_list = ["-zg", "-o", mbtiles_out]
    if overwrite:
        tippe_options_list.append("--force")
    # tippe_options_list.append("--drop-densest-as-needed")
    tippe_options_list.append(roadway_geojson)
    tippe_options_list.append(transit_geojson)

    try:
        WranglerLogger.info(
            "Running tippecanoe with following options: {}".format(
                " ".join(tippe_options_list)
            )
        )
        tippe_out = subprocess.run(["tippecanoe"] + tippe_options_list)
    except:
        WranglerLogger.error()
        raise (
            "If tippecanoe isn't installed, try `brew install tippecanoe` or visit https://github.com/mapbox/tippecanoe"
        )

    try:
        WranglerLogger.info(
            "Running mbview with following options: {}".format(
                " ".join(tippe_options_list)
            )
        )
        mbview_out = subprocess.run(
            ["mbview", "--port", port, ",/{}".format(mbtiles_out)]
        )
    except:
        WranglerLogger.error()
        raise (
            "If mbview isn't installed, try `npm install -g @mapbox/mbview` or visit https://github.com/mapbox/mbview"
        )

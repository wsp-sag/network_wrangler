#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations
import os
import glob
import copy
import pprint

from collections import deque, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Union, Collection

import geopandas as gpd

from projectcard import read_cards, ProjectCard

from .logger import WranglerLogger
from .roadwaynetwork import RoadwayNetwork
from .transitnetwork import TransitNetwork
from .utils import topological_sort

BASE_SCENARIO_SUGGESTED_PROPS = [
    "road_net",
    "transit_net",
    "applied_projects",
    "conflicts",
]


class ScenarioConflictError(Exception):
    pass


class ScenarioCorequisiteError(Exception):
    pass


class ScenarioPrerequisiteError(Exception):
    pass


class ProjectCardError(Exception):
    pass


class Scenario(object):
    """
    Holds information about a scenario.

    Typical usage example:

    ```python
    my_base_year_scenario = {
        "road_net": RoadwayNetwork.read(
            links_file=STPAUL_LINK_FILE,
            nodes_file=STPAUL_NODE_FILE,
            shapes_file=STPAUL_SHAPE_FILE,
        ),
        "transit_net": TransitNetwork.read(STPAUL_DIR),
    }

    # create a future baseline scenario from base by searching for all cards in dir w/ baseline tag
    project_card_directory = os.path.join(STPAUL_DIR, "project_cards")
    my_scenario = Scenario.create_scenario(
        base_scenario=my_base_year_scenario,
        card_search_dir=project_card_directory,
        filter_tags = [ "baseline2050" ]
    )

    # check project card queue and then apply the projects
    my_scenario.queued_projects
    my_scenario.apply_all_projects()

    # check applied projects, write it out, and create a summary report.
    my_scenario.applied_projects
    my_scenario.write("baseline")
    my_scenario.summarize(outfile = "baseline2050summary.txt")

    # Add some projects to create a build scenario based on a list of files.
    build_card_filenames = [
        "3_multiple_roadway_attribute_change.yml",
        "multiple_changes.yml",
        "4_simple_managed_lane.yml",
    ]
    my_scenario.add_projects_from_files(build_card_filenames)
    my_scenario.write("build2050")
    my_scenario.summarize(outfile = "build2050summary.txt")
    ```

    Attributes:
        base_scenario: dictionary representation of a scenario
        road_net: instance of RoadwayNetwork for the scenario
        transit_net: instance of TransitNetwork for the scenario
        project_cards: Mapping[ProjectCard.name,ProjectCard] Storage of all project cards by name.
        queued_projects: Projects which are "shovel ready" - have had pre-requisits checked and
            done any required re-ordering. Similar to a git staging, project cards aren't
            recognized in this collecton once they are moved to applied.
        applied_projects: list of project names that have been applied
        projects: list of all projects either planned, queued, or applied
        prerequisites:  dictionary storing prerequiste information
        corequisites:  dictionary storing corequisite information
        conflicts: dictionary storing conflict information
    """

    def __init__(
        self,
        base_scenario: Union[Scenario, dict],
        project_card_list: list[ProjectCard] = [],
        name="",
    ):
        """
        Constructor

        args:
        base_scenario: A base scenario object to base this isntance off of, or a dict which
            describes the scenario attributes including applied projects and respective conflicts.
            `{"applied_projects": [],"conflicts":{...}}`
        project_card_list: Optional list of ProjectCard instances to add to planned projects.
        """
        WranglerLogger.info("Creating Scenario")

        if type(base_scenario) == "Scenario":
            base_scenario = base_scenario.__dict__

        if not set(BASE_SCENARIO_SUGGESTED_PROPS) <= set(base_scenario.keys()):
            WranglerLogger.warning(
                f"Base_scenario doesn't contain {BASE_SCENARIO_SUGGESTED_PROPS}"
            )

        self.base_scenario = base_scenario
        self.name = name
        # if the base scenario had roadway or transit networks, use them as the basis.
        self.road_net = copy.deepcopy(self.base_scenario.get("road_net"))
        self.transit_net = copy.deepcopy(self.base_scenario.get("transit_net"))

        self.project_cards = {}
        self._planned_projects = []
        self._queued_projects = None
        self.applied_projects = self.base_scenario.get("applied_projects", [])

        self.prerequisites = self.base_scenario.get("prerequisites", {})
        self.corequisites = self.base_scenario.get("corequisites", {})
        self.conflicts = self.base_scenario.get("conflicts", {})

        for p in project_card_list:
            self._add_project(p)

    @property
    def projects(self):
        return self.applied_projects + self._planned_projects

    @property
    def queued_projects(self):
        """Returns a list version of _queued_projects queue."""
        if not self._queued_projects:
            self._check_projects_requirements_satisfied(self._planned_projects)
            self._queued_projects = self.order_projects(self._planned_projects)
        return list(self._queued_projects)

    def __str__(self):
        s = ["{}: {}".format(key, value) for key, value in self.__dict__.items()]
        return "\n".join(s)

    @staticmethod
    def create_scenario(
        base_scenario: Union["Scenario", dict] = {},
        project_card_list=[],
        project_card_filepath: Union[Collection[str], str] = None,
        filter_tags: Collection[str] = [],
        validate=True,
    ) -> Scenario:
        """
        Creates scenario from a base scenario and adds project cards.

        Project cards can be added using any/all of the following methods:
        1. List of ProjectCard instances
        2. List of ProjectCard files
        3. Directory and optional glob search to find project card files in

        Checks that a project of same name is not already in scenario.
        If selected, will validate ProjectCard before adding.
        If provided, will only add ProjectCard if it matches at least one filter_tags.

        args:
            base_scenario: base Scenario scenario instances of dictionary of attributes.
            project_card_list: List of ProjectCard instances to create Scenario from. Defaults
                to [].
            project_card_filepath: where the project card is.  A single path, list of paths,
            a directory, or a glob pattern. Defaults to None.
            filter_tags (Collection[str], optional): If used, will only add the project card if
                its tags match one or more of these filter_tags. Defaults to []
                which means no tag-filtering will occur.
            validate (bool, optional): If True, will validate the projectcard before
                being adding it to the scenario. Defaults to True.
        """

        scenario = Scenario(base_scenario)

        if project_card_filepath:
            project_card_list += list(read_cards(filter_tags=filter_tags).values())

        if project_card_list:
            scenario.add_project_cards(
                project_card_list, filter_tags=filter_tags, validate=validate
            )

        return scenario

    def _add_dependencies(self, project_name, dependencies: dict) -> None:
        """Add dependencies from a project card to relevant scenario variables.

        Updates existing "prerequisites", "corequisites" and "conflicts".
        Lowercases everything to enable string matching.

        Args:
            project_name: name of project you are adding dependencies for.
            dependencies: Dictionary of depndencies by dependency type and list of associated
                projects.
        """
        project_name = project_name.lower()
        WranglerLogger.debug(f"Adding {project_name} dependencies:\n{dependencies}")
        for d, v in dependencies.items():
            _dep = list(map(str.lower, v))
            self.__dict__[d].update({project_name: _dep})

    def _add_project(
        self,
        project_card: ProjectCard,
        validate: bool = True,
        filter_tags: Collection[str] = [],
    ) -> None:
        """Adds a single ProjectCard instances to the Scenario.

        Checks that a project of same name is not already in scenario.
        If selected, will validate ProjectCard before adding.
        If provided, will only add ProjectCard if it matches at least one filter_tags.

        Resets scenario queued_projects.

        Args:
            project_card (ProjectCard): ProjectCard instance to add to scenario.
            validate (bool, optional): If True, will validate the projectcard before
                being adding it to the scenario. Defaults to True.
            filter_tags (Collection[str], optional): If used, will only add the project card if
                its tags match one or more of these filter_tags. Defaults to []
                which means no tag-filtering will occur.

        """
        project_name = project_card.project.lower()
        filter_tags = list(map(str.lower, filter_tags))

        if project_name in self.projects:
            raise ProjectCardError(
                f"Names not unique from existing scenario projects: {project_card.project}"
            )

        if filter_tags and set(project_card.tags).isdisjoint(set(filter_tags)):
            WranglerLogger.debug(
                f"Skipping {project_name} - no overlapping tags with {filter_tags}."
            )
            return

        if validate:
            assert project_card.valid

        WranglerLogger.info(f"Adding {project_name} to scenario.")
        self.project_cards[project_name] = project_card
        self._planned_projects.append(project_name)
        self._queued_projects = None
        self._add_dependencies(project_name, project_card.dependencies)

    def add_project_cards(
        self,
        project_card_list: Collection[ProjectCard],
        validate: bool = True,
        filter_tags: Collection[str] = [],
    ) -> None:
        """Adds a list of ProjectCard instances to the Scenario.

        Checks that a project of same name is not already in scenario.
        If selected, will validate ProjectCard before adding.
        If provided, will only add ProjectCard if it matches at least one filter_tags.

        Args:
            project_card_list (Collection[ProjectCard]): List of ProjectCard instances to add to
                scenario.
            validate (bool, optional): If True, will require each ProjectCard is validated before
                being added to scenario. Defaults to True.
            filter_tags (Collection[str], optional): If used, will filter ProjectCard instances
                and only add those whose tags match one or more of these filter_tags.
                Defaults to [] - which means no tag-filtering will occur.
        """
        for p in project_card_list:
            self._add_project(p, validate=validate, filter_tags=filter_tags)

    def _check_projects_requirements_satisfied(self, project_list: Collection[str]):
        """Checks that all requirements are satisified to apply this specific set of projects
        including:

        1. has an associaed project card
        2. is in scenario's planned projects
        3. pre-requisites satisfied
        4. co-requisies satisfied by applied or co-applied projects
        5. no conflicing applied or co-applied projects

        Args:
            project_name (str): name of project.
            co_applied_project_list (Collection[str]): List of projects that will be applied
                with this project.
        """
        self._check_projects_planned(project_list)
        self._check_projects_have_project_cards(project_list)
        self._check_projects_prerequisites(project_list)
        self._check_projects_corequisites(project_list)
        self._check_projects_conflicts(project_list)

    def _check_projects_planned(self, project_names: Collection[str]) -> None:
        """Checks that a list of projects are in the scenario's planned projects."""
        _missing_ps = [
            p for p in self._planned_projects if p not in self._planned_projects
        ]
        if _missing_ps:
            raise ValueError(
                f"Projects are not in planned projects:\n {_missing_ps}. Add them by \
                using add_project_cards(), add_projects_from_files(), or \
                add_projects_from_directory()."
            )

    def _check_projects_have_project_cards(self, project_list: Collection[str]) -> bool:
        """Checks that a list of projects has an associated project card in the scenario."""
        _missing = [p for p in project_list if p not in self.project_cards]
        if _missing:
            WranglerLogger.error(
                f"Projects referenced which are missing project cards: {_missing}"
            )
            return False
        return True

    def _check_projects_prerequisites(self, project_names: str) -> None:
        """Checks that list of projects' pre-requisites have been or will be applied to scenario."""
        if set(project_names).isdisjoint(set(self.prerequisites.keys())):
            return
        _prereqs = []
        for p in project_names:
            _prereqs += self.prerequisites.get(p, [])
        _projects_applied = self.applied_projects + project_names
        _missing = list(set(_prereqs) - set(_projects_applied))
        if _missing:
            WranglerLogger.debug(
                f"project_names: {project_names}\nprojects_have_or_will_be_applied:\
                    {_projects_applied}\nmissing: {_missing}"
            )
            raise ScenarioPrerequisiteError(
                f"Missing {len(_missing)} pre-requisites: {_missing}"
            )

    def _check_projects_corequisites(self, project_names: str) -> None:
        """Checks that a list of projects' co-requisites have been or will be applied to scenario."""
        if set(project_names).isdisjoint(set(self.corequisites.keys())):
            return
        _coreqs = []
        for p in project_names:
            _coreqs += self.corequisites.get(p, [])
        _projects_applied = self.applied_projects + project_names
        _missing = list(set(_coreqs) - set(_projects_applied))
        if _missing:
            WranglerLogger.debug(
                f"project_names: {project_names}\nprojects_have_or_will_be_applied:\
                    {_projects_applied}\nmissing: {_missing}"
            )
            raise ScenarioCorequisiteError(
                f"Missing {len(_missing)} corequisites: {_missing}"
            )

    def _check_projects_conflicts(self, project_names: str) -> None:
        """Checks that list of projects' conflicts have not been or will be applied to scenario."""
        # WranglerLogger.debug("Checking Conflicts...")
        projects_to_check = project_names + self.applied_projects
        # WranglerLogger.debug(f"\nprojects_to_check:{projects_to_check}\nprojects_with_conflicts:{set(self.conflicts.keys())}")
        if set(projects_to_check).isdisjoint(set(self.conflicts.keys())):
            # WranglerLogger.debug("Projects have no conflicts to check")
            return
        _conflicts = []
        for p in project_names:
            _conflicts += self.conflicts.get(p, [])
        _conflict_problems = [p for p in _conflicts if p in projects_to_check]
        if _conflict_problems:
            WranglerLogger.warning(f"Conflict Problems: \n{_conflict_problems}")
            _conf_dict = {
                k: v
                for k, v in self.conflicts.items()
                if k in projects_to_check
                and not set(v).isdisjoint(set(_conflict_problems))
            }
            WranglerLogger.debug(f"Problematic Conflicts:\n{_conf_dict}")
            raise ScenarioConflictError(
                f"Found {len(_conflicts)} conflicts: {_conflict_problems}"
            )

    def order_projects(self, project_list: Collection[str]) -> deque:
        """
        Orders a list of projects based on moving up pre-requisites into a deque.

        args:
            project_list: list of projects to order

        Returns: deque for applying projects.
        """
        project_list = [p.lower() for p in project_list]
        assert self._check_projects_have_project_cards(project_list)

        # build prereq (adjacency) list for topological sort
        adjacency_list = defaultdict(list)
        visited_list = defaultdict()

        for project in project_list:
            visited_list[project] = False
            if not self.prerequisites.get(project):
                continue
            for prereq in self.prerequisites[project]:
                # this will always be true, else would have been flagged in missing \
                # prerequsite check, but just in case
                if prereq.lower() in project_list:
                    adjacency_list[prereq.lower()] = [project]

        # sorted_project_names is topological sorted project card names (based on prerequsiite)
        _ordered_projects = topological_sort(
            adjacency_list=adjacency_list, visited_list=visited_list
        )

        if not set(_ordered_projects) == set(project_list):
            _missing = list(set(project_list) - set(_ordered_projects))
            raise ValueError(f"Project sort resulted in missing projects:{_missing}")

        project_deque = deque(_ordered_projects)

        WranglerLogger.debug(f"Ordered Projects:\n{project_deque}")

        return project_deque

    def apply_all_projects(self):
        """Applies all planned projects in the queue."""
        # Call this to make sure projects are appropriately queued in hidden variable.
        self.queued_projects

        # Use hidden variable.
        while self._queued_projects:
            self._apply_project(self._queued_projects.popleft())

        # set this so it will trigger re-queuing any more projects.
        self._queued_projects = None

    def _apply_change(self, change: dict) -> None:
        """Applies a specific change specified in a project card.

        "category" must be in at least one of:
        - ROADWAY_CATEGORIES
        - TRANSIT_CATEGORIES

        Args:
            change (dict): dictionary of a project card change
        """
        if change["category"] in ProjectCard.ROADWAY_CATEGORIES:
            if not self.road_net:
                raise ("Missing Roadway Network")
            self.road_net.apply(change)
        if change["category"] in ProjectCard.TRANSIT_CATEGORIES:
            if not self.transit_net:
                raise ("Missing Transit Network")
            self.transit_net.apply(change)
        if (
            change["category"] in ProjectCard.SECONDARY_TRANSIT_CATEGORIES
            and self.transit_net
        ):
            self.transit_net.apply(change)

        if (
            change["category"]
            not in ProjectCard.TRANSIT_CATEGORIES + ProjectCard.ROADWAY_CATEGORIES
        ):
            raise ProjectCardError(
                f"Don't understand project category: {change['category']}"
            )

    def _apply_project(self, project_name: str) -> None:
        """Applies project card to scenario.

        If a list of changes is specified in referenced project card, iterates through each change.

        Args:
            project_name (str): name of project to be applied.
        """
        project_name = project_name.lower()

        WranglerLogger.info(f"Applying {project_name}")

        p = self.project_cards[project_name].__dict__
        if "changes" in p:
            for pc in p["changes"]:
                pc["project"] = p["project"]
                self._apply_change(pc)

        else:
            self._apply_change(p)

        self._planned_projects.remove(project_name)
        self.applied_projects.append(project_name)

    def apply_projects(self, project_list: Collection[str]):
        """
        Applies a specific list of projects from the planned project queue.

        Will order the list of projects based on pre-requisites.

        NOTE: does not check co-requisites b/c that isn't possible when applying a sin

        Args:
            project_list: List of projects to be applied. All need to be in the planned project
                queue.
        """
        project_list = [p.lower() for p in project_list]

        self._check_projects_requirements_satisfied(project_list)
        ordered_project_queue = self.order_projects(project_list)

        while ordered_project_queue:
            self._apply_project(ordered_project_queue.popleft())

        # Set so that when called again it will retrigger queueing from planned projects.
        self._ordered_projects = None

    def write(self, path: Union(Path, str), name: str) -> None:
        """_summary_

        Args:
            path: Path to write scenario networks and scenario summary to.
            name: Name to use.
        """
        self.road_net.write(path, name)
        self.transit_net.write(path, name)
        self.summarize(outfile=os.path.join(path, name))

    def summarize(
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

        WranglerLogger.info(f"Summarizing Scenario {self.name}")
        report_str = "------------------------------\n"
        report_str += f"Scenario created on {datetime.now()}\n"

        report_str += "Base Scenario:\n"
        report_str += "--Road Network:\n"
        report_str += f"----Link File: {self.base_scenario['road_net'].link_file}\n"
        report_str += f"----Node File: {self.base_scenario['road_net'].node_file}\n"
        report_str += f"----Shape File: {self.base_scenario['road_net'].shape_file}\n"
        report_str += "--Transit Network:\n"
        report_str += f"----Feed Path: {self.base_scenario['transit_net'].feed_path}\n"

        report_str += "\nProject Cards:\n -"
        report_str += "\n-".join([pc.file for p, pc in self.project_cards.items()])

        report_str += "\nApplied Projects:\n-"
        report_str += "\n-".join(self.applied_projects)

        if project_detail:
            report_str += "\n---Project Card Details---\n"
            for p in self.project_cards:
                report_str += "\n{}".format(
                    pprint.pformat(
                        [self.project_cards[p].__dict__ for p in self.applied_projects]
                    )
                )

        if outfile:
            with open(outfile, mode) as f:
                f.write(report_str)
            WranglerLogger.info(f"Wrote Scenario Report to: {outfile}")

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
        os.getenv("MAPBOX_ACCESS_TOKEN")
    except:
        raise (
            "NEED TO SET MAPBOX ACCESS TOKEN IN ENVIRONMENT VARIABLES/n \
                In command line: >>export MAPBOX_ACCESS_TOKEN='pk.0000.1111' # \
                replace value with your mapbox public access token"
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
        subprocess.run(["tippecanoe"] + tippe_options_list)
    except:
        WranglerLogger.error()
        raise (
            "If tippecanoe isn't installed, try `brew install tippecanoe` or \
                visit https://github.com/mapbox/tippecanoe"
        )

    try:
        WranglerLogger.info(
            "Running mbview with following options: {}".format(
                " ".join(tippe_options_list)
            )
        )
        subprocess.run(["mbview", "--port", port, ",/{}".format(mbtiles_out)])
    except:
        WranglerLogger.error()
        raise (
            "If mbview isn't installed, try `npm install -g @mapbox/mbview` or \
                visit https://github.com/mapbox/mbview"
        )


def project_card_files_from_directory(
    search_dir: str, glob_search=""
) -> Collection[str]:
    """Returns a list of ProjectCard instances from searching a directory.

    Args:
        search_dir (str): Search directory.
        glob_search (str, optional): Optional glob search parameters.

    Returns:
        Collection[cls]: list of ProjectCard isntances.
    """

    project_card_files = []
    if not Path(search_dir).exists():
        raise FileNotFoundError(
            "Cannot find specified directory to find project cards: {search_dir}"
        )

    if glob_search:
        WranglerLogger.debug(
            f"Finding project cards using glob search: {glob_search} in {search_dir}"
        )
        for f in glob.iglob(os.path.join(search_dir, glob_search)):
            # Path.suffix returns string starting with .
            if not Path(f).suffix[1:] in ProjectCard.FILE_TYPES:
                continue
            else:
                project_card_files.append(f)
    else:
        for f in os.listdir(search_dir):
            if not Path(f).suffix in ProjectCard.FILE_TYPES:
                continue
            else:
                project_card_files.append(f)
    return project_card_files


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
        links_file=base_network_link_file,
        nodes_file=base_network_node_file,
        shapes_file=base_network_shape_file,
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

"""Scenario class and related functions for managing a scenario.

Usage:

```python
my_base_year_scenario = {
    "road_net": load_roadway(
        links_file=STPAUL_LINK_FILE,
        nodes_file=STPAUL_NODE_FILE,
        shapes_file=STPAUL_SHAPE_FILE,
    ),
    "transit_net": load_transit(STPAUL_DIR),
}

# create a future baseline scenario from base by searching for all cards in dir w/ baseline tag
project_card_directory = os.path.join(STPAUL_DIR, "project_cards")
my_scenario = create_scenario(
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
    "road.prop_changes.segment.yml",
    "4_simple_managed_lane.yml",
]
my_scenario.add_projects_from_files(build_card_filenames)
my_scenario.write("build2050")
my_scenario.summarize(outfile = "build2050summary.txt")
```

"""

from __future__ import annotations
import os
import copy
import pprint

from collections import deque, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Union, Collection, Optional, TYPE_CHECKING


from projectcard import read_cards, ProjectCard, SubProject

from .logger import WranglerLogger
from .params import (
    BASE_SCENARIO_SUGGESTED_PROPS,
    ROADWAY_CARD_TYPES,
    SECONDARY_TRANSIT_CARD_TYPES,
    TRANSIT_CARD_TYPES,
)
from .roadway.io import load_roadway, write_roadway
from .transit.io import load_transit, write_transit
from .utils.utils import topological_sort

if TYPE_CHECKING:
    from .roadway.network import RoadwayNetwork
    from .transit.network import TransitNetwork


class ScenarioConflictError(Exception):
    """Raised when a conflict is detected."""

    pass


class ScenarioCorequisiteError(Exception):
    """Raised when a co-requisite is not satisfied."""

    pass


class ScenarioPrerequisiteError(Exception):
    """Raised when a pre-requisite is not satisfied."""

    pass


class ProjectCardError(Exception):
    """Raised when a project card is not valid."""

    pass


class Scenario(object):
    """Holds information about a scenario.

    Typical usage example:

    ```python
    my_base_year_scenario = {
        "road_net": load_roadway(
            links_file=STPAUL_LINK_FILE,
            nodes_file=STPAUL_NODE_FILE,
            shapes_file=STPAUL_SHAPE_FILE,
        ),
        "transit_net": load_transit(STPAUL_DIR),
    }

    # create a future baseline scenario from base by searching for all cards in dir w/ baseline tag
    project_card_directory = os.path.join(STPAUL_DIR, "project_cards")
    my_scenario = create_scenario(
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
        "road.prop_changes.segment.yml",
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
        """Constructor.

        Args:
        base_scenario: A base scenario object to base this isntance off of, or a dict which
            describes the scenario attributes including applied projects and respective conflicts.
            `{"applied_projects": [],"conflicts":{...}}`
        project_card_list: Optional list of ProjectCard instances to add to planned projects.
        name: Optional name for the scenario.
        """
        WranglerLogger.info("Creating Scenario")

        if isinstance(base_scenario, Scenario):
            base_scenario = base_scenario.__dict__

        if not set(BASE_SCENARIO_SUGGESTED_PROPS) <= set(base_scenario.keys()):
            WranglerLogger.warning(
                f"Base_scenario doesn't contain {BASE_SCENARIO_SUGGESTED_PROPS}"
            )

        self.base_scenario = base_scenario
        self.name = name
        # if the base scenario had roadway or transit networks, use them as the basis.
        self.road_net: Optional[RoadwayNetwork] = copy.deepcopy(self.base_scenario.get("road_net"))
        self.transit_net: Optional[TransitNetwork] = copy.deepcopy(
            self.base_scenario.get("transit_net")
        )

        self.project_cards: dict[str, ProjectCard] = {}
        self._planned_projects: list[str] = []
        self._queued_projects = None
        self.applied_projects = self.base_scenario.get("applied_projects", [])

        self.prerequisites = self.base_scenario.get("prerequisites", {})
        self.corequisites = self.base_scenario.get("corequisites", {})
        self.conflicts = self.base_scenario.get("conflicts", {})

        for p in project_card_list:
            self._add_project(p)

    @property
    def projects(self):
        """Returns a list of all projects in the scenario: applied and planned."""
        return self.applied_projects + self._planned_projects

    @property
    def queued_projects(self):
        """Returns a list version of _queued_projects queue.

        Queued projects are thos that have been planned, have all pre-requisites satisfied, and
        have been ordered based on pre-requisites.

        If no queued projects, will dynamically generate from planned projects based on
        pre-requisites and return the queue.
        """
        if not self._queued_projects:
            self._check_projects_requirements_satisfied(self._planned_projects)
            self._queued_projects = self.order_projects(self._planned_projects)
        return list(self._queued_projects)

    def __str__(self):
        """String representation of the Scenario object."""
        s = ["{}: {}".format(key, value) for key, value in self.__dict__.items()]
        return "\n".join(s)

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

        for d, v in dependencies.items():
            _dep = list(map(str.lower, v))
            WranglerLogger.debug(f"Adding {_dep} to {project_name} dependency table.")
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
        """Checks all requirements are satisified to apply this specific set of projects.

        Including:
        1. has an associaed project card
        2. is in scenario's planned projects
        3. pre-requisites satisfied
        4. co-requisies satisfied by applied or co-applied projects
        5. no conflicing applied or co-applied projects

        Args:
            project_list: list of projects to check requirements for.
        """
        self._check_projects_planned(project_list)
        self._check_projects_have_project_cards(project_list)
        self._check_projects_prerequisites(project_list)
        self._check_projects_corequisites(project_list)
        self._check_projects_conflicts(project_list)

    def _check_projects_planned(self, project_names: Collection[str]) -> None:
        """Checks that a list of projects are in the scenario's planned projects."""
        _missing_ps = [p for p in self._planned_projects if p not in self._planned_projects]
        if _missing_ps:
            raise ValueError(
                f"Projects are not in planned projects: \n {_missing_ps}. Add them by \
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

    def _check_projects_prerequisites(self, project_names: list[str]) -> None:
        """Check a list of projects' pre-requisites have been or will be applied to scenario."""
        if set(project_names).isdisjoint(set(self.prerequisites.keys())):
            return
        _prereqs = []
        for p in project_names:
            _prereqs += self.prerequisites.get(p, [])
        _projects_applied = self.applied_projects + project_names
        _missing = list(set(_prereqs) - set(_projects_applied))
        if _missing:
            WranglerLogger.debug(
                f"project_names: {project_names}\nprojects_have_or_will_be_applied: \
                    {_projects_applied}\nmissing: {_missing}"
            )
            raise ScenarioPrerequisiteError(f"Missing {len(_missing)} pre-requisites: {_missing}")

    def _check_projects_corequisites(self, project_names: list[str]) -> None:
        """Check a list of projects' co-requisites have been or will be applied to scenario."""
        if set(project_names).isdisjoint(set(self.corequisites.keys())):
            return
        _coreqs = []
        for p in project_names:
            _coreqs += self.corequisites.get(p, [])
        _projects_applied = self.applied_projects + project_names
        _missing = list(set(_coreqs) - set(_projects_applied))
        if _missing:
            WranglerLogger.debug(
                f"project_names: {project_names}\nprojects_have_or_will_be_applied: \
                    {_projects_applied}\nmissing: {_missing}"
            )
            raise ScenarioCorequisiteError(f"Missing {len(_missing)} corequisites: {_missing}")

    def _check_projects_conflicts(self, project_names: list[str]) -> None:
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
                if k in projects_to_check and not set(v).isdisjoint(set(_conflict_problems))
            }
            WranglerLogger.debug(f"Problematic Conflicts: \n{_conf_dict}")
            raise ScenarioConflictError(f"Found {len(_conflicts)} conflicts: {_conflict_problems}")

    def order_projects(self, project_list: Collection[str]) -> deque:
        """Orders a list of projects based on moving up pre-requisites into a deque.

        Args:
            project_list: list of projects to order

        Returns: deque for applying projects.
        """
        project_list = [p.lower() for p in project_list]
        assert self._check_projects_have_project_cards(project_list)

        # build prereq (adjacency) list for topological sort
        adjacency_list = defaultdict(list)
        visited_list = defaultdict(bool)

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
            raise ValueError(f"Project sort resulted in missing projects: {_missing}")

        project_deque = deque(_ordered_projects)

        WranglerLogger.debug(f"Ordered Projects: \n{project_deque}")

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

    def _apply_change(self, change: Union[ProjectCard, SubProject]) -> None:
        """Applies a specific change specified in a project card.

        Change type must be in at least one of:
        - ROADWAY_CATEGORIES
        - TRANSIT_CATEGORIES

        Args:
            change: a project card or subproject card
        """
        if change.change_type in ROADWAY_CARD_TYPES:
            if not self.road_net:
                raise ValueError("Missing Roadway Network")
            self.road_net.apply(change)
        if change.change_type in TRANSIT_CARD_TYPES:
            if not self.transit_net:
                raise ValueError("Missing Transit Network")
            self.transit_net.apply(change)
        if change.change_type in SECONDARY_TRANSIT_CARD_TYPES and self.transit_net:
            self.transit_net.apply(change)

        if change.change_type not in TRANSIT_CARD_TYPES + ROADWAY_CARD_TYPES:
            raise ProjectCardError(
                f"Project {change.project}: Don't understand project cat: {change.change_type}"
            )

    def _apply_project(self, project_name: str) -> None:
        """Applies project card to scenario.

        If a list of changes is specified in referenced project card, iterates through each change.

        Args:
            project_name (str): name of project to be applied.
        """
        project_name = project_name.lower()

        WranglerLogger.info(f"Applying {project_name}")

        p = self.project_cards[project_name]
        WranglerLogger.debug(f"types: {p.change_types}")
        WranglerLogger.debug(f"type: {p.change_type}")
        if p.sub_projects:
            for sp in p.sub_projects:
                WranglerLogger.debug(f"- applying subproject: {sp.change_type}")
                self._apply_change(sp)

        else:
            self._apply_change(p)

        self._planned_projects.remove(project_name)
        self.applied_projects.append(project_name)

    def apply_projects(self, project_list: Collection[str]):
        """Applies a specific list of projects from the planned project queue.

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

    def write(self, path: Union[Path, str], name: str) -> None:
        """_summary_.

        Args:
            path: Path to write scenario networks and scenario summary to.
            name: Name to use.
        """
        if self.road_net:
            write_roadway(self.road_net, prefix=name, out_dir=path)
        if self.transit_net:
            write_transit(self.transit_net, prefix=name, out_dir=path)
        self.summarize(outfile=os.path.join(path, name))

    def summarize(self, project_detail: bool = True, outfile: str = "", mode: str = "a") -> str:
        """A high level summary of the created scenario.

        Args:
            project_detail: If True (default), will write out project card summaries.
            outfile: If specified, will write scenario summary to text file.
            mode: Outfile open mode. 'a' to append 'w' to overwrite.

        Returns:
            string of summary

        """
        return scenario_summary(self, project_detail, outfile, mode)


def create_scenario(
    base_scenario: Union[Scenario, dict] = {},
    project_card_list=[],
    project_card_filepath: Optional[Union[Collection[str], str]] = None,
    filter_tags: Collection[str] = [],
    validate=True,
) -> Scenario:
    """Creates scenario from a base scenario and adds project cards.

    Project cards can be added using any/all of the following methods:
    1. List of ProjectCard instances
    2. List of ProjectCard files
    3. Directory and optional glob search to find project card files in

    Checks that a project of same name is not already in scenario.
    If selected, will validate ProjectCard before adding.
    If provided, will only add ProjectCard if it matches at least one filter_tags.

    Args:
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
        project_card_list += list(
            read_cards(project_card_filepath, filter_tags=filter_tags).values()
        )

    if project_card_list:
        scenario.add_project_cards(project_card_list, filter_tags=filter_tags, validate=validate)

    return scenario


def scenario_summary(
    scenario: Scenario, project_detail: bool = True, outfile: str = "", mode: str = "a"
) -> str:
    """A high level summary of the created scenario.

    Args:
        scenario: Scenario instance to summarize.
        project_detail: If True (default), will write out project card summaries.
        outfile: If specified, will write scenario summary to text file.
        mode: Outfile open mode. 'a' to append 'w' to overwrite.

    Returns:
        string of summary
    """
    WranglerLogger.info(f"Summarizing Scenario {scenario.name}")
    report_str = "------------------------------\n"
    report_str += f"Scenario created on {datetime.now()}\n"

    report_str += "Base Scenario:\n"
    report_str += "--Road Network:\n"
    report_str += f"----Link File: {scenario.base_scenario['road_net']._links_file}\n"
    report_str += f"----Node File: {scenario.base_scenario['road_net']._nodes_file}\n"
    report_str += f"----Shape File: {scenario.base_scenario['road_net']._shapes_file}\n"
    report_str += "--Transit Network:\n"
    report_str += f"----Feed Path: {scenario.base_scenario['transit_net'].feed.feed_path}\n"

    report_str += "\nProject Cards:\n -"
    report_str += "\n-".join([str(pc.file) for p, pc in scenario.project_cards.items()])

    report_str += "\nApplied Projects:\n-"
    report_str += "\n-".join(scenario.applied_projects)

    if project_detail:
        report_str += "\n---Project Card Details---\n"
        for p in scenario.project_cards:
            report_str += "\n{}".format(
                pprint.pformat(
                    [scenario.project_cards[p].__dict__ for p in scenario.applied_projects]
                )
            )

    if outfile:
        with open(outfile, mode) as f:
            f.write(report_str)
        WranglerLogger.info(f"Wrote Scenario Report to: {outfile}")

    return report_str


def create_base_scenario(
    base_shape_name: str,
    base_link_name: str,
    base_node_name: str,
    roadway_dir: str = "",
    transit_dir: str = "",
) -> dict:
    """Creates a base scenario dictionary from roadway and transit network files.

    Args:
        base_shape_name: filename of the base network shape
        base_link_name: filename of the base network link
        base_node_name: filename of the base network node
        roadway_dir: optional path to the base scenario roadway network files
        transit_dir: optional path to base scenario transit files
    """
    if roadway_dir:
        base_network_shape_file = os.path.join(roadway_dir, base_shape_name)
        base_network_link_file = os.path.join(roadway_dir, base_link_name)
        base_network_node_file = os.path.join(roadway_dir, base_node_name)
    else:
        base_network_shape_file = base_shape_name
        base_network_link_file = base_link_name
        base_network_node_file = base_node_name

    road_net = load_roadway(
        links_file=base_network_link_file,
        nodes_file=base_network_node_file,
        shapes_file=base_network_shape_file,
    )

    if transit_dir:
        transit_net = load_transit(transit_dir)
        transit_net.road_net = road_net
    else:
        transit_net = None
        WranglerLogger.info(
            "No transit directory specified, base scenario will have empty transit network."
        )

    base_scenario = {"road_net": road_net, "transit_net": transit_net}

    return base_scenario

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
```
"""

from __future__ import annotations
import copy
import pprint
import yaml

from collections import deque, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Union, Optional, TYPE_CHECKING

from projectcard import read_cards, ProjectCard, SubProject, write_card

from .logger import WranglerLogger
from .roadway.io import load_roadway_from_dir, write_roadway
from .transit.io import load_transit, write_transit
from .utils.io_table import prep_dir
from .utils.utils import topological_sort
from .configs import load_wrangler_config, WranglerConfig, load_scenario_config, ScenarioConfig
from .configs.scenario import ScenarioOutputConfig, ScenarioInputConfig
from .roadway.network import RoadwayNetwork
from .transit.network import TransitNetwork

if TYPE_CHECKING:
    from .models._base.types import RoadwayFileTypes, TransitFileTypes

"""
List of attributes that are suggested to be in a base scenario dictionary.
"""
BASE_SCENARIO_SUGGESTED_PROPS: list[str] = [
    "road_net",
    "transit_net",
    "applied_projects",
    "conflicts",
]

"""
List of card types that that will be applied to the transit network.
"""
TRANSIT_CARD_TYPES: list[str] = [
    "transit_property_change",
    "transit_routing_change",
    "transit_route_addition",
    "transit_service_deletion",
]

"""
List of card types that that will be applied to the roadway network.
"""
ROADWAY_CARD_TYPES: list[str] = [
    "roadway_property_change",
    "roadway_deletion",
    "roadway_addition",
]


"""
List of card types that that will be applied to the transit network AFTER being applied to
the roadway network.
"""
SECONDARY_TRANSIT_CARD_TYPES: list[str] = [
    "roadway_deletion",
]


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
        config: WranglerConfig instance.
    """

    def __init__(
        self,
        base_scenario: Union[Scenario, dict],
        project_card_list: Optional[list[ProjectCard]] = None,
        config: Optional[Union[WranglerConfig, dict, Path, list[Path]]] = None,
        name: str = "",
    ):
        """Constructor.

        Args:
            base_scenario: A base scenario object to base this isntance off of, or a dict which
                describes the scenario attributes including applied projects and respective
                conflicts. `{"applied_projects": [],"conflicts":{...}}`
            project_card_list: Optional list of ProjectCard instances to add to planned projects.
                Defaults to None.
            config: WranglerConfig instance or a dictionary of configuration settings or a path to
                one or more configuration files. Configurations that are not explicity set will
                default to the values in the default configuration in
                `/configs/wrangler/default.yml`.
            name: Optional name for the scenario.
        """
        WranglerLogger.info("Creating Scenario")
        self.config = load_wrangler_config(config)

        if project_card_list is None:
            project_card_list = []

        if isinstance(base_scenario, Scenario):
            base_scenario = base_scenario.__dict__

        self.base_scenario: dict = extract_base_scenario_metadata(base_scenario)

        if not set(BASE_SCENARIO_SUGGESTED_PROPS) <= set(base_scenario.keys()):
            WranglerLogger.warning(
                f"Base_scenario doesn't contain {BASE_SCENARIO_SUGGESTED_PROPS}"
            )
        self.name: str = name
        # if the base scenario had roadway or transit networks, use them as the basis.
        self.road_net: Optional[RoadwayNetwork] = copy.deepcopy(
            base_scenario.pop("road_net", None)
        )

        self.transit_net: Optional[TransitNetwork] = copy.deepcopy(
            base_scenario.pop("transit_net", None)
        )
        # Set configs for networks to be the same as scenario.
        if isinstance(self.road_net, RoadwayNetwork):
            self.road_net.config = self.config
        if isinstance(self.transit_net, TransitNetwork):
            self.transit_net.config = self.config

        self.project_cards: dict[str, ProjectCard] = {}
        self._planned_projects: list[str] = []
        self._queued_projects = None
        self.applied_projects = base_scenario.pop("applied_projects", [])

        self.prerequisites = base_scenario.pop("prerequisites", {})
        self.corequisites = base_scenario.pop("corequisites", {})
        self.conflicts = base_scenario.pop("conflicts", {})

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
        filter_tags: list[str] = [],
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
            filter_tags: If used, will only add the project card if
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
        project_card_list: list[ProjectCard],
        validate: bool = True,
        filter_tags: list[str] = [],
    ) -> None:
        """Adds a list of ProjectCard instances to the Scenario.

        Checks that a project of same name is not already in scenario.
        If selected, will validate ProjectCard before adding.
        If provided, will only add ProjectCard if it matches at least one filter_tags.

        Args:
            project_card_list: List of ProjectCard instances to add to
                scenario.
            validate (bool, optional): If True, will require each ProjectCard is validated before
                being added to scenario. Defaults to True.
            filter_tags: If used, will filter ProjectCard instances
                and only add those whose tags match one or more of these filter_tags.
                Defaults to [] - which means no tag-filtering will occur.
        """
        for p in project_card_list:
            self._add_project(p, validate=validate, filter_tags=filter_tags)

    def _check_projects_requirements_satisfied(self, project_list: list[str]):
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

    def _check_projects_planned(self, project_names: list[str]) -> None:
        """Checks that a list of projects are in the scenario's planned projects."""
        _missing_ps = [p for p in self._planned_projects if p not in self._planned_projects]
        if _missing_ps:
            raise ValueError(
                f"Projects are not in planned projects: \n {_missing_ps}. Add them by \
                using add_project_cards(), add_projects_from_files(), or \
                add_projects_from_directory()."
            )

    def _check_projects_have_project_cards(self, project_list: list[str]) -> bool:
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

    def order_projects(self, project_list: list[str]) -> deque:
        """Orders a list of projects based on moving up pre-requisites into a deque.

        Args:
            project_list: list of projects to order

        Returns: deque for applying projects.
        """
        project_list = [p.lower() for p in project_list]
        assert self._check_projects_have_project_cards(project_list)

        # build prereq (adjacency) list for topological sort
        adjacency_list: dict[str, list] = defaultdict(list)
        visited_list: dict[str, bool] = defaultdict(bool)

        for project in project_list:
            visited_list[project] = False
            if not self.prerequisites.get(project):
                continue
            for prereq in self.prerequisites[project]:
                # this will always be true, else would have been flagged in missing \
                # prerequsite check, but just in case
                if prereq.lower() in project_list:
                    if adjacency_list.get(prereq.lower()):
                        adjacency_list[prereq.lower()].append(project)
                    else:
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
        - ROADWAY_CARD_TYPES
        - TRANSIT_CARD_TYPES

        Args:
            change: a project card or subproject card
        """
        if change.change_type in ROADWAY_CARD_TYPES:
            if not self.road_net:
                raise ValueError("Missing Roadway Network")
            if change.change_type in SECONDARY_TRANSIT_CARD_TYPES and self.transit_net:
                self.road_net.apply(change, transit_net = self.transit_net)
            else:
                self.road_net.apply(change)
        if change.change_type in TRANSIT_CARD_TYPES:
            if not self.transit_net:
                raise ValueError("Missing Transit Network")
            self.transit_net.apply(change)

        if change.change_type not in ROADWAY_CARD_TYPES + TRANSIT_CARD_TYPES:
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

        WranglerLogger.info(f"Applying {project_name} from file:\
                            {self.project_cards[project_name].file}")

        p = self.project_cards[project_name]
        WranglerLogger.debug(f"types: {p.change_types}")
        WranglerLogger.debug(f"type: {p.change_type}")
        if p._sub_projects:
            for sp in p._sub_projects:
                WranglerLogger.debug(f"- applying subproject: {sp.change_type}")
                self._apply_change(sp)

        else:
            self._apply_change(p)

        self._planned_projects.remove(project_name)
        self.applied_projects.append(project_name)

    def apply_projects(self, project_list: list[str]):
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

    def write(
        self,
        path: Path,
        name: str,
        overwrite: bool = True,
        roadway_write: bool = True,
        transit_write: bool = True,
        projects_write: bool = True,
        roadway_convert_complex_link_properties_to_single_field: bool = False,
        roadway_out_dir: Optional[Path] = None,
        roadway_prefix: Optional[str] = None,
        roadway_file_format: RoadwayFileTypes = "parquet",
        roadway_true_shape: bool = False,
        transit_out_dir: Optional[Path] = None,
        transit_prefix: Optional[str] = None,
        transit_file_format: TransitFileTypes = "txt",
        projects_out_dir: Optional[Path] = None,
    ) -> None:
        """_summary_.

        Args:
            path: Path to write scenario networks and scenario summary to.
            name: Name to use.
            overwrite: If True, will overwrite the files if they already exist.
            roadway_write: If True, will write out the roadway network.
            transit_write: If True, will write out the transit network.
            projects_write: If True, will write out the project cards.
            roadway_convert_complex_link_properties_to_single_field: If True, will convert complex
                link properties to a single field.
            roadway_out_dir: Path to write the roadway network files to.
            roadway_prefix: Prefix to add to the file name.
            roadway_file_format: File format to write the roadway network to
            roadway_true_shape: If True, will write the true shape of the roadway network
            transit_out_dir: Path to write the transit network files to.
            transit_prefix: Prefix to add to the file name.
            transit_file_format: File format to write the transit network to
            projects_out_dir: Path to write the project cards to.
        """
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        if self.road_net and roadway_write:
            if roadway_out_dir is None:
                roadway_out_dir = path / "roadway"
            roadway_out_dir.mkdir(parents=True, exist_ok=True)
            write_roadway(
                net=self.road_net,
                out_dir=roadway_out_dir,
                prefix=roadway_prefix or name,
                convert_complex_link_properties_to_single_field=roadway_convert_complex_link_properties_to_single_field,
                file_format=roadway_file_format,
                true_shape=roadway_true_shape,
                overwrite=overwrite,
            )
        if self.transit_net and transit_write:
            if transit_out_dir is None:
                transit_out_dir = path / "transit"
            transit_out_dir.mkdir(parents=True, exist_ok=True)
            write_transit(
                self.transit_net,
                out_dir=transit_out_dir,
                prefix=transit_prefix or name,
                file_format=transit_file_format,
                overwrite=overwrite,
            )
        if projects_write:
            if projects_out_dir is None:
                projects_out_dir = path / "projects"
            write_applied_projects(
                self,
                out_dir=projects_out_dir,
                overwrite=overwrite,
            )

        scenario_data = self.summary
        if transit_write:
            scenario_data["transit_net"] = str(transit_out_dir)
        if roadway_write:
            scenario_data["road_net"] = str(roadway_out_dir)
        if projects_write:
            scenario_data["project_cards"] = str(projects_out_dir)
        with open(Path(path) / f"{name}_scenario.yml", "w") as f:
            yaml.dump(scenario_data, f)

    @property
    def summary(
        self,
        skip: list[str] = [
            "road_net",
            "transit_net",
            "project_cards",
        ],
    ) -> dict:
        """A high level summary of the created scenario and public attributes."""
        summary_dict = {
            k: v for k, v in self.__dict__.items() if not k.startswith("_") and k not in skip
        }
        if isinstance(summary_dict.get("config"), WranglerConfig):
            summary_dict["config"] = summary_dict["config"].to_dict()
        return summary_dict


def create_scenario(
    base_scenario: Union[Scenario, dict] = {},
    project_card_list=None,
    project_card_filepath: Optional[Union[list[Path], Path]] = None,
    filter_tags: list[str] = [],
    config: Optional[Union[dict, Path, list[Path], WranglerConfig]] = None,
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
        filter_tags: If used, will only add the project card if
            its tags match one or more of these filter_tags. Defaults to []
            which means no tag-filtering will occur.
        config: Optional wrangler configuration file or dictionary or instance. Defaults to
            default config.
    """
    if project_card_list is None:
        project_card_list = []
    
    scenario = Scenario(base_scenario, config=config)

    if project_card_filepath:
        project_card_list += list(
            read_cards(project_card_filepath, filter_tags=filter_tags).values()
        )

    if project_card_list:
        scenario.add_project_cards(project_card_list, filter_tags=filter_tags)

    return scenario


def write_applied_projects(scenario: Scenario, out_dir: Path, overwrite: bool = True) -> None:
    """Summarizes all projects in a scenario to folder.

    Args:
        scenario: Scenario instance to summarize.
        out_dir: Path to write the project cards.
        overwrite: If True, will overwrite the files if they already exist.
    """
    outdir = Path(out_dir)
    prep_dir(out_dir, overwrite=overwrite)

    for p in scenario.applied_projects:
        card = scenario.project_cards[p]
        filename = Path(card.__dict__.get("file", f"{p}.yml")).name
        outpath = outdir / filename
        write_card(card, outpath)


def create_base_scenario(
    roadway: Optional[dict] = None,
    transit: Optional[dict] = None,
    applied_projects: Optional[list] = [],
    conflicts: Optional[list] = [],
    config: WranglerConfig = None,
) -> dict:
    """Creates a base scenario dictionary from roadway and transit network files.

    Args:
        roadway: kwargs for load_roadway_from_dir
        transit: kwargs for load_transit from dir
        applied_projects: list of projects that have been applied to the base scenario.
        conflicts: list of conflicts that have been identified in the base scenario.
        config: WranglerConfig instance.
    """
    if roadway:
        road_net = load_roadway_from_dir(**roadway, config=config)
    else:
        road_net = None
        WranglerLogger.info(
            "No roadway directory specified, base scenario will have empty roadway network."
        )

    if transit:
        transit_net = load_transit(**transit, config=config)
        if roadway:
            transit_net.road_net = road_net
    else:
        transit_net = None
        WranglerLogger.info(
            "No transit directory specified, base scenario will have empty transit network."
        )

    base_scenario = {
        "road_net": road_net,
        "transit_net": transit_net,
        "applied_projects": applied_projects,
        "conflicts": conflicts,
    }

    return base_scenario


def extract_base_scenario_metadata(base_scenario: dict) -> dict:
    """Extract metadata from base scenario rather than keeping all of big files.

    Useful for summarizing a scenario.
    """
    _skip_copy = ["road_net", "transit_net", "config"]
    out_dict = {k: v for k, v in base_scenario.items() if k not in _skip_copy}
    if isinstance(base_scenario.get("road_net"), RoadwayNetwork):
        nodes_file = Path(base_scenario["road_net"].nodes_df.attrs["source_file"])
        out_dict["roadway"] = {"dir": str(nodes_file.parent), "suffix": str(nodes_file.suffix)}
    if isinstance(base_scenario.get("transit_net"), TransitNetwork):
        feed_path = base_scenario["transit_net"].feed.feed_path
        out_dict["transit"] = {"dir": str(feed_path)}
    return out_dict


def build_scenario_from_config(
    scenario_config: Union[Path, list[Path], ScenarioConfig, dict],
) -> Scenario:
    """Builds a scenario from a dictionary configuration.

    Args:
        scenario_config: Path to a configuration file, list of paths, or a dictionary of
            configuration.
    """
    WranglerLogger.info(f"Building Scenario from Configuration: {scenario_config}")
    scenario_config = load_scenario_config(scenario_config)
    WranglerLogger.debug(f"{pprint.pformat(scenario_config)}")

    base_scenario = create_base_scenario(
        **scenario_config.base_scenario.to_dict(), config=scenario_config.wrangler_config
    )  # type: ignore

    my_scenario = create_scenario(
        base_scenario=base_scenario,
        config=scenario_config.wrangler_config,
        **scenario_config.projects.to_dict(),  # type: ignore
    )

    my_scenario.apply_all_projects()

    write_args = _scenario_output_config_to_scenario_write(scenario_config.output_scenario)
    my_scenario.write(**write_args, name=scenario_config.name)  # type: ignore
    return my_scenario


def _scenario_output_config_to_scenario_write(
    scenario_output_config: ScenarioOutputConfig,
) -> dict:
    """Converts a ScenarioOutputConfig to a dictionary for use in write method."""
    _exc = ["roadway", "transit", "project_cards"]
    scenario_write_args = {k: v for k, v in scenario_output_config.items() if k not in _exc}
    roadway_args = {f"roadway_{k}": v for k, v in scenario_output_config.roadway.items()}
    transit_args = {f"transit_{k}": v for k, v in scenario_output_config.transit.items()}
    project_args = {f"projects_{k}": v for k, v in scenario_output_config.project_cards.items()}

    scenario_write_args.update(roadway_args)
    scenario_write_args.update(transit_args)
    scenario_write_args.update(project_args)

    return scenario_write_args


def _base_scenario_config_to_create_scenario(base_scenario_config: ScenarioInputConfig) -> dict:
    """Converts a ScenarioInputConfig to a dictionary for use in create_base_scenario method."""
    base_scenario_args = {}
    base_scenario_args["roadway"] = base_scenario_config["roadway"]
    base_scenario_args["transit"] = base_scenario_config["transit"]
    other_args = {k: v for k, v in base_scenario_config.items() if k not in ["roadway", "transit"]}
    base_scenario_args.update(other_args)
    return base_scenario_args

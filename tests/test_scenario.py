"""Tests related to scenarios.

Run just the tests labeled scenario using `pytest tests/test_scenario.py`
To run with print statments, use `pytest -s tests/test_scenario.py`
"""

import os
import copy
import sys
import subprocess

import pytest

from projectcard import read_card, write_card, ProjectCard
from network_wrangler.scenario import create_scenario
from network_wrangler.scenario import (
    ScenarioConflictError,
    ScenarioCorequisiteError,
    ScenarioPrerequisiteError,
)
from network_wrangler.logger import WranglerLogger


def test_project_card_read(request, stpaul_card_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    in_file = os.path.join(stpaul_card_dir, "road.prop_change.simple.yml")
    project_card = read_card(in_file)
    WranglerLogger.debug(project_card)
    assert project_card.change_type == "roadway_property_change"
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_project_card_write(request, stpaul_card_dir, scratch_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    in_file = os.path.join(stpaul_card_dir, "road.prop_change.simple.yml")
    outfile = os.path.join(scratch_dir, "t_simple_roadway_attribute_change.yml")
    project_card = read_card(in_file)
    write_card(project_card, outfile)
    test_card = read_card(in_file)
    for k, v in project_card.__dict__.items():
        assert v == test_card.__dict__[k]

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_scenario_conflicts(request, stpaul_card_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    project_a = ProjectCard({"project": "project a", "dependencies": {"conflicts": ["project b"]}})
    project_b = ProjectCard(
        {
            "project": "project b",
        }
    )

    project_card_list = [project_a, project_b]
    scen = create_scenario(base_scenario={}, project_card_list=project_card_list, validate=False)

    # should raise an error whenever calling queued projects or when applying them.
    with pytest.raises(ScenarioConflictError):
        WranglerLogger.info(scen.queued_projects)

    with pytest.raises(ScenarioConflictError):
        scen.apply_all_projects()

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_scenario_corequisites(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    project_a = ProjectCard(
        {
            "project": "project a",
            "dependencies": {"corequisites": ["project b", "project c"]},
        }
    )
    project_b = ProjectCard(
        {
            "project": "project b",
        }
    )

    project_card_list = [project_a, project_b]
    scen = create_scenario(base_scenario={}, project_card_list=project_card_list, validate=False)

    # should raise an error whenever calling queued projects or when applying them.
    with pytest.raises(ScenarioCorequisiteError):
        WranglerLogger.info(scen.queued_projects)

    with pytest.raises(ScenarioCorequisiteError):
        scen.apply_all_projects()
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_scenario_prerequisites(request):
    """Shouldn't be able to apply projects if they don't have their pre-requisites applied first."""
    WranglerLogger.info(f"--Starting: {request.node.name}")

    project_a = ProjectCard(
        {"project": "project a", "dependencies": {"prerequisites": ["project b"]}}
    )

    project_b = ProjectCard(
        {"project": "project b", "dependencies": {"prerequisites": ["project c"]}}
    )

    project_c = ProjectCard({"project": "project c"})

    project_d = ProjectCard(
        {"project": "project d", "dependencies": {"prerequisites": ["project b"]}}
    )
    scen = create_scenario(base_scenario={}, project_card_list=[project_a], validate=False)

    # should raise an error whenever calling queued projects or when applying them.
    with pytest.raises(ScenarioPrerequisiteError):
        WranglerLogger.info(scen.queued_projects)

    with pytest.raises(ScenarioPrerequisiteError):
        scen.apply_all_projects()

    # add other projects...
    scen.add_project_cards([project_b, project_c, project_d], validate=False)

    # if apply a project singuarly, it should also fail if it doesn't have prereqs
    with pytest.raises(ScenarioPrerequisiteError):
        scen.apply_projects(["project b"])

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_project_sort(request):
    """Make sure projects sort correctly before being applied."""
    WranglerLogger.info(f"--Starting: {request.node.name}")
    project_a = ProjectCard(
        {"project": "project a", "dependencies": {"prerequisites": ["project b"]}}
    )

    project_b = ProjectCard(
        {"project": "project b", "dependencies": {"prerequisites": ["project c"]}}
    )

    project_c = ProjectCard({"project": "project c"})

    project_d = ProjectCard(
        {
            "project": "project d",
            "dependencies": {"prerequisites": ["project b", "project a"]},
        }
    )

    expected_project_queue = ["project c", "project b", "project a", "project d"]

    scen = create_scenario(
        base_scenario={},
        project_card_list=[project_a, project_b, project_c, project_d],
        validate=False,
    )

    WranglerLogger.debug(f"scen.queued_projects: {scen.queued_projects}")
    assert list(scen.queued_projects) == expected_project_queue

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_apply_summary_wrappers(request, stpaul_card_dir, stpaul_net, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    stpaul_base_scenario = {
        "road_net": copy.deepcopy(stpaul_net),
        "transit_net": copy.deepcopy(stpaul_transit_net),
    }

    card_files = [
        "road.prop_change.multiple.yml",
        "road.managed_lane.simple.yml",
    ]

    project_card_path_list = [os.path.join(stpaul_card_dir, filename) for filename in card_files]

    my_scenario = create_scenario(
        base_scenario=stpaul_base_scenario,
        project_card_filepath=project_card_path_list,
    )

    my_scenario.apply_all_projects()

    WranglerLogger.debug(my_scenario.summarize())

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_scenario_building_from_script(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    config_file = os.path.join(os.getcwd(), "examples", "config_1.yml")
    # config_file = os.path.join(os.getcwd(),"example","config_2.yml")
    script_to_run = os.path.join(os.getcwd(), "scripts", "build_scenario.py")

    # replace backward slash with forward slash
    config_file = config_file.replace(os.sep, "/")
    script_to_run = script_to_run.replace(os.sep, "/")

    # print(config_file)
    # print(script_to_run)

    p = subprocess.Popen([sys.executable, script_to_run, config_file])
    p.communicate()  # wait for the subprocess call to finish

    WranglerLogger.info(f"--Finished: {request.node.name}")

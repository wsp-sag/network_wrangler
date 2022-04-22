"""Scenario Tests

Usage
-----
Run just the tests in this file run `pytest -m scenario`

"""

import os
import sys
import subprocess
import pytest

from network_wrangler import ProjectCard
from network_wrangler import RoadwayNetwork
from network_wrangler import TransitNetwork
from network_wrangler import Scenario
from network_wrangler.logger import WranglerLogger

from network_wrangler.conftest import (
    base_dir,
    small_net,
    small_dir,
    cached_small_net,
    stpaul_net,
    cached_stpaul_net,
    stpaul_transit,
    cached_stpaul_transit,
    stpaul_dir,
    stpaul_project_cards,
    example_dir,
    stpaul_basic_scenario,
)

pytestmark = pytest.mark.scenario


def test_project_card_read(request, stpaul_project_cards):
    print("\n--Starting:", request.node.name)

    in_file = os.path.join(
        stpaul_project_cards, "1_simple_roadway_attribute_change.yml"
    )
    project_card = ProjectCard.read(in_file)
    WranglerLogger.info(project_card)
    print(str(project_card))
    assert project_card.category == "Roadway Property Change"
    print("--Finished:", request.node.name)


def test_project_card_write(request, stpaul_project_cards, tmpdir):
    print("\n--Starting:", request.node.name)

    in_file = os.path.join(
        stpaul_project_cards, "1_simple_roadway_attribute_change.yml"
    )
    outfile = os.path.join(tmpdir, "t_simple_roadway_attribute_change.yml")
    project_card = ProjectCard.read(in_file)
    project_card.write(outfile)
    test_card = ProjectCard.read(in_file)
    for k, v in project_card.__dict__.items():
        assert v == test_card.__dict__[k]


def test_scenario_conflicts(request, stpaul_basic_scenario):

    scen = stpaul_basic_scenario

    print(str(scen), "\n")

    scen.check_scenario_conflicts()
    if scen.has_conflict_error:
        print("Conflicting project found for scenario!")

    print("Conflict checks done:", scen.conflicts_checked)
    print("--Finished:", request.node.name)


def test_scenario_requisites(request, stpaul_basic_scenario):
    print("\n--Starting:", request.node.name)
    scen = stpaul_basic_scenario

    print(str(scen), "\n")

    scen.check_scenario_requisites()
    if scen.has_requisite_error:
        print("Missing pre- or co-requisite projects found for scenario!")

    print("Requisite checks done:", scen.requisites_checked)
    print("--Finished:", request.node.name)


def test_project_sort(request, stpaul_basic_scenario):
    print("\n--Starting:", request.node.name)
    scen = stpaul_basic_scenario
    print("\n> Prerequisites:")
    import pprint

    pprint.pprint(scen.prerequisites)
    print("\nUnordered Projects:", scen.get_project_names())
    scen.check_scenario_conflicts()
    scen.check_scenario_requisites()

    scen.order_project_cards()
    print("Ordered Projects:", scen.get_project_names())
    print("--Finished:", request.node.name)


@pytest.mark.managed
def test_managed_lane_project_card(request, stpaul_project_cards):
    print("\n--Starting:", request.node.name)

    print("Reading project card ...")
    project_card_name = "5_managed_lane.yml"
    project_card_path = os.path.join(stpaul_project_cards, project_card_name)
    project_card = ProjectCard.read(project_card_path)
    print(project_card)

    print("--Finished:", request.node.name)


# selection, answer
query_tests = [
    # TEST 1
    (
        # SELECTION 1
        {
            "selection": {
                "link": [{"name": ["6th", "Sixth", "sixth"]}],
                "A": {"osm_node_id": "187899923"},  # start searching for segments at A
                "B": {"osm_node_id": "187865924"},  # end at B
            },
            "ignore": [],
        },
        # ANSWER 1
        '((name.str.contains("6th") or '
        + 'name.str.contains("Sixth") or '
        + 'name.str.contains("sixth")) and '
        + "(drive_access==1))",
    ),
    # TEST 2
    (
        # SELECTION 2
        {
            "selection": {
                "link": [{"name": ["6th", "Sixth", "sixth"]}],
                "A": {"osm_node_id": "187899923"},  # start searching for segments at A
                "B": {"osm_node_id": "187865924"},  # end at B
            },
            "ignore": ["name"],
        },
        # ANSWER 1
        "((drive_access==1))",
    ),
    # TEST 3
    (
        # SELECTION 3
        {
            "selection": {
                "link": [
                    {
                        "name": ["6th", "Sixth", "sixth"]
                    },  # find streets that have one of the various forms of 6th
                    {"lanes": [1, 2]},  # only select links that are either 1 or 2 lanes
                    {
                        "bike_access": [1]
                    },  # only select links that are marked for biking
                ],
                "A": {"osm_node_id": "187899923"},  # start searching for segments at A
                "B": {"osm_node_id": "187865924"},  # end at B
            },
            "ignore": [],
        },
        # ANSWER 3
        '((name.str.contains("6th") or '
        + 'name.str.contains("Sixth") or '
        + 'name.str.contains("sixth")) and '
        + "(lanes==1 or lanes==2) and "
        + "(bike_access==1) and (drive_access==1))",
    ),
    # TEST 4
    (
        # SELECTION 4
        {
            "selection": {
                "link": [
                    {
                        "name": ["6th", "Sixth", "sixth"]
                    },  # find streets that have one of the various forms of 6th
                    {"model_link_id": [134574]},
                    {"lanes": [1, 2]},  # only select links that are either 1 or 2 lanes
                    {
                        "bike_access": [1]
                    },  # only select links that are marked for biking
                ],
                "A": {"osm_node_id": "187899923"},  # start searching for segments at A
                "B": {"osm_node_id": "187865924"},  # end at B
            },
            "ignore": [],
        },
        # ANSWER 4
        "((model_link_id==134574))",
    ),
]


@pytest.mark.parametrize("test_spec", query_tests)
def test_query_builder(request, test_spec):
    selection, answer = test_spec

    sel_query = ProjectCard.build_link_selection_query(
        selection=selection["selection"],
        unique_model_link_identifiers=RoadwayNetwork.UNIQUE_MODEL_LINK_IDENTIFIERS,
        ignore=selection["ignore"],
    )

    print("\nsel_query:\n", sel_query)
    print("\nanswer:\n", answer)
    assert sel_query == answer

    print("--Finished:", request.node.name)


def test_apply_summary_wrappers(
    request, stpaul_project_cards, stpaul_net, stpaul_dir
):
    print("\n--Starting:", request.node.name)

    card_filenames = [
        "3_multiple_roadway_attribute_change.yml",
        "multiple_changes.yml",
        "4_simple_managed_lane.yml",
    ]

    project_cards_list = [
        ProjectCard.read(os.path.join(stpaul_project_cards, f), validate=False)
        for f in card_filenames
    ]

    base_scenario = {
        "road_net": stpaul_net,
        "transit_net": TransitNetwork.read(stpaul_dir),
    }

    my_scenario = Scenario.create_scenario(
        base_scenario=base_scenario, project_cards_list=project_cards_list
    )

    my_scenario.apply_all_projects()

    my_scenario.scenario_summary()

    print("--Finished:", request.node.name)


def test_scenario_building_from_script(request, base_dir, example_dir):
    print("\n--Starting:", request.node.name)

    config_file = os.path.join(example_dir, "config_1.yml")
    # config_file = os.path.join(example_dir,"config_2.yml")
    script_to_run = os.path.join(base_dir, "scripts", "build_scenario.py")

    # replace backward slash with forward slash
    config_file = config_file.replace(os.sep, "/")
    script_to_run = script_to_run.replace(os.sep, "/")

    # print(config_file)
    # print(script_to_run)

    p = subprocess.Popen([sys.executable, script_to_run, config_file])
    p.communicate()  # wait for the subprocess call to finish

    print("--Finished:", request.node.name)

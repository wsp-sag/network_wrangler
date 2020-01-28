import os
import sys
import subprocess
import pytest
from network_wrangler import ProjectCard
from network_wrangler import RoadwayNetwork
from network_wrangler import TransitNetwork
from network_wrangler import Scenario
from network_wrangler.Logger import WranglerLogger

"""
Run just the tests labeled scenario using `pytest -v -m scenario`
To run with print statments, use `pytest -s -m scenario`
"""

STPAUL_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "examples", "stpaul"
)
SCRATCH_DIR = os.path.join(os.getcwd(), "scratch")

STPAUL_SHAPE_FILE = os.path.join(STPAUL_DIR, "shape.geojson")
STPAUL_LINK_FILE = os.path.join(STPAUL_DIR, "link.json")
STPAUL_NODE_FILE = os.path.join(STPAUL_DIR, "node.geojson")


@pytest.mark.scenario
def test_project_card_read(request):
    print("\n--Starting:", request.node.name)
    in_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
        "examples",
        "stpaul",
        "project_cards",
    )
    in_file = os.path.join(in_dir, "1_simple_roadway_attribute_change.yml")
    project_card = ProjectCard.read(in_file)
    WranglerLogger.info(project_card)
    print(str(project_card))
    assert project_card.category == "Roadway Property Change"
    print("--Finished:", request.node.name)


@pytest.mark.scenario
def test_project_card_write(request):
    print("\n--Starting:", request.node.name)
    in_dir = os.path.join(STPAUL_DIR, "project_cards")
    in_file = os.path.join(in_dir, "1_simple_roadway_attribute_change.yml")
    outfile = os.path.join(SCRATCH_DIR, "t_simple_roadway_attribute_change.yml")
    project_card = ProjectCard.read(in_file)
    project_card.write(outfile)
    test_card = ProjectCard.read(in_file)
    for k, v in project_card.__dict__.items():
        assert v == test_card.__dict__[k]


@pytest.mark.scenario
@pytest.mark.travis
def test_scenario_conflicts(request):

    project_cards_list = []
    project_cards_list.append(
        ProjectCard.read(
            os.path.join(STPAUL_DIR, "project_cards", "a_test_project_card.yml")
        )
    )
    project_cards_list.append(
        ProjectCard.read(
            os.path.join(STPAUL_DIR, "project_cards", "b_test_project_card.yml")
        )
    )
    project_cards_list.append(
        ProjectCard.read(
            os.path.join(STPAUL_DIR, "project_cards", "c_test_project_card.yml")
        )
    )

    scen = Scenario.create_scenario(
        base_scenario={}, project_cards_list=project_cards_list
    )

    print(str(scen), "\n")

    scen.check_scenario_conflicts()
    if scen.has_conflict_error:
        print("Conflicting project found for scenario!")

    print("Conflict checks done:", scen.conflicts_checked)
    print("--Finished:", request.node.name)


@pytest.mark.scenario
@pytest.mark.travis
def test_scenario_requisites(request):
    print("\n--Starting:", request.node.name)
    base_scenario = {}

    project_cards_list = []
    project_cards_list.append(
        ProjectCard.read(
            os.path.join(STPAUL_DIR, "project_cards", "a_test_project_card.yml")
        )
    )
    project_cards_list.append(
        ProjectCard.read(
            os.path.join(STPAUL_DIR, "project_cards", "b_test_project_card.yml")
        )
    )
    project_cards_list.append(
        ProjectCard.read(
            os.path.join(STPAUL_DIR, "project_cards", "c_test_project_card.yml")
        )
    )

    scen = Scenario.create_scenario(
        base_scenario=base_scenario, project_cards_list=project_cards_list
    )

    print(str(scen), "\n")

    scen.check_scenario_requisites()
    if scen.has_requisite_error:
        print("Missing pre- or co-requisite projects found for scenario!")

    print("Requisite checks done:", scen.requisites_checked)
    print("--Finished:", request.node.name)


@pytest.mark.scenario
@pytest.mark.travis
def test_project_sort(request):
    print("\n--Starting:", request.node.name)
    base_scenario = {}

    project_cards_list = []
    project_cards_list.append(
        ProjectCard.read(
            os.path.join(STPAUL_DIR, "project_cards", "a_test_project_card.yml")
        )
    )
    project_cards_list.append(
        ProjectCard.read(
            os.path.join(STPAUL_DIR, "project_cards", "b_test_project_card.yml")
        )
    )
    project_cards_list.append(
        ProjectCard.read(
            os.path.join(STPAUL_DIR, "project_cards", "c_test_project_card.yml")
        )
    )

    scen = Scenario.create_scenario(
        base_scenario=base_scenario, project_cards_list=project_cards_list
    )
    print("\n> Prerequisites:")
    import pprint

    pprint.pprint(scen.prerequisites)
    print("\nUnordered Projects:", scen.get_project_names())
    scen.check_scenario_conflicts()
    scen.check_scenario_requisites()

    scen.order_project_cards()
    print("Ordered Projects:", scen.get_project_names())
    print("--Finished:", request.node.name)


@pytest.mark.roadway
@pytest.mark.scenario
@pytest.mark.travis
def test_managed_lane_project_card(request):
    print("\n--Starting:", request.node.name)

    print("Reading project card ...")
    project_card_name = "5_managed_lane.yml"
    project_card_path = os.path.join(
        os.getcwd(), "examples", "stpaul", "project_cards", project_card_name
    )
    project_card = ProjectCard.read(project_card_path)
    print(project_card)

    print("--Finished:", request.node.name)


@pytest.mark.unique_ids
@pytest.mark.travis
def test_query_builder(request):
    selection_1 = {
        "link": [{"name": ["6th", "Sixth", "sixth"]}],
        "A": {"osm_node_id": "187899923"},  # start searching for segments at A
        "B": {"osm_node_id": "187865924"},  # end at B
    }

    sel_query = ProjectCard.build_link_selection_query(
        selection=selection_1,
        unique_model_link_identifiers=RoadwayNetwork.UNIQUE_MODEL_LINK_IDENTIFIERS,
    )
    answer = (
        '((name.str.contains("6th") or '
        'name.str.contains("Sixth") or '
        'name.str.contains("sixth")) and '
        "drive_access==1)"
    )
    # print("\nsel_query:\n", sel_query)
    assert sel_query == answer

    sel_query = ProjectCard.build_link_selection_query(
        selection=selection_1,
        unique_model_link_identifiers=RoadwayNetwork.UNIQUE_MODEL_LINK_IDENTIFIERS,
        ignore=["name"],
    )
    answer = "(drive_access==1)"
    # print("\nsel_query:\n", sel_query)
    assert sel_query == answer

    selection_2 = {
        "link": [
            {
                "name": ["6th", "Sixth", "sixth"]
            },  # find streets that have one of the various forms of 6th
            {"lanes": [1, 2]},  # only select links that are either 1 or 2 lanes
            {"bike_access": [1]},  # only select links that are marked for biking
        ],
        "A": {"osm_node_id": "187899923"},  # start searching for segments at A
        "B": {"osm_node_id": "187865924"},  # end at B
    }

    sel_query = ProjectCard.build_link_selection_query(
        selection=selection_2,
        unique_model_link_identifiers=RoadwayNetwork.UNIQUE_MODEL_LINK_IDENTIFIERS,
    )
    answer = (
        '((name.str.contains("6th") or '
        'name.str.contains("Sixth") or '
        'name.str.contains("sixth")) and '
        "(lanes==1 or lanes==2) and "
        "(bike_access==1) and drive_access==1)"
    )
    # print("\nsel_query:\n", sel_query)
    assert sel_query == answer

    sel_query = ProjectCard.build_link_selection_query(
        selection=selection_2,
        unique_model_link_identifiers=RoadwayNetwork.UNIQUE_MODEL_LINK_IDENTIFIERS,
        ignore=["name"],
    )
    answer = "((lanes==1 or lanes==2) and " "(bike_access==1) and drive_access==1)"
    # print("\nsel_query:\n", sel_query)
    assert sel_query == answer

    selection_3 = {
        "link": [
            {
                "name": ["6th", "Sixth", "sixth"]
            },  # find streets that have one of the various forms of 6th
            {"model_link_id": [134574]},
            {"lanes": [1, 2]},  # only select links that are either 1 or 2 lanes
            {"bike_access": [1]},  # only select links that are marked for biking
        ],
        "A": {"osm_node_id": "187899923"},  # start searching for segments at A
        "B": {"osm_node_id": "187865924"},  # end at B
    }

    sel_query = ProjectCard.build_link_selection_query(
        selection=selection_3,
        unique_model_link_identifiers=RoadwayNetwork.UNIQUE_MODEL_LINK_IDENTIFIERS,
    )
    answer = "((model_link_id==134574))"
    # print("\nsel_query:\n", sel_query)
    assert sel_query == answer

    print("--Finished:", request.node.name)


@pytest.mark.scenario
@pytest.mark.travis
def test_apply_wrapper(request):
    print("\n--Starting:", request.node.name)

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

    base_scenario = {
        "road_net": RoadwayNetwork.read(
            link_file=STPAUL_LINK_FILE,
            node_file=STPAUL_NODE_FILE,
            shape_file=STPAUL_SHAPE_FILE,
            fast=True,
        ),
        "transit_net": TransitNetwork.read(STPAUL_DIR),
    }

    my_scenario = Scenario.create_scenario(
        base_scenario=base_scenario, project_cards_list=project_cards_list
    )

    my_scenario.apply_all_projects()

    my_scenario.scenario_summary()

    print("--Finished:", request.node.name)


@pytest.mark.scenario_building
def test_scenario_building_from_script(request):
    print("\n--Starting:", request.node.name)

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

    print("--Finished:", request.node.name)

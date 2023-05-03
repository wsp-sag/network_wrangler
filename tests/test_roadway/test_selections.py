import copy
import os

import pytest

from projectcard import read_card
import network_wrangler
from network_wrangler import RoadwayNetwork
from network_wrangler import WranglerLogger
from network_wrangler.roadwaynetwork import _dict_to_query

"""
Run just the tests labeled basic using `pytest tests/test_roadway/test_selections.py`
To run with print statments, use `pytest -s tests/test_roadway/test_selections.py`
"""


@pytest.mark.parametrize(
    "selection",
    [
        {  # SELECTION 1 - OLD PROJECT CARD
            "links": [{"name": ["6th", "Sixth", "sixth"]}],
            "A": {"osm_node_id": "187899923"},
            "B": {"osm_node_id": "187865924"},
            "answer": ["187899923", "187858777", "187923585", "187865924"],
        },
        {  # SELECTION 1 - CURRENT PROJECT CARD
            "name": ["6th", "Sixth", "sixth"],
            "from": {"osm_node_id": "187899923"},
            "to": {"osm_node_id": "187865924"},
            "answer": ["187899923", "187858777", "187923585", "187865924"],
        },
        {  # SELECTION 2 - OLD PROJECT CARD
            "links": [{"name": ["6th", "Sixth", "sixth"]}],
            "A": {"osm_node_id": "187899923"},
            "B": {"osm_node_id": "187942339"},
        },
        {  # SELECTION 2 - CURRENT PROJECT CARD
            "name": ["6th", "Sixth", "sixth"],
            "from": {"osm_node_id": "187899923"},
            "to": {"osm_node_id": "187942339"},
        },
        {  # SELECTION 3 - OLD
            "links": [{"name": ["6th", "Sixth", "sixth"]}, {"lanes": [1, 2]}],
            "A": {"osm_node_id": "187899923"},
            "B": {"osm_node_id": "187942339"},
        },
        {  # SELECTION 3 - CURRENT
            "name": ["6th", "Sixth", "sixth"],
            "lanes": [1, 2],
            "from": {"osm_node_id": "187899923"},
            "to": {"osm_node_id": "187942339"},
        },
        {  # SELECTION 4 - OLD
            "links": [{"name": ["I 35E"]}],
            "A": {"osm_node_id": "961117623"},
            "B": {"osm_node_id": "2564047368"},
        },
        {  # SELECTION 4 - NEW
            "name": ["I 35E"],
            "from": {"osm_node_id": "961117623"},
            "to": {"osm_node_id": "2564047368"},
        },
        {  # SELECTION 5
            "osm_link_id": ["221685900"],
            "from": {"model_node_id": "68075"},
            "to": {"model_node_id": "131216"},
            "answer": ["445978", "147798"],
        },
        {  # SELECTION 6
            "model_link_id": ["390239", "391206", "281", "1464"],
            "lanes": [1, 2],
            "answer": ["281", "1464"],
        },
    ],
)
def test_select_roadway_features(request, selection, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = stpaul_net
    print("--->", selection)

    _selection_dict = copy.deepcopy(selection)
    _selection_dict.pop("answer", None)

    _selection = net.get_selection(_selection_dict)

    selected_link_indices = _selection.selected_links
    WranglerLogger.debug(f"{len(_selection.selected_links)} links selected")
    WranglerLogger.debug(
        f"Selected Links: {len(_selection.selected_links_df[_selection_dict.keys])}"
    )
    if "answer" in selection.keys():
        assert set(_selection.segment.segment_nodes) == set(selection["answer"])

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_select_roadway_features_from_projectcard(request, stpaul_net, stpaul_ex_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = stpaul_net

    # selection is a facility segment
    project_card_name = "3_multiple_roadway_attribute_change.yml"
    _expected_answer = ["187899923", "187858777", "187923585", "187865924"]
    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", project_card_name)
    project_card = read_card(project_card_path)

    _facility = project_card.roadway_property_change["facility"]
    _selection = net.get_selection(_facility)
    selected_link_idx = _selection.selected_links
    WranglerLogger.debug(f"Features selected: {len(selected_link_idx)}")

    selected_nodes_idx = _selection.segment.segment_nodes
    assert set(selected_nodes_idx) == set(_expected_answer)

    WranglerLogger.info(f"--Finished: {request.node.name}")


variable_queries = [
    {"v": "lanes", "category": None, "time_period": ["7:00", "9:00"]},
    {"v": "ML_price", "category": "sov", "time_period": ["7:00", "9:00"]},
    {"v": "ML_price", "category": ["hov3", "hov2"], "time_period": ["7:00", "9:00"]},
]


@pytest.mark.parametrize("variable_query", variable_queries)
def test_query_roadway_property_by_time_group(
    request, variable_query, stpaul_net, stpaul_ex_dir
):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = stpaul_net

    project_card_path = os.path.join(
        stpaul_ex_dir, "project_cards", "5_managed_lane.yml"
    )
    project_card = read_card(project_card_path)
    net = net.apply(project_card)

    WranglerLogger.debug(f"QUERY:\n{variable_query}")
    v_series = net.get_property_by_time_period_and_group(
        variable_query["v"],
        category=variable_query["category"],
        time_period=variable_query["time_period"],
    )
    _selection = net.get_selection(project_card.roadway_managed_lanes["facility"])
    selected_link_indices = _selection.selected_links

    WranglerLogger.debug(f"CALCULATED:\n{v_series.loc[selected_link_indices]}")
    WranglerLogger.debug(
        f"ORIGINAL:\n{net.links_df.loc[selected_link_indices, variable_query['v']]}"
    )

    # TODO make test make sure the values are correct.
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_get_modal_network(request, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = stpaul_net
    mode = "transit"
    _links_df = net.links_df.mode_query(mode)

    test_links_of_selection = _links_df["model_link_id"].tolist()
    WranglerLogger.debug(
        f"TEST - Number of selected links: {len(test_links_of_selection)}"
    )

    control_links_of_selection = []
    for m in net.links_df.params.modes_to_network_link_variables[mode]:
        control_links_of_selection.extend(
            net.links_df.loc[net.links_df[m], "model_link_id"]
        )
    WranglerLogger.debug(
        f"CONTROL - Number of selected links: {len(control_links_of_selection)}"
    )

    all_model_link_ids = _links_df["model_link_id"].tolist()
    WranglerLogger.debug(f"CONTROL - Number of total links: {len(all_model_link_ids)}")

    assert set(test_links_of_selection) == set(control_links_of_selection)
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_identify_segment_ends(request, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    from network_wrangler.roadway.segment import identify_segment_endpoints

    net = stpaul_net

    _df = identify_segment_endpoints(net)

    calculated_d = _df.groupby("segment_id")["model_node_id"].apply(list).to_dict()
    correct_d = {
        0: [4785, 4798],
        1: [12163, 39484],
        2: [36271, 50577],
        3: [45746, 47478],
        4: [47478, 311086],
        5: [66416, 347045],
        6: [75351, 75352],
        7: [78880, 78885],
        8: [106815, 241023],
        9: [106811, 106814],
        10: [126388, 223962],
        11: [136296, 136301],
        12: [147096, 147097],
        13: [193468, 217752],
        14: [239877, 239878],
    }
    WranglerLogger.debug(f"Expected segment: {correct_d}")
    WranglerLogger.debug(f"Calculated segment: {calculated_d}")

    assert calculated_d == correct_d
    WranglerLogger.info(f"--Finished: {request.node.name}")


# selection, answer
query_tests = [
    # TEST 2
    (
        # SELECTION 2
        {"name": ["6th", "Sixth", "sixth"], "drive_access": 1},
        # ANSWER 2
        '((name.str.contains("6th") or '
        + 'name.str.contains("Sixth") or '
        + 'name.str.contains("sixth")) and '
        + "drive_access==1)",
    ),
    # TEST 2
    (
        # SELECTION 2
        {
            "name": [
                "6th",
                "Sixth",
                "sixth",
            ],  # find streets that have one of the various forms of 6th
            "lanes": [1, 2],  # only select links that are either 1 or 2 lanes
            "bike_access": [1],  # only select links that are marked for biking
        },
        # ANSWER 2
        '((name.str.contains("6th") or '
        + 'name.str.contains("Sixth") or '
        + 'name.str.contains("sixth")) and '
        + "(lanes==1 or lanes==2) and "
        + "(bike_access==1))",
    ),
    # TEST 3
    (
        # SELECTION 3
        {
            "model_link_id": [134574],
        },
        # ANSWER 3
        "((model_link_id==134574))",
    ),
]


@pytest.mark.parametrize("test_spec", query_tests)
def test_query_builder(request, test_spec, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    selection, answer = test_spec
    WranglerLogger.debug(f"Getting selection: \n{selection}")

    sel_query = _dict_to_query(selection)

    WranglerLogger.debug(f"\nsel_query:\n{sel_query}")
    WranglerLogger.debug(f"\nanswer:\n{answer}")
    assert sel_query == answer

    WranglerLogger.info(f"--Finished: {request.node.name}")

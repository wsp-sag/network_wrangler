import copy
import os

import pandas as pd

import pytest

from projectcard import read_card
import network_wrangler
from network_wrangler import RoadwayNetwork
from network_wrangler import WranglerLogger
from network_wrangler.roadway import RoadwaySelection
from network_wrangler.roadwaynetwork import _dict_to_query

"""
Run just the tests labeled basic using `pytest tests/test_roadway/test_selections.py`
To run with print statments, use `pytest -s tests/test_roadway/test_selections.py`
"""

def test_dfhash(request,stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    df = pd.DataFrame({'a':[1,2,3],'b':[2,4,5]})
    hash1 = df.df_hash()
    hash2 = df.df_hash()
    WranglerLogger.debug(f"Simple DF\nhash1: {hash1}\nhash2: {hash2}")
    assert hash1==hash2

    df = stpaul_net.nodes_df
    hash1 = df.df_hash()
    hash2 = df.df_hash()
    WranglerLogger.debug(f"Full Nodes\nhash1: {hash1}\nhash2: {hash2}")
    assert hash1==hash2
    WranglerLogger.info(f"--Finished: {request.node.name}")

def test_links_in_path(request,stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    links_df = pd.DataFrame({'A':[1,1,2,4,3],'B':[2,4,4,5,5],'id':['one','two','three','four','five']})
    path = [1,4,5]
    path_links_df = stpaul_net.links_in_path(links_df,path)
    WranglerLogger.info(f"Path Nodes: {path}")
    WranglerLogger.info(f"Path Links:\n {path_links_df}")
    answer_ids = ['two','four']
    assert path_links_df['id'].tolist() == answer_ids
    WranglerLogger.info(f"--Finished: {request.node.name}")

TEST_SELECTIONS = [
    {  # SELECTION 1
        "links":{
            "name": ["6th", "Sixth", "sixth"],
        },
        "from": {"osm_node_id": "187899923"},
        "to": {"osm_node_id": "187865924"},
    },
    {  # SELECTION 2 
        "links":{
            "name": ["Lafayette"],
        },
        "from": {"osm_node_id": "2292977517"},
        "to": {"osm_node_id": "507951637"},
    },
    {  # SELECTION 3 
        "links":{
            "name": ["University Ave"],
            "lanes": [1],
        },
        "from": {"osm_node_id": "716319401"},
        "to": {"model_node_id": "62153"},
    },
    {  # SELECTION 4 
        "links":{
            "name": ["I 35E"],
        },
        "from": {"osm_node_id": "954746969"},
        "to": {"osm_node_id": "3071141242"},
    },
    {  # SELECTION 5
        "links":{
            "osm_link_id": ["221685893"],
        },
        "from": {"model_node_id": "131209"},
        "to": {"model_node_id": "131221"},
    },
    {  # SELECTION 6
        "links":{
            "model_link_id": ["390239", "391206", "281", "1464"],
            "lanes": [1, 2],
        },
    },
    {  # SELECTION 7
        "links":{
            "all": True,
            "lanes": [1, 2],
        },
    },
    {  # SELECTION 8
        "links":{
            "all": True,
        },
    },
    {  # SELECTION 9
        "modes": ["walk"],
        "links":{
            "name": ["Valley Street"],
        },
        "from":{
            "model_node_id":174762
        },
        "to":{
            "model_node_id":43041
        },
    }
]


node_sel_dict_answers = [
    {   
        "from": {"osm_node_id": "187899923"},
        "to": {"osm_node_id": "187865924"},
    },
    {  
        "from": {"osm_node_id": "2292977517"},
        "to": {"osm_node_id": "507951637"},
    },
    {  
        "from": {"osm_node_id": "716319401"},
        "to": {"model_node_id": "62153"},
    },
    {  
        "from": {"osm_node_id": "961117623"},
        "to": {"osm_node_id": "2564047368"},
    },
    {  
        "from": {"model_node_id": "131209"},
        "to": {"model_node_id": "131221"},
    },
    { },
    { },
    { },
    {
        "from":{"model_node_id":174762 },
        "to":{"model_node_id":43041},
    },
]


@pytest.mark.parametrize("selection,answer",zip(TEST_SELECTIONS,node_sel_dict_answers))
def test_calc_node_selection_dict(request,selection,answer):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    d = RoadwaySelection.calc_node_selection_dict(selection)
    assert d == answer
    WranglerLogger.info(f"--Finished: {request.node.name}")

link_sel_dict_answers = [
    {  
        "name": ["6th", "Sixth", "sixth"],
    },
    {  
        "name": ["Lafayette"],
    },
    {  
        "name": ["University Ave"],
        "lanes": [1],
    },
    {  
        "name": ["I 35E"],
    },
    {  
        "osm_link_id": ["221685893"],
    },
    {  
        "model_link_id": ["390239", "391206", "281", "1464"],
        "lanes": [1, 2],
    },
    {  
        "all": True,
        "lanes": [1, 2],
    },
    {  
        "all": True,
    },
    {
        "name": ["Valley Street"]
    },
]

@pytest.mark.parametrize("selection,answer",zip(TEST_SELECTIONS,link_sel_dict_answers))
def test_calc_link_selection_dict(request,selection,answer):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    d = RoadwaySelection.calc_link_selection_dict(selection)
    assert d == answer
    WranglerLogger.info(f"--Finished: {request.node.name}")

#  134543, 488245

answer_selected_links = [
    [134543, 85185, 154004], # SELECTION 1
    [386035,401018,401019], # SELECTION 2
    [412924,389361], # SELECTION 3
    [381412,392837,394194,394196,391146], # SELECTION 4
    [294513,294518,294532], # SELECTION 5
    [281, 1464],# SELECTION 6
    None,# SELECTION 7 - all links
    None,# SELECTION 8 - all links with some features
    [460228,481940] # SELECTION 9 - Valley Street Pedestrian Ways
]
@pytest.mark.menow
@pytest.mark.parametrize("selection,answer",zip(TEST_SELECTIONS,answer_selected_links))
def test_select_roadway_features(request, selection, answer, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = stpaul_net
    WranglerLogger.info(f"Selecting--->{selection}")
    WranglerLogger.info(f"Num Net Selections: {len(net._selections)}")
    _selection = net.get_selection(selection)
    _show_f = ["A","B","name","osm_link_id","model_link_id","lanes"]
    selected_link_indices = _selection.selected_links
    WranglerLogger.info(f"{len(_selection.selected_links)} links selected")
    if _selection.selection_type == "segment_search":
        WranglerLogger.info(f"Segment Path: \n{_selection.segment.segment_nodes}")
        WranglerLogger.info(f"Segment Links: \n{_selection.segment.segment_links_df[_show_f]}")
    WranglerLogger.info(f"Selected Links")
    
    if len(selected_link_indices )<10:
        WranglerLogger.info(f"Selected Links: \n{_selection.selected_links_df[_show_f]}")
        
    if answer:
        WranglerLogger.info(f"Answer Links: {answer}")
        assert set(selected_link_indices) == set(answer)

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

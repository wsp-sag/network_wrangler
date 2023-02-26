import os
import time

import pytest

import pandas as pd

from network_wrangler import ProjectCard
from network_wrangler import RoadwayNetwork
from network_wrangler import WranglerLogger

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 50000)

"""
Run just the tests labeled basic using `pytest -m roadway`
To run with print statments, use `pytest -s -m roadway`
"""

@pytest.mark.parametrize(
    "selection",
    [
        {  # SELECTION 1
            "links": [{"name": ["6th", "Sixth", "sixth"]}],
            "A": {"osm_node_id": "187899923"},
            "B": {"osm_node_id": "187865924"},
            "answer": ["187899923", "187858777", "187923585", "187865924"],
        },
        {  # SELECTION 2
            "links": [{"name": ["6th", "Sixth", "sixth"]}],
            "A": {"osm_node_id": "187899923"},  # start searching for segments at A
            "B": {"osm_node_id": "187942339"},
        },
        {  # SELECTION 3
            "links": [{"name": ["6th", "Sixth", "sixth"]}, {"lanes": [1, 2]}],
            "A": {"osm_node_id": "187899923"},  # start searching for segments at A
            "B": {"osm_node_id": "187942339"},
        },
        {  # SELECTION 4
            "links": [{"name": ["I 35E"]}],
            "A": {"osm_node_id": "961117623"},  # start searching for segments at A
            "B": {"osm_node_id": "2564047368"},
        },
        {  # SELECTION 5
            "links": [
                {"name": ["6th", "Sixth", "sixth"]},
                {"model_link_id": [2846, 2918]},
                {"lanes": [1, 2]},
            ]
        },
    ],
)
@pytest.mark.roadway
def test_select_roadway_features(request, selection, stpaul_net):
    print("\n--Starting:", request.node.name)
    net = stpaul_net
    print("--->", selection)
    net.select_roadway_features(selection)

    print("building a selection key")
    sel_key = net.build_selection_key(selection)

    print("Features selected:", len(net.selections[sel_key]["selected_links"]))
    selected_link_indices = net.selections[sel_key]["selected_links"].index.tolist()
    if "answer" in selection.keys():
        print("Comparing answer")
        selected_nodes = [str(selection["A"]["osm_node_id"])] + net.links_df.loc[
            selected_link_indices, "v"
        ].tolist()
        # print("Nodes selected: ",selected_nodes)
        # print("Expected Answer: ", sel["answer"])
        assert set(selected_nodes) == set(selection["answer"])

    print("--Finished:", request.node.name)


@pytest.mark.roadway
@pytest.mark.travis
def test_select_roadway_features_from_projectcard(request,stpaul_net,stpaul_ex_dir):
    print("\n--Starting:", request.node.name)
    net = stpaul_net
    print("Reading project card ...")
    project_card_name = "3_multiple_roadway_attribute_change.yml"

    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path)

    print("Selecting roadway features ...")
    sel = project_card.facility
    selected_link_indices = net.select_roadway_features(sel)
    print("Features selected:", len(selected_link_indices))

    print("--Finished:", request.node.name)


variable_queries = [
    {"v": "lanes", "category": None, "time_period": ["7:00", "9:00"]},
    {"v": "ML_price", "category": "sov", "time_period": ["7:00", "9:00"]},
    {"v": "ML_price", "category": ["hov3", "hov2"], "time_period": ["7:00", "9:00"]},
]


@pytest.mark.parametrize("variable_query",variable_queries)
@pytest.mark.roadway
def test_query_roadway_property_by_time_group(request, variable_query,stpaul_net,stpaul_ex_dir):
    print("\n--Starting:", request.node.name)
    net = stpaul_net
    print("Applying project card...")
    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", "5_managed_lane.yml")
    project_card = ProjectCard.read(project_card_path, validate=False)
    net.apply_managed_lane_feature_change(
        net.select_roadway_features(project_card.facility), project_card.properties
    )
    print("Querying Attribute...")
    print("QUERY:\n", variable_query)
    v_series = net.get_property_by_time_period_and_group(
        variable_query["v"],
        category=variable_query["category"],
        time_period=variable_query["time_period"],
    )
    selected_link_indices = net.select_roadway_features(project_card.facility)

    print("CALCULATED:\n", v_series.loc[selected_link_indices])
    print("ORIGINAL:\n", net.links_df.loc[selected_link_indices, variable_query["v"]])

    # TODO make test make sure the values are correct.


@pytest.mark.roadway
@pytest.mark.travis
def test_get_modal_network(request,stpaul_net):
    print("\n--Starting:", request.node.name)

    mode = "transit"
    print("Reading network. Mode: {} ...".format(mode))

    net = stpaul_net
    _links_df, _nodes_df = RoadwayNetwork.get_modal_links_nodes(
        net.links_df,
        net.nodes_df,
        modes=[mode],
    )

    test_links_of_selection = _links_df["model_link_id"].tolist()
    print("TEST - Number of selected links: {}".format(len(test_links_of_selection)))

    mode_variables = RoadwayNetwork.MODES_TO_NETWORK_LINK_VARIABLES[mode]

    control_links_of_selection = []
    for m in mode_variables:
        control_links_of_selection.extend(
            net.links_df.loc[net.links_df[m], "model_link_id"]
        )
    print(
        "CONTROL - Number of selected links: {}".format(len(control_links_of_selection))
    )

    all_model_link_ids = _links_df["model_link_id"].tolist()
    print("CONTROL - Number of total links: {}".format(len(all_model_link_ids)))

    assert set(test_links_of_selection) == set(control_links_of_selection)


@pytest.mark.travis
@pytest.mark.roadway
def test_identify_segment_ends(request,stpaul_net):
    print("\n--Starting:", request.node.name)

    print("Reading network ...")
    net = stpaul_net

    _df = net.identify_segment_endpoints()

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

    print(calculated_d)
    assert calculated_d == correct_d


@pytest.mark.travis
@pytest.mark.roadway
def test_find_segment(request,stpaul_net):
    print("\n--Starting:", request.node.name)

    print("Reading network ...")
    net = stpaul_net

    seg_ends = [4785, 4798]
    sel_dict = {"name": "North Mounds Boulevard", "ref": "US 61"}
    seg_df = net.identify_segment(seg_ends[0], seg_ends[1], selection_dict=sel_dict)
    print(seg_df)

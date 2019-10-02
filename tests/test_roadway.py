import os
import json
from geopandas import GeoDataFrame
import pytest
from network_wrangler import RoadwayNetwork
from network_wrangler import ProjectCard
import time
import numpy as np
import pandas as pd

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 50000)

"""
Run just the tests labeled basic using `pytest -m roadway`
To run with print statments, use `pytest -s -m roadway`
"""

STPAUL_DIR = os.path.join(os.getcwd(), "example", "stpaul")
STPAUL_SHAPE_FILE = os.path.join(STPAUL_DIR, "shape.geojson")
STPAUL_LINK_FILE = os.path.join(STPAUL_DIR, "link.json")
STPAUL_NODE_FILE = os.path.join(STPAUL_DIR, "node.geojson")

SMALL_DIR = os.path.join(os.getcwd(), "example", "single")
SMALL_SHAPE_FILE = os.path.join(SMALL_DIR, "shape.geojson")
SMALL_LINK_FILE = os.path.join(SMALL_DIR, "link.json")
SMALL_NODE_FILE = os.path.join(SMALL_DIR, "node.geojson")

SCRATCH_DIR = os.path.join(os.getcwd(), "scratch")


@pytest.mark.roadway
def test_roadway_read_write(request):
    print("\n--Starting:", request.node.name)

    out_prefix = "t_readwrite"
    out_shape_file = os.path.join(SCRATCH_DIR, out_prefix + "_" + "shape.geojson")
    out_link_file = os.path.join(SCRATCH_DIR, out_prefix + "_" + "link.json")
    out_node_file = os.path.join(SCRATCH_DIR, out_prefix + "_" + "node.geojson")

    time0 = time.time()

    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    time1 = time.time()
    print("Writing to: {}".format(SCRATCH_DIR))
    net.write(filename=out_prefix, path=SCRATCH_DIR)
    time2 = time.time()
    net_2 = RoadwayNetwork.read(
        link_file=out_link_file, node_file=out_node_file, shape_file=out_shape_file
    )
    time3 = time.time()

    read_time1 = time1 - time0
    read_time2 = time3 - time2
    write_time = time2 - time1

    print("TIME, read (w/out valdiation, with): {},{}".format(read_time1, read_time2))
    print("TIME, write:{}".format(write_time))
    """
    # right now don't have a good way of ignoring differences in rounding
    with open(shape_file, 'r') as s1:
        og_shape = json.loads(s1.read())
        #og_shape.replace('\r', '').replace('\n', '').replace(' ','')
    with open(os.path.join('scratch','t_readwrite_shape.geojson'), 'r')  as s2:
        new_shape = json.loads(s2.read())
        #new_shape.replace('\r', '').replace('\n', '').replace(' ','')
    assert(og_shape==new_shape)
    """


@pytest.mark.roadway
@pytest.mark.travis
def test_quick_roadway_read_write(request):
    print("\n--Starting:", request.node.name)

    out_prefix = "t_readwrite"
    out_shape_file = os.path.join(SCRATCH_DIR, out_prefix + "_" + "shape.geojson")
    out_link_file = os.path.join(SCRATCH_DIR, out_prefix + "_" + "link.json")
    out_node_file = os.path.join(SCRATCH_DIR, out_prefix + "_" + "node.geojson")
    net = RoadwayNetwork.read(
        link_file=SMALL_LINK_FILE,
        node_file=SMALL_NODE_FILE,
        shape_file=SMALL_SHAPE_FILE,
        fast=True,
    )
    net.write(filename=out_prefix, path=SCRATCH_DIR)
    net_2 = RoadwayNetwork.read(
        link_file=out_link_file, node_file=out_node_file, shape_file=out_shape_file
    )
    print("--Finished:", request.node.name)


@pytest.mark.basic
@pytest.mark.roadway
def test_select_roadway_features(request):
    print("\n--Starting:", request.node.name)
    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    test_selections = {
        "1. simple": {
            "link": [{"name": ["6th", "Sixth", "sixth"]}],
            "A": {"osmNodeId": "187899923"},
            "B": {"osmNodeId": "187865924"},
            "answer": ["187899923", "187858777", "187923585", "187865924"],
        },
        "2. farther": {
            "link": [{"name": ["6th", "Sixth", "sixth"]}],
            "A": {"osmNodeId": "187899923"},  # start searching for segments at A
            "B": {"osmNodeId": "187942339"},
        },
        "3. multi-criteria": {
            "link": [{"name": ["6th", "Sixth", "sixth"]}, {"LANES": [1, 2]}],
            "A": {"osmNodeId": "187899923"},  # start searching for segments at A
            "B": {"osmNodeId": "187942339"},
        },
        "4. reference": {
            "link": [{"name": ["I 35E"]}],
            "A": {"osmNodeId": "961117623"},  # start searching for segments at A
            "B": {"osmNodeId": "2564047368"},
        },
    }

    for i, sel in test_selections.items():
        print("--->", i, "\n", sel)
        selected_link_indices = net.select_roadway_features(sel)
        print("Features selected:", len(selected_link_indices))

        if "answer" in sel.keys():
            selected_nodes = [str(sel["A"]["osmNodeId"])] + net.links_df.loc[selected_link_indices, "v"].tolist()
            #print("Nodes selected: ",selected_nodes)
            #print("Expected Answer: ", sel["answer"])
            assert set(selected_nodes) == set(sel["answer"])

    print("--Finished:", request.node.name)


@pytest.mark.roadway
@pytest.mark.travis
def test_select_roadway_features_from_projectcard(request):
    print("\n--Starting:", request.node.name)

    print("Reading network ...")
    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    print("Reading project card ...")
    project_card_name = "3_multiple_roadway_attribute_change.yml"

    project_card_path = os.path.join(STPAUL_DIR, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path)

    print("Selecting roadway features ...")
    sel = project_card.facility
    selected_link_indices = net.select_roadway_features(sel)
    print("Features selected:", len(selected_link_indices))

    print("--Finished:", request.node.name)


@pytest.mark.roadway
@pytest.mark.travis
def test_apply_roadway_feature_change(request):
    print("\n--Starting:", request.node.name)

    print("Reading network ...")
    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    project_card_set = [
        (net, "1_simple_roadway_attribute_change.yml"),
        (net, "2_multiple_roadway.yml"),
        (net, "3_multiple_roadway_attribute_change.yml"),
    ]

    for my_net, project_card_name in project_card_set:
        print("Reading project card", project_card_name, "...")
        project_card_path = os.path.join(STPAUL_DIR, "project_cards", project_card_name)
        project_card = ProjectCard.read(project_card_path)

        print("Selecting roadway features ...")
        selected_link_indices = my_net.select_roadway_features(project_card.facility)

        attributes_to_update = [p["property"] for p in project_card.properties]
        orig_links = my_net.links_df.loc[selected_link_indices, attributes_to_update]
        print("Original Links:\n", orig_links)

        my_net.apply_roadway_feature_change(
            my_net.select_roadway_features(project_card.facility),
            project_card.properties
        )

        rev_links = my_net.links_df.loc[selected_link_indices, attributes_to_update]
        print("Revised Links:\n", rev_links)

    print("--Finished:", request.node.name)

@pytest.mark.managed
@pytest.mark.roadway
@pytest.mark.travis
def test_add_managed_lane(request):
    print("\n--Starting:", request.node.name)

    print("Reading network ...")
    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    print("Reading project card ...")
    project_card_name = "5_managed_lane.yml"
    project_card_path = os.path.join(STPAUL_DIR, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path)

    print("Selecting roadway features ...")
    selected_link_indices = net.select_roadway_features(project_card.facility)

    attributes_to_update = [p["property"] for p in project_card.properties]
    orig_links = net.links_df.loc[selected_link_indices, attributes_to_update]
    print("Original Links:\n", orig_links)

    net.apply_managed_lane_feature_change(
        net.select_roadway_features(project_card.facility),
        project_card.properties
    )

    rev_links = net.links_df.loc[selected_link_indices, attributes_to_update]
    print("Revised Links:\n", rev_links)

    net.write(filename="test_ml", path=SCRATCH_DIR)

    print("--Finished:", request.node.name)

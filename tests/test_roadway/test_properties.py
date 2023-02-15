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

STPAUL_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "examples", "stpaul"
)
STPAUL_SHAPE_FILE = os.path.join(STPAUL_DIR, "shape.geojson")
STPAUL_LINK_FILE = os.path.join(STPAUL_DIR, "link.json")
STPAUL_NODE_FILE = os.path.join(STPAUL_DIR, "node.geojson")

SMALL_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "examples", "single"
)
SMALL_SHAPE_FILE = os.path.join(SMALL_DIR, "shape.geojson")
SMALL_LINK_FILE = os.path.join(SMALL_DIR, "link.json")
SMALL_NODE_FILE = os.path.join(SMALL_DIR, "node.geojson")

SCRATCH_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "scratch"
)


def _read_small_net():
    net = RoadwayNetwork.read(
        link_file=SMALL_LINK_FILE,
        node_file=SMALL_NODE_FILE,
        shape_file=SMALL_SHAPE_FILE,
        fast=True,
    )
    return net


def _read_stpaul_net():
    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    return net


@pytest.mark.parametrize("variable_query", variable_queries)
@pytest.mark.roadway
@pytest.mark.travis
def test_query_roadway_property_by_time_group(request, variable_query):
    print("\n--Starting:", request.node.name)
    net = _read_stpaul_net()
    print("Applying project card...")
    project_card_path = os.path.join(STPAUL_DIR, "project_cards", "5_managed_lane.yml")
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
def test_network_connectivity(request):
    print("\n--Starting:", request.node.name)

    print("Reading network ...")

    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    print("Checking network connectivity ...")
    print("Drive Network Connected:", net.is_network_connected(mode="drive"))
    print("--Finished:", request.node.name)


@pytest.mark.roadway
@pytest.mark.travis
def test_network_connectivity_ignore_single_nodes(request):
    print("\n--Starting:", request.node.name)

    print("Reading network ...")

    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    print("Assessing network connectivity for walk...")
    _, disconnected_nodes = net.assess_connectivity(mode="walk", ignore_end_nodes=True)
    print("{} Disconnected Subnetworks:".format(len(disconnected_nodes)))
    print("-->\n{}".format("\n".join(list(map(str, disconnected_nodes)))))
    print("--Finished:", request.node.name)

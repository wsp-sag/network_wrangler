import copy
import os

import pytest

import pandas as pd

from network_wrangler import ProjectCard
from network_wrangler import RoadwayNetwork
from network_wrangler import WranglerLogger

"""
Run just the tests labeled basic using `pytest tests/test_roadway/test_changes/test_roadway_add_delete.py`
"""


def test_add_roadway_link_project_card(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    _project = "Add Newsy Link"
    _category = "Add New Roadway"
    _links = [
        {
            "A": 3494,
            "B": 3230,
            "model_link_id": 123456,
            "roadway": "busway",
            "walk_access": 0,
            "drive_access": 0,
            "bus_only": 1,
            "rail_only": 1,
            "bike_access": 0,
            "lanes": 1,
            "name": "new busway link",
        },
        {
            "A": 3230,
            "B": 3494,
            "model_link_id": 123457,
            "roadway": "busway",
            "walk_access": 0,
            "drive_access": 0,
            "bus_only": 1,
            "rail_only": 1,
            "bike_access": 0,
            "lanes": 1,
            "name": "new busway link",
        },
    ]

    pc_dict = {
        "project": _project,
        "category": _category,
        "links": _links,
    }

    net = copy.deepcopy(small_net)
    net = net.apply(pc_dict)

    _new_link_idxs = [i["model_link_id"] for i in _links]
    _expected_new_link_fks = [(i["A"], i["B"]) for i in _links]
    _new_links = net.links_df.loc[_new_link_idxs]

    WranglerLogger.debug(f"New Links:\n{_new_links}")
    assert len(_new_links) == len(_links)

    assert set(list(zip(_new_links.A, _new_links.B))) == set(_expected_new_link_fks)

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_add_roadway_project_card(request, stpaul_net, stpaul_ex_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(stpaul_net)
    card_name = "10_simple_roadway_add_change.yml"
    expected_net_links = 2
    expected_net_nodes = 0

    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", card_name)
    project_card = ProjectCard.read(project_card_path, validate=False)

    orig_links_count = len(net.links_df)
    orig_nodes_count = len(net.nodes_df)
    net = net.apply(project_card.__dict__)
    net_links = len(net.links_df) - orig_links_count
    net_nodes = len(net.nodes_df) - orig_nodes_count

    assert net_links == expected_net_links
    assert net_nodes == expected_net_nodes


def test_multiple_add_delete_roadway_project_card(request, stpaul_net, stpaul_ex_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(stpaul_net)
    card_name = "11_multiple_roadway_add_and_delete_change.yml"
    expected_net_links = -2 + 2
    expected_net_nodes = +1 - 1 + 1

    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", card_name)
    project_card = ProjectCard.read(project_card_path, validate=False)

    orig_links_count = len(net.links_df)
    orig_nodes_count = len(net.nodes_df)
    net = net.apply(project_card.__dict__)
    net_links = len(net.links_df) - orig_links_count
    net_nodes = len(net.nodes_df) - orig_nodes_count

    assert net_links == expected_net_links
    assert net_nodes == expected_net_nodes


@pytest.mark.xfail(strict=True)
def test_add_roadway_links(request, stpaul_net, stpaul_ex_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(stpaul_net)

    print("Reading project card ...")
    # project_card_name = "10_simple_roadway_add_change.yml"
    project_card_name = "10a_incorrect_roadway_add_change.yml"

    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path)

    project_card_dictionary = project_card.__dict__

    net.add_new_roadway_feature_change(
        project_card_dictionary.get("links", []),
        project_card_dictionary.get("nodes", []),
    )

    print("--Finished:", request.node.name)


def test_delete_roadway_shape(request, stpaul_net, stpaul_ex_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(stpaul_net)

    card_name = "13_simple_roadway_delete_change.yml"
    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", card_name)
    project_card = ProjectCard.read(project_card_path, validate=False)

    expected_net_links = -1

    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", card_name)
    project_card = ProjectCard.read(project_card_path, validate=False)

    orig_links_count = len(net.links_df)

    net = net.apply(project_card.__dict__)
    net_links = len(net.links_df) - orig_links_count

    assert net_links == expected_net_links

    print("--Finished:", request.node.name)


def test_add_nodes(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(small_net)

    node_properties = {
        "X": -93.14412,
        "Y": 44.87497,
        "bike_node": 1,
        "drive_node": 1,
        "transit_node": 0,
        "walk_node": 1,
        "model_node_id": 1234567,
    }

    net = net.apply(
        {
            "category": "add new roadway",
            "project": "test adding a node",
            "nodes": [node_properties],
        }
    )

    WranglerLogger.debug(
        f"Added Node 1234567:\n{net.nodes_df.loc[net.nodes_df.model_node_id == 1234567]}"
    )

    assert 1234567 in net.nodes_df.model_node_id.tolist()

    # should fail when adding a node with a model_node_id that already exists
    bad_node_properties = node_properties.copy()
    bad_node_properties["model_node_id"] = (3494,)  # should already be in network
    try:
        net = net.apply(
            {
                "category": "add new roadway",
                "project": "test adding a node already in network",
                "nodes": [bad_node_properties],
            },
        )
    except ValueError:
        "expected ValueError when adding a node with a model_node_id that already exists"
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_change_node_xy(request, small_net):
    """Tests if X and Y property changes from a project card also update the node/link geometry."""
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(small_net)

    _test_link = net.links_df.iloc[0]
    _test_link_idx = _test_link[RoadwayNetwork.UNIQUE_LINK_KEY]
    _test_node = net.nodes_df.loc[[_test_link["A"]]].iloc[0]
    _test_node_idx = _test_node[[RoadwayNetwork.UNIQUE_NODE_KEY]].iloc[0]

    WranglerLogger.debug(f"Node Index: {_test_node_idx}")
    WranglerLogger.debug(f"Link Index: {_test_link_idx}")
    WranglerLogger.info(
        f"Original Node (Index: {_test_node_idx}):\n{net.nodes_df.loc[_test_node_idx]}"
    )

    facility = {
        "nodes": [
            {"model_node_id": [_test_node_idx]},
        ]
    }
    _expected_X = -1000
    _expected_Y = 1000000
    properties = [
        {"property": "X", "set": _expected_X},
        {"property": "Y", "set": _expected_Y},
    ]

    net = net.apply(
        {
            "category": "Roadway Property Change",
            "project": "Update node geometry",
            "facility": facility,
            "properties": properties,
        }
    )

    _updated_node = net.nodes_df.loc[_test_node_idx]
    _updated_link = net.links_df.loc[_test_link_idx]
    _first_point = _updated_link.geometry.coords[0]

    WranglerLogger.info(
        f"Updated Node:\n{_updated_node[[RoadwayNetwork.UNIQUE_NODE_KEY,'X','Y','geometry']]}"
    )
    WranglerLogger.info(
        f"Updated Link Geometry for ({_updated_link.A}-->{_updated_link.B}):\n{_updated_link[['geometry']]}"
    )

    assert _updated_node.geometry.x == _expected_X
    assert _updated_node.geometry.y == _expected_Y
    assert _updated_node.X == _expected_X
    assert _updated_node.Y == _expected_Y
    assert _first_point[0] == _expected_X
    assert _first_point[1] == _expected_Y
    WranglerLogger.info(f"--Finished: {request.node.name}")

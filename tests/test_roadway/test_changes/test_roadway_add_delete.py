import copy
import os

import pytest

import pandas as pd

from network_wrangler import ProjectCard
from network_wrangler import RoadwayNetwork
from network_wrangler import WranglerLogger

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 50000)

"""
Run just the tests labeled basic using `pytest tests/test_roadway/test_changes/test_roadway_add_delete.py`
"""

def test_add_roadway_link_project_card(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    
    _project = "Add Newsy Link"
    _category = "Add New Roadway"
    _links = [
        {
            'A': 3494,
            'B': 3230,
            'model_link_id':123456,
            'roadway':'busway',
            'walk_access':0,
            'drive_access':0,
            'bus_only':1,
            'rail_only':1,
            'bike_access':0,
            'lanes':1,
            'name': "new busway link"
        },
        {
            'A': 3230,
            'B': 3494,
            'model_link_id':123457,
            'roadway':'busway',
            'walk_access':0,
            'drive_access':0,
            'bus_only':1,
            'rail_only':1,
            'bike_access':0,
            'lanes':1,
            'name': "new busway link"
        },
    ]

    pc_dict = {
        "project":_project,
        "category":_category,
        "links":_links,
    }

    net = copy.deepcopy(small_net)
    net = net.apply(pc_dict)

    _new_link_idxs = [i['model_link_id'] for i in _links]
    _expected_new_link_fks = [(i["A"],i["B"]) for i in _links]
    _new_links = net.links_df.loc[_new_link_idxs]

    WranglerLogger.debug(f"New Links:\n{_new_links}")
    assert len(_new_links) == len(_links)

    assert set(list(zip(_new_links.A, _new_links.B))) == set(_expected_new_link_fks)

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_add_delete_roadway_project_card(request, stpaul_net, stpaul_ex_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(stpaul_net)
    project_cards_list = [
        "10_simple_roadway_add_change.yml",
        "11_multiple_roadway_add_and_delete_change.yml",
    ]

    for card_name in project_cards_list:
        print("Applying project card - ", card_name, "...")
        project_card_path = os.path.join(stpaul_ex_dir, "project_cards", card_name)
        project_card = ProjectCard.read(project_card_path, validate=False)

        orig_links_count = len(net.links_df)
        orig_nodes_count = len(net.nodes_df)

        missing_nodes = []
        missing_links = []

        project_card_dictionary = project_card.__dict__

        def _get_missing_nodes(project_card_dictionary):
            if project_card_dictionary[
                "category"
            ].lower() == "roadway deletion" and project_card_dictionary.get("nodes"):
                for key, val in project_card_dictionary["nodes"].items():
                    return [v for v in val if v not in net.nodes_df[key].tolist()]

        def _get_missing_links(project_card_dictionary):
            if project_card_dictionary[
                "category"
            ].lower() == "roadway deletion" and project_card_dictionary.get("links"):
                for key, val in project_card_dictionary["links"].items():
                    return [v for v in val if v not in net.links_df[key].tolist()]

        # count nodes that are in original network that should be deleted
        if not project_card_dictionary.get("changes"):
            m_n = _get_missing_nodes(project_card_dictionary)
            if m_n:
                missing_nodes += m_n

            m_l = _get_missing_links(project_card_dictionary)
            if m_l:
                missing_links += m_l
        else:
            for project_dictionary in project_card_dictionary["changes"]:
                m_n = _get_missing_nodes(project_dictionary)
                if m_n:
                    missing_nodes += m_n

                m_l = _get_missing_links(project_dictionary)
                if m_l:
                    missing_links += m_l

        net = net.apply(project_card.__dict__)

        rev_links_count = len(net.links_df)
        rev_nodes_count = len(net.nodes_df)

        def _count_add_or_delete_features(project_card_dictionary):
            _links_added = 0
            _nodes_added = 0
            _links_deleted = 0
            _nodes_deleted = 0

            if project_card_dictionary["category"].lower() == "add new roadway":
                if project_card_dictionary.get("links") is not None:
                    _links_added = len(project_card_dictionary["links"])
                if project_card_dictionary.get("nodes") is not None:
                    _nodes_added = len(project_card_dictionary["nodes"])

            if project_card_dictionary["category"].lower() == "roadway deletion":
                if project_card_dictionary.get("links") is not None:
                    print("links", project_card_dictionary["links"])
                    _links_deleted = sum(
                        len(project_card_dictionary["links"][key])
                        for key in project_card_dictionary["links"]
                    )
                if project_card_dictionary.get("nodes"):
                    print("nodes", project_card_dictionary["nodes"])
                    _nodes_deleted = sum(
                        len(project_card_dictionary["nodes"][key])
                        for key in project_card_dictionary["nodes"]
                    )
                    print("nodes_deleted:", _nodes_deleted)
                    print("NODES:", project_card_dictionary["nodes"])

            return {
                "links_added": _links_added,
                "nodes_added": _nodes_added,
                "links_deleted": _links_deleted,
                "nodes_deleted": _nodes_deleted,
            }

        links_added = 0
        links_deleted = 0
        nodes_added = 0
        nodes_deleted = 0

        if not project_card_dictionary.get("changes"):
            count_info = _count_add_or_delete_features(project_card_dictionary)
            links_added = count_info["links_added"]
            links_deleted = count_info["links_deleted"]
            nodes_added = count_info["nodes_added"]
            nodes_deleted = count_info["nodes_deleted"]
        else:
            for project_dictionary in project_card_dictionary["changes"]:
                count_info = _count_add_or_delete_features(project_dictionary)
                links_added += count_info["links_added"]
                links_deleted += count_info["links_deleted"]
                nodes_added += count_info["nodes_added"]
                nodes_deleted += count_info["nodes_deleted"]

        num_missing_nodes = len(set(missing_nodes))
        print("MISSING NODES:", num_missing_nodes)

        num_missing_links = len(set(missing_links))
        print("MISSING LINKS:", num_missing_links)

        net_links_network = rev_links_count - orig_links_count
        net_links_project_card = links_added - links_deleted + num_missing_links

        net_nodes_network = rev_nodes_count - orig_nodes_count
        net_nodes_project_card = nodes_added - nodes_deleted + num_missing_nodes
        assert net_links_network == net_links_project_card
        assert net_nodes_network == net_nodes_project_card

    print("--Finished:", request.node.name)


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
        project_card_dictionary.get("links",[]), project_card_dictionary.get("nodes",[])
    )

    print("--Finished:", request.node.name)


def test_delete_roadway_shape(request, stpaul_net, stpaul_ex_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(stpaul_net)

    project_card_name = "13_simple_roadway_delete_change.yml"
    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path, validate=False)
    project_card_dictionary = project_card.__dict__

    orig_links_count = len(net.links_df)
    orig_shapes_count = len(net.shapes_df)

    net.delete_roadway_feature_change(
        project_card_dictionary.get("links"), project_card_dictionary.get("nodes")
    )

    rev_links_count = len(net.links_df)
    rev_shapes_count = len(net.shapes_df)

    assert (orig_links_count - rev_links_count) == (
        orig_shapes_count - rev_shapes_count
    )
    assert orig_shapes_count > rev_shapes_count
    assert orig_links_count > rev_links_count

    print("--Finished:", request.node.name)


def test_create_default_geometry(request,stpaul_net, stpaul_ex_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(stpaul_net)

    project_card_name = "10_simple_roadway_add_change.yml"
    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path, validate=False)
    project_card_dictionary = project_card.__dict__

    net.add_new_roadway_feature_change(
        project_card_dictionary.get("links",[]), project_card_dictionary.get("nodes",[])
    )

    links_without_geometry = net.links_df[net.links_df["geometry"] == ""]

    assert len(links_without_geometry) == 0

    print("--Finished:", request.node.name)


def test_add_roadway_shape(request,stpaul_net,stpaul_ex_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(stpaul_net)

    project_card_name = "10_simple_roadway_add_change.yml"
    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path, validate=False)
    project_card_dictionary = project_card.__dict__

    orig_links_count = len(net.links_df)
    orig_shapes_count = len(net.shapes_df)

    net.add_new_roadway_feature_change(
        project_card_dictionary.get("links",[]), project_card_dictionary.get("nodes",[])
    )

    rev_links_count = len(net.links_df)
    rev_shapes_count = len(net.shapes_df)

    assert (rev_links_count - orig_links_count) == (
        rev_shapes_count - orig_shapes_count
    )
    assert rev_shapes_count == orig_shapes_count + 2
    assert rev_links_count == orig_links_count + 2

    WranglerLogger.info(f"--Finished: {request.node.name}" )


def test_add_nodes(request,small_net):
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

    WranglerLogger.debug(f"Added Node 1234567:\n{net.nodes_df.loc[net.nodes_df.model_node_id == 1234567]}" )

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
    WranglerLogger.info(f"--Finished: {request.node.name}" )

def test_change_node_xy(request,small_net):
    """Tests if X and Y property changes from a project card also update the node geometry."""
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

    WranglerLogger.info(f"Updated Node:\n{_test_node[['X','Y','geometry']]}")
    WranglerLogger.info(f"Updated Link Geometry for ({_test_link.A}-->{_test_link.B}):\n{_test_link.geometry}")

    assert _test_node.geometry.x == _expected_X
    assert _test_node.geometry.y == _expected_X
    assert _test_node.X == _expected_X
    assert _test_node.Y == _expected_Y
    assert _test_link.geometry[0].coords[0][0] == _expected_X
    
    WranglerLogger.info(f"--Finished: {request.node.name}" )

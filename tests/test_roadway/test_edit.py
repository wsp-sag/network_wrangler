"""Tests roadway network editing functions."""

import copy

import pandas as pd
import pytest

from network_wrangler.errors import LinkAddError, NodeAddError
from network_wrangler.logger import WranglerLogger
from network_wrangler.utils.models import TableValidationError


def test_add_nodes(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(small_net)
    og_node_count = len(net.nodes_df)
    # Create a DataFrame of nodes to add
    new_node_ids = [999, 888, 777]
    add_nodes_df = pd.DataFrame(
        {
            "model_node_id": new_node_ids,
            "X": [0, 1, 2],
            "Y": [0, 1, 2],
        }
    )

    # Add the nodes to the network
    net.add_nodes(add_nodes_df)

    # Check if the nodes were added correctly
    assert len(net.nodes_df) == og_node_count + len(new_node_ids)
    # WranglerLogger.debug(f"Nodes:\n{net.nodes_df}")
    new_nodes_in_nodes_df = net.nodes_df.loc[new_node_ids]

    assert len(new_nodes_in_nodes_df) == len(new_node_ids)

    # should raise an error if try to add again.
    with pytest.raises(NodeAddError):
        net.add_nodes(add_nodes_df)
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_add_links(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(small_net)
    og_link_count = len(net.links_df)
    new_link_ids = [999, 888]
    # Create a DataFrame of additional links to add
    add_links_df = pd.DataFrame(
        {
            "model_link_id": new_link_ids,
            "A": [8, 3],
            "B": [3, 8],
            "name": ["Link 1", "Link 2"],
            "lanes": [1, 1],
        }
    )

    # Add the additional links to the RoadwayNetwork
    net.add_links(add_links_df)

    new_links_in_links_df = net.links_df.loc[new_link_ids]
    WranglerLogger.debug(f"Added Links: \n{new_links_in_links_df}")

    # Check if the nodes were added correctly
    assert len(net.links_df) == og_link_count + len(new_link_ids)
    assert len(new_links_in_links_df) == len(new_link_ids)

    # should raise an error if try to add again.
    with pytest.raises(LinkAddError):
        net.add_links(add_links_df)

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_add_shapes(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(small_net)
    new_shape_ids = [1, 2]
    add_shapes_df = pd.DataFrame(
        {
            "shape_id": new_shape_ids,
            "geometry": ["LINESTRING (0 0, 1 1)", "LINESTRING (1 1, 2 2)"],
        }
    )

    # Call the add_shapes method
    net.add_shapes(add_shapes_df)

    # Assert that the shapes_df property has been updated with the added shapes
    WranglerLogger.debug(f"Shapes: \n{net.shapes_df}")
    assert len(net.shapes_df.loc[new_shape_ids]) == len(new_shape_ids)

    # should raise an error if try to add again.
    with pytest.raises(TableValidationError):
        net.add_shapes(add_shapes_df)

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_delete_links_by_name(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(small_net)
    # Define a selection dictionary for deleting links
    selection_dict = {
        "all": True,
        "name": ["7th St"],
    }

    # Delete the links based on the selection dictionary
    net.delete_links(selection_dict)
    WranglerLogger.debug(f"net.links_df: \n{net.links_df}")
    # Check if the links are deleted from the links dataframe
    assert len(net.links_df[net.links_df.name.str.contains("7th St")]) == 0
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_delete_links_by_id_with_associated_nodes(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(small_net)
    del_node_ids = [1, 2]
    del_link_ids = net.links_with_nodes(del_node_ids).model_link_id.to_list()
    # Define a selection dictionary for deleting links
    selection_dict = {
        "model_link_id": del_link_ids,
    }
    net.delete_links(selection_dict, clean_nodes=True)
    WranglerLogger.debug(f"net.nodes_df: \n{net.nodes_df}")
    WranglerLogger.debug(f"net.links_df: \n{net.links_df}")
    # Check if the links are deleted from the links dataframe
    assert not any(link_id in net.links_df.model_link_id for link_id in del_link_ids)

    WranglerLogger.debug(f"net.nodes_df: \n{net.nodes_df}")
    # Check if the nodes associated with the deleted links are also deleted
    assert not any(node_id in net.nodes_df.model_node_id for node_id in del_node_ids)

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_delete_nodes(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(small_net)
    del_node_ids = [999, 888, 777]
    add_nodes_df = pd.DataFrame(
        {
            "model_node_id": del_node_ids,
            "X": [0, 1, 2],
            "Y": [0, 1, 2],
        }
    )
    net.add_nodes(add_nodes_df)
    if sum(net.nodes_df.model_node_id.isin(del_node_ids)) != len(del_node_ids):
        msg = "Nodes not added correctly"
    # Define the selection dictionary for nodes to delete
    selection_dict = {
        "model_node_id": del_node_ids,
        "ignore_missing": False,
    }
    net.delete_nodes(selection_dict)

    # Check if the nodes were deleted
    assert not any(node_id in net.nodes_df.model_node_id for node_id in del_node_ids)
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_move_nodes(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(small_net)
    moved_node_ids = [1, 2]
    # Define the node geometry change table
    new_geo = pd.DataFrame(
        {
            "model_node_id": moved_node_ids,
            "X": [10, 20],
            "Y": [10, 20],
        }
    )

    # Apply the move_nodes method
    net.move_nodes(new_geo)
    new_geo.set_index("model_node_id", inplace=True)
    # Define the expected results after moving the nodes
    for node_id in moved_node_ids:
        WranglerLogger.debug(
            f"Node ID: {node_id}: \n{net.nodes_df.loc[node_id, ['X', 'Y', 'geometry']]}"
        )
        assert net.nodes_df.loc[node_id, "X"] == new_geo.loc[node_id, "X"]
        assert net.nodes_df.loc[node_id, "Y"] == new_geo.loc[node_id, "Y"]
        assert net.nodes_df.loc[node_id, "geometry"].x == new_geo.loc[node_id, "X"]
        assert net.nodes_df.loc[node_id, "geometry"].y == new_geo.loc[node_id, "Y"]

    # Check if the links are updated correctly
    moved_A_link_ids = net.links_df.loc[
        net.links_df.A.isin(moved_node_ids)
    ].model_link_id.to_list()
    for link_id in moved_A_link_ids:
        WranglerLogger.debug(
            f"link ID: {link_id}: \n{net.links_df.loc[link_id, ['A', 'geometry']]}"
        )
        a_id = net.links_df.loc[link_id, "A"]
        assert net.links_df.loc[link_id, "geometry"].coords[0][0] == new_geo.loc[a_id, "X"]
        assert net.links_df.loc[link_id, "geometry"].coords[0][1] == new_geo.loc[a_id, "Y"]

    moved_B_link_ids = net.links_df.loc[
        net.links_df.B.isin(moved_node_ids)
    ].model_link_id.to_list()
    for link_id in moved_B_link_ids:
        WranglerLogger.debug(
            f"link ID: {link_id}: \n{net.links_df.loc[link_id, ['B', 'geometry']]}"
        )
        b_id = net.links_df.loc[link_id, "B"]
        assert net.links_df.loc[link_id, "geometry"].coords[-1][0] == new_geo.loc[b_id, "X"]
        assert net.links_df.loc[link_id, "geometry"].coords[-1][1] == new_geo.loc[b_id, "Y"]

    WranglerLogger.info(f"--Finished: {request.node.name}")

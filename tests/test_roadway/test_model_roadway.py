import os
import time

import pytest

import pandas as pd

from network_wrangler import ProjectCard
from network_wrangler import RoadwayNetwork
from network_wrangler.roadway import create_managed_lane_network
from network_wrangler.roadway.model_roadway import (
    _link_id_to_managed_lane_link_id,
    _access_model_link_id,
    _egress_model_link_id,
)
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


@pytest.mark.menow
@pytest.mark.roadway
@pytest.mark.travis
def test_add_adhoc_managed_lane_field(request):
    """
    Makes sure new fields can be added to the network for managed lanes that get moved there.
    """
    from network_wrangler.roadway import create_managed_lane_network

    print("\n--Starting:", request.node.name)
    net = _read_small_net()

    facility = {"links": [{"model_link_id": 224}]}
    selected_link_indices = net.select_roadway_features(facility)
    net.links_df["ML_my_ad_hoc_field"] = 0
    net.links_df["ML_my_ad_hoc_field"].loc[selected_link_indices] = 22.5
    net.links_df["ML_lanes"] = 0
    net.links_df["ML_lanes"].loc[selected_link_indices] = 1
    net.links_df["ML_price"] = 0
    net.links_df["ML_price"].loc[selected_link_indices] = 1.5
    net.links_df["managed"] = 0
    net.links_df["managed"].loc[selected_link_indices] = 1
    print(
        "Network with field...\n ",
        net.links_df[
            [
                "model_link_id",
                "name",
                "ML_my_ad_hoc_field",
                "lanes",
                "ML_lanes",
                "ML_price",
                "managed",
            ]
        ],
    )

    net = create_managed_lane_network(net)
    print("Managed Lane Network")
    print(
        net.m_links_df[["model_link_id", "name", "my_ad_hoc_field", "lanes", "price"]]
    )
    # assert net.links_df["my_ad_hoc_field"][0] == 22.5
    # print("CALCULATED:\n", v_series.loc[selected_link_indices])


@pytest.mark.menow
@pytest.mark.roadway
@pytest.mark.travis
def test_add_adhoc_managed_lane_field(request):
    """
    Makes sure new fields can be added to the network for managed lanes that get moved there.
    """
    print("\n--Starting:", request.node.name)
    net = _read_small_net()

    facility = {"links": [{"model_link_id": 224}]}
    selected_link_indices = net.select_roadway_features(facility)
    net.links_df["ML_my_ad_hoc_field"] = 0
    net.links_df["ML_my_ad_hoc_field"].loc[selected_link_indices] = 22.5
    net.links_df["ML_lanes"] = 0
    net.links_df["ML_lanes"].loc[selected_link_indices] = 1
    net.links_df["ML_price"] = 0
    net.links_df["ML_price"].loc[selected_link_indices] = 1.5
    net.links_df["managed"] = 0
    net.links_df["managed"].loc[selected_link_indices] = 1
    print(
        "Network with field...\n ",
        net.links_df[
            [
                "model_link_id",
                "name",
                "ML_my_ad_hoc_field",
                "lanes",
                "ML_lanes",
                "ML_price",
                "managed",
            ]
        ],
    )
    net = create_managed_lane_network(net)
    print("Managed Lane Network")
    print(
        net.m_links_df[["model_link_id", "name", "my_ad_hoc_field", "lanes", "price"]]
    )
    # assert net.links_df["my_ad_hoc_field"][0] == 22.5
    # print("CALCULATED:\n", v_series.loc[selected_link_indices])


@pytest.mark.menow
@pytest.mark.roadway
@pytest.mark.travis
def test_bad_properties_statements(request):
    """
    Makes sure new fields can be added from a project card and that
    they will be the right type.
    """

    print("\n--Starting:", request.node.name)
    net = _read_stpaul_net()
    ok_properties_change = [{"property": "lanes", "change": 1}]
    bad_properties_change = [{"property": "my_random_var", "change": 1}]
    bad_properties_existing = [{"property": "my_random_var", "existing": 1}]

    with pytest.raises(ValueError):
        net.validate_properties(bad_properties_change)

    with pytest.raises(ValueError):
        net.validate_properties(ok_properties_change, require_existing_for_change=True)

    with pytest.raises(ValueError):
        net.validate_properties(bad_properties_existing, ignore_existing=False)

    print("--Finished:", request.node.name)


@pytest.mark.menow
@pytest.mark.roadway
@pytest.mark.travis
def test_write_model_net(request):
    print("\n--Starting:", request.node.name)
    from network_wrangler.roadway import create_managed_lane_network

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

    net = net.apply(project_card.__dict__)
    net.links_df.to_csv(os.path.join(SCRATCH_DIR, "in_ml_links.csv"), index=False)
    net.nodes_df.to_csv(os.path.join(SCRATCH_DIR, "in_ml_nodes.csv"), index=False)
    net.shapes_df.to_csv(os.path.join(SCRATCH_DIR, "in_ml_shape.csv"), index=False)

    net = create_managed_lane_network(net)
    net.m_links_df.to_csv(os.path.join(SCRATCH_DIR, "out_ml_links.csv"), index=False)
    net.m_nodes_df.to_csv(os.path.join(SCRATCH_DIR, "out_ml_nodes.csv"), index=False)
    net.m_shapes_df.to_csv(os.path.join(SCRATCH_DIR, "out_ml_shape.csv"), index=False)

    print("--Finished:", request.node.name)


@pytest.mark.menow
@pytest.mark.travis
@pytest.mark.roadway
def test_create_ml_network_shape(request):
    print("\n--Starting:", request.node.name)
    from network_wrangler.roadway import create_managed_lane_network

    print("Reading network ...")
    net = _read_stpaul_net()

    print("Reading project card ...")
    project_card_name = "4_simple_managed_lane.yml"
    project_card_path = os.path.join(STPAUL_DIR, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path, validate=False)
    project_card_dictionary = project_card.__dict__

    _orig_links_count = len(net.links_df)
    _orig_shapes_count = len(net.shapes_df)

    net = net.apply(project_card_dictionary)
    net = create_managed_lane_network(net)

    base_model_link_ids = project_card.__dict__["facility"]["links"][0]["model_link_id"]
    ml_model_link_ids = [
        RoadwayNetwork.MANAGED_LANES_LINK_ID_SCALAR + x for x in base_model_link_ids
    ]
    access_model_link_ids = [
        sum(x) + 1 for x in zip(base_model_link_ids, ml_model_link_ids)
    ]
    egress_model_link_ids = [
        sum(x) + 2 for x in zip(base_model_link_ids, ml_model_link_ids)
    ]

    gp_links = net.m_links_df[net.m_links_df["model_link_id"].isin(base_model_link_ids)]
    ml_links = net.m_links_df[net.m_links_df["model_link_id"].isin(ml_model_link_ids)]
    access_links = net.m_links_df[
        net.m_links_df["model_link_id"].isin(access_model_link_ids)
    ]
    egress_links = net.m_links_df[
        net.m_links_df["model_link_id"].isin(egress_model_link_ids)
    ]

    _num_added_links = len(net.links_df) - _orig_links_count
    _num_added_shapes = len(net.shapes_df) - _orig_shapes_count

    ## 1 Num Added links == added shapes
    assert _num_added_links == _num_added_shapes

    # 2 new ML links, each ML link has 2 more access/egress links for total of 3 links per ML link
    # total new links for 2 ML links will be 6 (2*3)
    _display_c = ["model_link_id", "roadway", "A", "B", "shape_id", "name"]
    assert (
        len(net.m_links_df[net.m_links_df["model_link_id"].isin(ml_model_link_ids)])
        == 2
    ), f"\n***ML Links\n{ml_links[_display_c]}\
        \n***ML Links\n{ml_links[_display_c]}\
        \n***GP Links\n{gp_links[_display_c]}"

    assert (
        len(net.m_links_df[net.m_links_df["model_link_id"].isin(access_model_link_ids)])
        == 2
    ), f"\n***Access Links\n{access_links[_display_c]}\
        \n***ML Links\n{ml_links[_display_c]}\
        \n***GP Links\n{gp_links[_display_c]}"

    assert (
        len(net.m_links_df[net.m_links_df["model_link_id"].isin(egress_model_link_ids)])
        == 2
    ), f"\n***Egress Links\n{egress_links[_display_c]}\
        \n***ML Links\n{ml_links[_display_c]}\
        \n***GP Links\n{gp_links[_display_c]}"

    print("--Finished:", request.node.name)


@pytest.mark.menow
@pytest.mark.roadway
@pytest.mark.travis
def test_managed_lane_restricted_access_egress(request):
    print("\n--Starting:", request.node.name)
    net = _read_stpaul_net()
    print("Reading project card ...")
    # project_card_name = "test_managed_lanes_change_keyword.yml"
    project_card_name = "test_managed_lanes_restricted_access_egress.yml"
    project_card_path = os.path.join(STPAUL_DIR, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path, validate=False)

    net.apply_managed_lane_feature_change(
        net.select_roadway_features(project_card.facility), project_card.properties
    )
    WranglerLogger.debug(f"{len(net.nodes_df)} Nodes in network")
    net = create_managed_lane_network(net)

    # with 'all' as access/egress, there would be total of 8 connector links (4 access, 4 egress)
    # with restricted access/egress, this project card should create 4 connector links

    dummy_links_df = net.m_links_df[
        (net.m_links_df["roadway"].isin(["ml_access", "ml_egress"]))
    ]
    dummy_links_count = len(dummy_links_df)
    WranglerLogger.debug(f"Dummy Links:\n {dummy_links_df}")

    pcard_gp_link_ids = project_card.__dict__["facility"]["links"][0]["model_link_id"]
    pcard_access_points = [
        p["set"]
        for p in project_card.__dict__["properties"]
        if p["property"] == "ML_access_point"
    ][0]

    pcard_egress_points = [
        p["set"]
        for p in project_card.__dict__["properties"]
        if p["property"] == "ML_egress_point"
    ][0]

    expected_ml_link_ids = [
        _link_id_to_managed_lane_link_id(x) for x in pcard_gp_link_ids
    ]
    expected_access_link_ids = [_access_model_link_id(x) for x in pcard_gp_link_ids]
    expected_egress_link_ids = [_egress_model_link_id(x) for x in pcard_gp_link_ids]

    net_gp_links = net.m_links_df.loc[net.m_links_df["managed"] == -1]
    net_ml_links = net.m_links_df.loc[net.m_links_df["managed"] == 1]
    net_access_links = net.m_links_df.loc[net.m_links_df["roadway"] == "ml_access"]
    net_egress_links = net.m_links_df.loc[net.m_links_df["roadway"] == "ml_egress"]

    _display_c = ["model_link_id", "roadway", "A", "B", "shape_id", "name"]

    WranglerLogger.debug(
        f"\n***ML Links\n{net_ml_links[_display_c]}\
        \n***Expected ML Link IDs\n{expected_ml_link_ids}\
        \n***GP Links\n{net_gp_links[_display_c]}\
        \n***Access Links\n{net_access_links[_display_c]}\
        \n***Expected Access Points\n{pcard_access_points}\
        \n***Expected Access Link IDs\n{expected_access_link_ids}\
        \n***Egress Links\n{net_egress_links[_display_c]}\
        \n***Expected Egress Points\n{pcard_egress_points}\
        \n***Expected Egress Link IDs\n{expected_egress_link_ids}\
        "
    )

    # Assert managed lane link IDs are expected
    assert set(net_ml_links["model_link_id"].tolist()) == set(expected_ml_link_ids)
    assert set(net_access_links["model_link_id"].tolist()) == set(
        expected_access_link_ids
    )
    assert set(net_egress_links["model_link_id"].tolist()) == set(
        expected_egress_link_ids
    )

    print("--Finished:", request.node.name)

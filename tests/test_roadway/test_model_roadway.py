import copy
import os

from projectcard import ProjectCard
from network_wrangler.roadway import ModelRoadwayNetwork
from network_wrangler import WranglerLogger


"""
Run just the tests labeled basic using `pytest tests/test_roadway/test_model_roadway.py`
To run with print statments, use `pytest -s tests/test_roadway/test_model_roadway.py`
"""


def test_add_adhoc_managed_lane_field(request, small_net):
    """
    Makes sure new fields can be added to the network for managed lanes that get moved there.
    """
    WranglerLogger.info(f"--Starting: {request.node.name}")

    AD_HOC_VALUE = 22.5
    SELECTED_LINK_INDEX = 1

    net = copy.deepcopy(small_net)

    net.links_df["ML_my_ad_hoc_field"] = 0
    net.links_df["ML_my_ad_hoc_field"].iloc[SELECTED_LINK_INDEX] = AD_HOC_VALUE
    net.links_df["ML_lanes"] = 0
    net.links_df["ML_lanes"].iloc[SELECTED_LINK_INDEX] = 1
    net.links_df["ML_price"] = 0
    net.links_df["ML_price"].iloc[SELECTED_LINK_INDEX] = 1.5
    net.links_df["managed"] = 0
    net.links_df["managed"].iloc[SELECTED_LINK_INDEX] = 1

    _model_link_id = net.links_df["model_link_id"].iloc[SELECTED_LINK_INDEX]
    
    WranglerLogger.debug(
        f"model_link_id: {_model_link_id}\nml_model_link_id: {_ml_model_link_id}"
    )

    _display_cols = [
        "model_link_id",
        "name",
        "ML_my_ad_hoc_field",
        "lanes",
        "ML_lanes",
        "ML_price",
        "managed",
    ]
    WranglerLogger.debug(f"Network with field.\n{net.links_df[ _display_cols]}")

    m_net = net.model_net
    _ml_model_link_id = m_net._link_id_to_managed_lane_link_id(_model_link_id)

    _display_cols = ["model_link_id", "name", "my_ad_hoc_field", "lanes", "price"]
    WranglerLogger.debug(f"Managed Lane Network\n{m_net.m_links_df[_display_cols]}")

    _managed_lane_record = m_net.m_links_df.loc[
        m_net.m_links_df["model_link_id"] == _ml_model_link_id
    ]
    _managed_lane_record = _managed_lane_record.iloc[0]

    WranglerLogger.debug(f"Managed Lane Record\n{_managed_lane_record[_display_cols]}")

    assert _managed_lane_record["my_ad_hoc_field"] == AD_HOC_VALUE
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_create_ml_network_shape(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(small_net)

    # Set facility selection
    _model_link_ids = net.links_df["model_link_id"].iloc[1:2].tolist()
    _facility = {"links": [{"model_link_id": _model_link_ids}]}

    # Set ML Properties
    _lanes_p = {
        "property": "lanes",
        "set": 3,
        "timeofday": [{"time": ["6:00", "9:00"], "set": 2}],
    }

    _ML_lanes_p = {
        "property": "lanes",
        "set": 0,
        "timeofday": [
            {
                "time": ["6:00", "9:00"],
                "set": 1,
            }
        ],
    }

    _segment_id_p = {"property": "segment_id", "set": 5}
    _ML_HOV_p = {"property": "ML_HOV", "set": 5}
    _HOV_p = {"property": "HOV", "set": 5}
    _ML_access_p = {"property": "ML_access", "set": "all"}
    _ML_egress_p = {"property": "ML_egress", "set": "all"}

    _properties = [
        _lanes_p,
        _ML_lanes_p,
        _segment_id_p,
        _ML_HOV_p,
        _HOV_p,
        _ML_access_p,
        _ML_egress_p,
    ]

    project_card_dictionary = {
        "project": "test managed lane project",
        "category": "Parallel Managed lanes",
        "facility": _facility,
        "properties": _properties,
    }

    _orig_links_count = len(net.links_df)
    _orig_shapes_count = len(net.shapes_df)

    net = net.apply(project_card_dictionary)

    base_model_link_ids = project_card_dictionary["facility"]["links"][0][
        "model_link_id"
    ]
    ml_model_link_ids = [net.model_net.managed_lanes_link_id_scalar+ x for x in base_model_link_ids]
    access_model_link_ids = [
        sum(x) + 1 for x in zip(base_model_link_ids, ml_model_link_ids)
    ]
    egress_model_link_ids = [
        sum(x) + 2 for x in zip(base_model_link_ids, ml_model_link_ids)
    ]

    gp_links = net.model_net.m_links_df[
        net.model_net.m_links_df["model_link_id"].isin(base_model_link_ids)
    ]
    ml_links = net.model_net.m_links_df[
        net.model_net.m_links_df["model_link_id"].isin(ml_model_link_ids)
    ]
    access_links = net.m_links_df[
        net.model_net.m_links_df["model_link_id"].isin(access_model_link_ids)
    ]
    egress_links = net.m_links_df[
        net.model_net.m_links_df["model_link_id"].isin(egress_model_link_ids)
    ]

    _num_added_links = len(net.links_df) - _orig_links_count
    _num_added_shapes = len(net.shapes_df) - _orig_shapes_count

    # 1 Num Added links == added shapes
    assert _num_added_links == _num_added_shapes

    # 2 new ML links, each ML link has 2 more access/egress links for total of 3 links per ML link
    # total new links for 2 ML links will be 6 (2*3)
    _display_c = ["model_link_id", "roadway", "A", "B", "shape_id", "name"]
    WranglerLogger.debug(
        f"\n***ML Link IDs\n{ml_model_link_ids}\
        \n***ML Links\n{ml_links[_display_c]}\
        \n***GP Links\n{gp_links[_display_c]}\
        \n***Access Link IDs\n{access_model_link_ids}\
        \n***Access Links\n{access_links[_display_c]}\
        \n***Egress Link IDs\n{egress_model_link_ids}\
        \n***Egress Links\n{egress_links[_display_c]}"
    )
    assert len(
        net.model_net.m_links_df[
            net.model_net.m_links_df["model_link_id"].isin(ml_model_link_ids)
        ]
    ) == len(ml_model_link_ids)

    assert len(
        net.model_net.m_links_df[
            net.model_net.m_links_df["model_link_id"].isin(access_model_link_ids)
        ]
    ) == len(access_model_link_ids)

    assert len(
        net.model_net.m_links_df[
            net.model_net.m_links_df["model_link_id"].isin(egress_model_link_ids)
        ]
    ) == len(egress_model_link_ids)

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_managed_lane_restricted_access_egress(request, stpaul_net, stpaul_ex_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(stpaul_net)

    # project_card_name = "test_managed_lanes_change_keyword.yml"
    project_card_name = "test_managed_lanes_restricted_access_egress.yml"
    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path, validate=False)

    net.apply_managed_lane_feature_change(
        net.select_roadway_features(project_card.facility), project_card.properties
    )
    WranglerLogger.debug(f"{len(net.nodes_df)} Nodes in network")

    # with 'all' as access/egress, there would be total of 8 connector links (4 access, 4 egress)
    # with restricted access/egress, this project card should create 4 connector links

    dummy_links_df = net.m_links_df[
        (net.model_net.m_links_df["roadway"].isin(["ml_access", "ml_egress"]))
    ]

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
        net.model_net._link_id_to_managed_lane_link_id(x) for x in pcard_gp_link_ids
    ]
    expected_access_link_ids = [net.model_net._access_model_link_id(x) for x in pcard_gp_link_ids]
    expected_egress_link_ids = [net.model_net._egress_model_link_id(x) for x in pcard_gp_link_ids]

    net_gp_links = net.model_net.m_links_df.loc[net.m_links_df["managed"] == -1]
    net_ml_links = net.model_net.m_links_df.loc[net.m_links_df["managed"] == 1]
    net_access_links = net.model_netm_links_df.loc[
        net.m_links_df["roadway"] == "ml_access"
    ]
    net_egress_links = net.model_netm_links_df.loc[
        net.m_links_df["roadway"] == "ml_egress"
    ]

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

    WranglerLogger.info(f"--Finished: {request.node.name}")

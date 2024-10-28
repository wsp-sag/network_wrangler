"""Tests functions for separating managed lanes from general purpose lanes as separate links.

Run just the tests in this file `pytest tests/test_roadway/test_model_roadway.py`
"""

import copy

import pytest
from projectcard import read_card

from network_wrangler import WranglerLogger


def test_add_adhoc_managed_lane_field(request, small_net):
    """Makes sure new fields can be added to the network for managed lanes that get moved there."""
    WranglerLogger.info(f"--Starting: {request.node.name}")
    MODEL_LINK_ID = 112
    AD_HOC_PROP = "my_ad_hoc_field"
    AD_HOC_VALUE = 22.5
    _facility = {"links": {"model_link_id": [MODEL_LINK_ID]}}
    _managed_lane = {
        "facility": _facility,
        "property_changes": {
            "ML_lanes": {"set": 1},
            "ML_" + AD_HOC_PROP: {"set": AD_HOC_VALUE},
        },
    }
    _project_card_dict = {"project": "test", "roadway_property_change": _managed_lane}

    net = copy.deepcopy(small_net)
    net = net.apply(_project_card_dict)

    _selection = net.get_selection(_facility)

    _display_cols = [
        "model_link_id",
        "name",
        "ML_my_ad_hoc_field",
        "lanes",
        "ML_lanes",
        "managed",
    ]
    WranglerLogger.debug(f"Applied links\n{_selection.selected_links_df}")

    m_net = net.model_net

    _ml_model_link_id = m_net.ml_link_id_lookup[MODEL_LINK_ID]
    WranglerLogger.debug(f"model_link_id: {MODEL_LINK_ID}\nml_model_link_id: {_ml_model_link_id}")
    _display_cols = ["model_link_id", "name", "my_ad_hoc_field", "lanes"]
    WranglerLogger.debug(f"\nManaged Lane Network\n{m_net.links_df[_display_cols]}")

    _managed_lane_record = m_net.links_df.loc[m_net.links_df["model_link_id"] == _ml_model_link_id]
    _managed_lane_record = _managed_lane_record.iloc[0]

    WranglerLogger.debug(
        f"\nManaged Lane Record with ad-hoc field\n{_managed_lane_record[_display_cols]}"
    )

    assert _managed_lane_record["my_ad_hoc_field"] == AD_HOC_VALUE
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_create_ml_network_shape(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(small_net)

    # Set facility selection
    _model_link_ids = [115]
    _facility = {"links": {"model_link_id": _model_link_ids}}

    # Set ML Properties
    _lanes_p = {
        "set": 3,
        "scoped": [{"timespan": ["6:00", "9:00"], "set": 2}],
    }

    _ML_lanes_p = {
        "set": 0,
        "scoped": [
            {
                "timespan": ["6:00", "9:00"],
                "set": 1,
            }
        ],
    }

    _properties = {
        "lanes": _lanes_p,
        "ML_lanes": _ML_lanes_p,
        "segment_id": {"set": 5},
        "ML_HOV": {"set": 5},
        "HOV": {"set": 5},
        "ML_access_point": {"set": "all"},
        "ML_egress_point": {"set": "all"},
    }

    project_card_dictionary = {
        "project": "test managed lane project",
        "roadway_property_change": {
            "facility": _facility,
            "property_changes": _properties,
        },
    }

    net = net.apply(project_card_dictionary)

    model_net = net.model_net

    base_model_link_ids = _facility["links"]["model_link_id"]
    ml_model_link_ids = [model_net.ml_link_id_lookup[x] for x in base_model_link_ids]

    gp_links = model_net.links_df[model_net.links_df["model_link_id"].isin(base_model_link_ids)]
    ml_links = model_net.links_df[net.model_net.links_df["model_link_id"].isin(ml_model_link_ids)]
    access_links = net.model_net.links_df[model_net.links_df["roadway"] == "ml_access_point"]
    egress_links = net.model_net.links_df[model_net.links_df["roadway"] == "ml_egress_point"]

    # CHECK: new ML links, each ML link has 2 more acc/egr links for total of 3 links per ML link
    # total new links for 2 ML links will be 6 (2*3)
    _display_c = ["model_link_id", "roadway", "A", "B", "shape_id", "name"]
    WranglerLogger.debug(
        f"\n***ML Link IDs\n{ml_model_link_ids}\
        \n***ML Links\n{ml_links[_display_c]}\
        \n***GP Links\n{gp_links[_display_c]}\
        \n***Access Link IDs\n{access_links.model_link_id}\
        \n***Access Links\n{access_links[_display_c]}\
        \n***Egress Link IDs\n{egress_links.model_link_id}\
        \n***Egress Links\n{egress_links[_display_c]}"
    )
    assert len(
        net.model_net.links_df[net.model_net.links_df["model_link_id"].isin(ml_model_link_ids)]
    ) == len(ml_model_link_ids)

    assert len(
        net.model_net.links_df[
            net.model_net.links_df["model_link_id"].isin(access_links.model_link_id)
        ]
    ) == len(access_links)

    assert len(
        net.model_net.links_df[
            net.model_net.links_df["model_link_id"].isin(egress_links.model_link_id)
        ]
    ) == len(egress_links)

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_managed_lane_restricted_access_egress(request, stpaul_net, stpaul_ex_dir):
    """Tests usage of ML_access/egress_point when they are set to a list of nodes instead of "all".

    - With 'all' as access/egress, there would be total of 8 connector links (4 access, 4 egress)
    - With restricted access/egress, this project card should create 4 connector links
    """
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(stpaul_net)

    project_card_name = "road.managed_lanes.restricted_access.yml"
    """
    ML_access_point:
      set: [38765, 87982]
    ML_egress_point:
      set: [87993, 37457]
    """
    project_card_path = stpaul_ex_dir / "project_cards" / project_card_name
    project_card = read_card(project_card_path, validate=False)

    net.apply(project_card)
    WranglerLogger.debug(f"{len(net.nodes_df)} Nodes in network")
    _m_links_df = net.model_net.links_df
    dummy_links_df = _m_links_df.of_type.dummy

    WranglerLogger.debug(f"Dummy Links: \n {dummy_links_df[['model_link_id', 'A', 'B']]}")

    pcard_gp_link_ids = project_card.roadway_property_change["facility"]["links"]["model_link_id"]
    pcard_access_points = project_card.roadway_property_change["property_changes"][
        "ML_access_point"
    ]["set"]
    pcard_egress_points = project_card.roadway_property_change["property_changes"][
        "ML_egress_point"
    ]["set"]

    expected_ml_link_ids = [net.model_net.ml_link_id_lookup[x] for x in pcard_gp_link_ids]

    net_gp_links = _m_links_df.of_type.parallel_general_purpose
    net_ml_links = _m_links_df.of_type.managed
    net_access_links = _m_links_df.of_type.access_dummy
    net_egress_links = _m_links_df.of_type.egress_dummy

    _display_c = ["model_link_id", "roadway", "A", "B", "shape_id", "name"]

    WranglerLogger.debug(
        f"\n***ML Links\n{net_ml_links[_display_c]}\
        \n***Expected ML Link IDs\n{expected_ml_link_ids}\
        \n***GP Links\n{net_gp_links[_display_c]}\
        \n***Access Links\n{net_access_links[_display_c]}\
        \n***Expected Access Points\n{pcard_access_points}\
        \n***Egress Links\n{net_egress_links[_display_c]}\
        \n***Expected Egress Points\n{pcard_egress_points}\
        "
    )

    # Assert managed lane link IDs are expected
    assert set(net_ml_links["model_link_id"].tolist()) == set(expected_ml_link_ids)
    assert set(net_access_links["A"].tolist()) == set(pcard_access_points)
    assert set(net_egress_links["B"].tolist()) == set(pcard_egress_points)

    WranglerLogger.info(f"--Finished: {request.node.name}")

import copy

import pandas as pd
import pytest
from projectcard import read_card

from network_wrangler import WranglerLogger
from network_wrangler.models.roadway.types import ScopedLinkValueItem

"""
Usage:  `pytest tests/test_roadway/test_changes/test_managed_lanes.py`
"""
pd.set_option("display.max_colwidth", None)
SIMPLE_MANAGED_LANE_PROPERTIES = {
    "lanes": {
        "set": 3,
        "scoped": [
            {"timespan": ["6:00", "9:00"], "set": 2},
            {"timespan": ["16:00", "19:00"], "set": 2},
        ],
    },
    "ML_lanes": {
        "set": 0,
        "scoped": [
            {"timespan": ["6:00", "9:00"], "set": 1},
            {"timespan": ["16:00", "19:00"], "set": 1},
        ],
    },
    "ML_access_point": {"set": "all"},
    "ML_egress_point": {"set": "all"},
    "ML_price": {
        "set": 0,
        "scoped": [
            {"category": "sov", "timespan": ["6:00", "9:00"], "set": 1.5},
            {"category": "sov", "timespan": ["16:00", "19:00"], "set": 2.5},
            {"category": "hov2", "timespan": ["6:00", "9:00"], "set": 1},
            {"category": "hov2", "timespan": ["16:00", "19:00"], "set": 2},
        ],
    },
}


def test_add_managed_lane(request, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    # Set facility selection
    _facility = {
        "links": {"name": ["I 35E"]},
        "from": {"osm_node_id": "961117623"},
        "to": {"osm_node_id": "2564047368"},
    }

    _properties = SIMPLE_MANAGED_LANE_PROPERTIES
    _project_card_dict = {
        "project": "test simple managed lanes",
        "roadway_property_change": {
            "facility": _facility,
            "property_changes": _properties,
        },
    }
    _am_period = ["6:00", "9:00"]
    _pm_period = ["16:00", "19:00"]
    _expected_property_values = {
        "managed": 1,
        "lanes": 3,
        "sc_lanes": [
            ScopedLinkValueItem(timespan=_am_period, value=2),
            ScopedLinkValueItem(timespan=_pm_period, value=2),
        ],
        "ML_lanes": 0,
        "sc_ML_lanes": [
            ScopedLinkValueItem(timespan=_am_period, value=1),
            ScopedLinkValueItem(timespan=_pm_period, value=1),
        ],
        "ML_access_point": True,
        "ML_egress_point": True,
        "ML_price": 0,
        "sc_ML_price": [
            ScopedLinkValueItem(category="sov", timespan=_am_period, value=1.5),
            ScopedLinkValueItem(category="sov", timespan=_pm_period, value=2.5),
            ScopedLinkValueItem(category="hov2", timespan=_am_period, value=1),
            ScopedLinkValueItem(category="hov2", timespan=_pm_period, value=2),
        ],
    }

    net = copy.deepcopy(stpaul_net)

    _selected_link_idx = net.get_selection(_facility).selected_links
    _p_to_track = list(_properties.keys())

    _orig_links = copy.deepcopy(
        net.links_df.loc[_selected_link_idx, net.links_df.columns.intersection(_p_to_track)]
    )
    WranglerLogger.debug(f"_orig_links: \n{_orig_links}")

    # apply change
    net = net.apply(_project_card_dict)

    _rev_links = net.links_df.loc[_selected_link_idx, list(_expected_property_values.keys())]
    WranglerLogger.debug(f"_rev_links: \n{_rev_links.iloc[0]}")
    pass_test = True
    _non_scoped_expected_property_values = {
        k: v for k, v in _expected_property_values.items() if not k.startswith("sc_")
    }
    _scoped_expected_property_values = {
        k: v for k, v in _expected_property_values.items() if k.startswith("sc_")
    }
    for p, _expected_value in _non_scoped_expected_property_values.items():
        not_equal_elements = _rev_links[p][_rev_links[p].ne(_expected_value)]
        if not_equal_elements.size > 0:
            WranglerLogger.debug(f"Expected_value of {p}: \n{_expected_value}")
            WranglerLogger.error(f"Elements not equal for {p}: \n{not_equal_elements}")
            pass_test = False
    # this is dumb, but pandas wont consider lists equal when comparing with vector op
    for idx in _selected_link_idx:
        for p, _expected_value in _scoped_expected_property_values.items():
            if net.links_df.loc[idx, p] != _expected_value:
                WranglerLogger.debug(f"Expected_value of {p}: \n{_expected_value}")
                WranglerLogger.error(
                    f"Elements not equal for {p}, idx: {idx}: \n\
                                     {net.links_df.loc[idx, p]}"
                )
                pass_test = False

    assert pass_test

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_managed_lane_change_functionality(request, stpaul_net, stpaul_ex_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(stpaul_net)

    _expected_property_values = {
        "managed": 1,
        "HOV": 5,
        "ML_HOV": 5,
    }

    project_card_name = "road.prop_change.managed_lanes.yml"
    project_card_path = stpaul_ex_dir / "project_cards" / project_card_name
    project_card = read_card(project_card_path)

    WranglerLogger.info("      start: select_roadway_features")
    _selected_link_idx = net.get_selection(
        project_card.roadway_property_change["facility"]
    ).selected_links

    attributes_to_update = list(project_card.roadway_property_change["property_changes"].keys())

    _orig_links = copy.deepcopy(
        net.links_df.loc[
            _selected_link_idx, net.links_df.columns.intersection(attributes_to_update)
        ]
    )

    WranglerLogger.debug(f"_orig_links: \n{_orig_links}")
    net = net.apply(project_card)

    _rev_links = net.links_df.loc[_selected_link_idx, list(_expected_property_values.keys())]
    WranglerLogger.debug(f"_rev_links: \n{_rev_links.iloc[0]}")

    for p, _expected_value in _expected_property_values.items():
        WranglerLogger.debug(f"Expected_value of {p}: \n{_expected_value}")
        WranglerLogger.debug(f"Actual Values of {p}: \n{_rev_links[p].iloc[0]}")
        assert _rev_links[p].eq(_expected_value).all()

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_existing_managed_lane_apply(request, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(stpaul_net)

    _facility_1 = {
        "links": {
            "model_link_id": [
                390975,
                391203,
                394205,
            ],
        }
    }

    _facility_2 = {
        "links": {
            "model_link_id": [
                394208,
                394214,
            ],
        }
    }

    _properties = SIMPLE_MANAGED_LANE_PROPERTIES
    _1_project_card_dict = {
        "project": "test simple managed lanes",
        "roadway_property_change": {
            "facility": _facility_1,
            "property_changes": _properties,
        },
    }
    _2_project_card_dict = {
        "project": "test simple managed lanes",
        "roadway_property_change": {
            "facility": _facility_2,
            "property_changes": _properties,
        },
    }
    _1_selected_link_idx = net.get_selection(_facility_1).selected_links
    _2_selected_link_idx = net.get_selection(_facility_2).selected_links

    _base_ML_links = copy.deepcopy(len(net.links_df.of_type.managed))
    WranglerLogger.info(f"Initial # of managed lane links: {_base_ML_links}")

    net = net.apply(_1_project_card_dict)
    _change_1_ML_links = copy.deepcopy(len(net.links_df.of_type.managed))
    WranglerLogger.info(f"Interim # of managed lane links: {_change_1_ML_links}")

    net = net.apply(_2_project_card_dict)

    _change_2_ML_links = copy.deepcopy(len(net.links_df.of_type.managed))
    WranglerLogger.info(f"Final # of managed lane links: {_change_2_ML_links}")

    assert _change_1_ML_links == _base_ML_links + len(_1_selected_link_idx)
    assert _change_2_ML_links == _change_1_ML_links + len(_2_selected_link_idx)

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_conflicting_managed_lane_apply(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(small_net)
    LINK_ID = 112
    TIMESPAN = ["6:00", "10:00"]
    project_1 = {
        "project": "ml_1",
        "roadway_property_change": {
            "facility": {"links": {"model_link_id": [LINK_ID]}},
            "property_changes": {
                "lanes": {"change": 0, "scoped": [{"change": -1, "timespan": TIMESPAN}]},
                "ML_lanes": {"set": 0, "scoped": [{"set": 1, "timespan": TIMESPAN}]},
            },
        },
    }
    project_2 = {
        "project": "ml_2",
        "roadway_property_change": {
            "facility": {"links": {"model_link_id": [LINK_ID]}},
            "property_changes": {
                "lanes": {"change": 0, "scoped": [{"change": 1, "timespan": TIMESPAN}]},
            },
        },
    }
    initial_lanes = net.links_df.loc[LINK_ID, "lanes"]
    WranglerLogger.info(f"Initial lanes: {initial_lanes}")
    net.apply(project_1)

    int_lanes = net.links_df.loc[LINK_ID, "lanes"]
    int_sc_lanes = net.links_df.loc[LINK_ID, "sc_lanes"]
    int_ml_lanes = net.links_df.loc[LINK_ID, "ML_lanes"]
    int_sc_ml_lanes = net.links_df.loc[LINK_ID, "sc_ML_lanes"]

    WranglerLogger.info(
        f"\n - Interim lanes: {int_lanes}\n\
         - Interim scoped lanes: {int_sc_lanes}\n\
         - Interim scoped ML lanes: {int_sc_ml_lanes}"
    )

    INT_EXP_LANES = initial_lanes
    INT_EXP_SC_LANES = [
        ScopedLinkValueItem(category="any", timespan=TIMESPAN, value=initial_lanes - 1)
    ]
    INT_EXP_ML_LANES = 0
    INT_EXP_SC_ML_LANES = [ScopedLinkValueItem(category="any", timespan=TIMESPAN, value=1)]
    assert int_lanes == INT_EXP_LANES
    assert int_sc_lanes == INT_EXP_SC_LANES
    assert int_sc_ml_lanes == INT_EXP_SC_ML_LANES
    assert int_ml_lanes == INT_EXP_ML_LANES

    net.apply(project_2)

    final_lanes = net.links_df.loc[LINK_ID, "lanes"]
    final_sc_lanes = net.links_df.loc[LINK_ID, "sc_lanes"]
    WranglerLogger.info(
        f"\n - Final lanes: {final_lanes}\n - Final scoped lanes: {final_sc_lanes}"
    )

    FINAL_EXP_LANES = initial_lanes
    FINAL_EXP_SC_LANES = [
        ScopedLinkValueItem(category="any", timespan=TIMESPAN, value=initial_lanes)
    ]
    assert final_lanes == FINAL_EXP_LANES
    assert final_sc_lanes == FINAL_EXP_SC_LANES
    WranglerLogger.info(f"--Finished: {request.node.name}")

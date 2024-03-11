import copy
import os

import pytest
from projectcard import read_card

from network_wrangler import WranglerLogger
from network_wrangler.utils import parse_timespans_to_secs


"""
Usage:  `pytest tests/test_roadway/test_changes/test_managed_lanes.py`
"""

_am_period = parse_timespans_to_secs(["6:00", "9:00"])
_pm_period = parse_timespans_to_secs(["16:00", "19:00"])

SIMPLE_MANAGED_LANE_PROPERTIES = {
    "lanes": {
        "set": 3,
        "timeofday": [
            {"timespan": ["6:00", "9:00"], "set": 2},
            {"timespan": ["16:00", "19:00"], "set": 2},
        ],
    },
    "ML_lanes": {
        "set": 0,
        "timeofday": [
            {"timespan": ["6:00", "9:00"], "set": 1},
            {"timespan": ["16:00", "19:00"], "set": 1},
        ],
    },
    "ML_access": {"set": "all"},
    "ML_egress": {"set": "all"},
    "ML_price": {
        "set": 0,
        "group": [
            {
                "category": "sov",
                "timeofday": [
                    {"timespan": ["6:00", "9:00"], "set": 1.5},
                    {"timespan": ["16:00", "19:00"], "set": 2.5},
                ],
            },
            {
                "category": "hov2",
                "timeofday": [
                    {"timespan": ["6:00", "9:00"], "set": 1},
                    {"timespan": ["16:00", "19:00"], "set": 2},
                ],
            },
        ],
    },
}


def test_add_managed_lane(request, stpaul_net, stpaul_ex_dir, scratch_dir):
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
        "roadway_managed_lanes": {
            "facility": _facility,
            "property_changes": _properties,
        },
    }
    _expected_property_values = {
        "managed": 1,
        "lanes": {
            "default": 3,
            "timeofday": [
                {"timespan": _am_period, "value": 2},
                {"timespan": _pm_period, "value": 2},
            ],
        },
        "ML_lanes": {
            "default": 0,
            "timeofday": [
                {"timespan": _am_period, "value": 1},
                {"timespan": _pm_period, "value": 1},
            ],
        },
        "ML_access": "all",
        "ML_egress": "all",
        "ML_price": {
            "default": 0,
            "timeofday": [
                {"category": "sov", "timespan": _am_period, "value": 1.5},
                {"category": "sov", "timespan": _pm_period, "value": 2.5},
                {"category": "hov2", "timespan": _am_period, "value": 1},
                {"category": "hov2", "timespan": _pm_period, "value": 2},
            ],
        },
    }

    net = copy.deepcopy(stpaul_net)

    _selected_link_idx = net.get_selection(_facility).selected_links
    _p_to_track = list(_properties.keys())

    _orig_links = net.links_df.loc[
        _selected_link_idx, net.links_df.columns.intersection(_p_to_track)
    ].copy()
    WranglerLogger.debug(f"_orig_links:\n{_orig_links}")

    # apply change
    net = net.apply(_project_card_dict)

    _rev_links = net.links_df.loc[
        _selected_link_idx, list(_expected_property_values.keys())
    ]
    WranglerLogger.debug(f"_rev_links:\n{_rev_links.iloc[0]}")

    for p, _expected_value in _expected_property_values.items():
        WranglerLogger.debug(f"Expected_value of {p}:\n{_expected_value}")
        WranglerLogger.debug(f"Actual Values of {p}:\n{_rev_links[p].iloc[0]}")
        assert _rev_links[p].eq(_expected_value).all()

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
    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", project_card_name)
    project_card = read_card(project_card_path)

    WranglerLogger.info("      start: select_roadway_features")
    _selected_link_idx = net.get_selection(project_card.facility).selected_links

    attributes_to_update = list(
        project_card.roadway_managed_lanes["property_changes"].keys()
    )

    _orig_links = net.links_df.loc[
        _selected_link_idx, net.links_df.columns.intersection(attributes_to_update)
    ].copy()

    WranglerLogger.debug(f"_orig_links:\n{_orig_links}")
    net = net.apply(project_card)

    _rev_links = net.links_df.loc[
        _selected_link_idx, list(_expected_property_values.keys())
    ]
    WranglerLogger.debug(f"_rev_links:\n{_rev_links.iloc[0]}")

    for p, _expected_value in _expected_property_values.items():
        WranglerLogger.debug(f"Expected_value of {p}:\n{_expected_value}")
        WranglerLogger.debug(f"Actual Values of {p}:\n{_rev_links[p].iloc[0]}")
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
        "roadway_managed_lanes": {
            "facility": _facility_1,
            "property_changes": _properties,
        },
    }
    _2_project_card_dict = {
        "project": "test simple managed lanes",
        "roadway_managed_lanes": {
            "facility": _facility_2,
            "property_changes": _properties,
        },
    }
    _1_selected_link_idx = net.get_selection(_facility_1).selected_links
    _2_selected_link_idx = net.get_selection(_facility_2).selected_links

    _base_ML_links = copy.deepcopy(net.num_managed_lane_links)
    WranglerLogger.info(f"Initial # of managed lane links: {_base_ML_links}")

    net = net.apply(_1_project_card_dict)
    _change_1_ML_links = copy.deepcopy(net.num_managed_lane_links)
    WranglerLogger.info(f"Interim # of managed lane links: {_change_1_ML_links}")

    net = net.apply(_2_project_card_dict)

    _change_2_ML_links = copy.deepcopy(net.num_managed_lane_links)
    WranglerLogger.info(f"Final # of managed lane links: {_change_2_ML_links}")

    assert _change_1_ML_links == _base_ML_links + len(_1_selected_link_idx)
    assert _change_2_ML_links == _change_1_ML_links + len(_2_selected_link_idx)

    WranglerLogger.info(f"--Finished: {request.node.name}")

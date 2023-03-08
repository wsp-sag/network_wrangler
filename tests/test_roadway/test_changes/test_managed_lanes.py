import copy
import os

import pytest

import pandas as pd

from network_wrangler import ProjectCard
from network_wrangler import RoadwayNetwork
from network_wrangler import WranglerLogger
from network_wrangler.utils import parse_time_spans_to_secs

from network_wrangler.roadway.model_roadway import (
    MANAGED_LANES_LINK_ID_SCALAR,
)

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 50000)

"""
Run just the tests labeled basic using `pytest tests/test_roadway/test_changes/test_managed_lanes.py`
"""

_am_period = parse_time_spans_to_secs(["6:00", "9:00"])
_pm_period = parse_time_spans_to_secs(["16:00", "19:00"])

SIMPLE_MANAGED_LANE_PROPERTIES = [
    {
        "property": "lanes",
        "set": 3,
        "timeofday": [
            {"time": ["6:00", "9:00"], "set": 2},
            {"time": ["16:00", "19:00"], "set": 2},
        ],
    },
    {
        "property": "ML_lanes",
        "set": 0,
        "timeofday": [
            {"time": ["6:00", "9:00"], "set": 1},
            {"time": ["16:00", "19:00"], "set": 1},
        ],
    },
    {
        "property": "ML_lanes",
        "set": 0,
        "timeofday": [
            {"time": ["6:00", "9:00"], "set": 1},
            {"time": ["16:00", "19:00"], "set": 1},
        ],
    },
    {"property": "ML_access", "set": "all"},
    {"property": "ML_egress", "set": "all"},
    {
        "property": "ML_price",
        "set": 0,
        "group": [
            {
                "category": "sov",
                "timeofday": [
                    {"time": ["6:00", "9:00"], "set": 1.5},
                    {"time": ["16:00", "19:00"], "set": 2.5},
                ],
            },
            {
                "category": "hov2",
                "timeofday": [
                    {"time": ["6:00", "9:00"], "set": 1},
                    {"time": ["16:00", "19:00"], "set": 2},
                ],
            },
        ],
    },
]


def test_add_managed_lane(request, stpaul_net, stpaul_ex_dir, scratch_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    # Set facility selection
    _facility = {
        "links": [{"name": ["I 35E"]}],
        "A": {"osm_node_id": "961117623"},
        "B": {"osm_node_id": "2564047368"},
    }

    _properties = SIMPLE_MANAGED_LANE_PROPERTIES
    _expected_property_values = {
        "managed": 1,
        "lanes": {
            "default": 3,
            "timeofday": [
                {"time": _am_period, "value": 2},
                {"time": _pm_period, "value": 2},
            ],
        },
        "ML_lanes": {
            "default": 0,
            "timeofday": [
                {"time": _am_period, "value": 1},
                {"time": _pm_period, "value": 1},
            ],
        },
        "ML_access": "all",
        "ML_egress": "all",
        "ML_price": {
            "default": 0,
            "timeofday": [
                {"category": "sov", "time": _am_period, "value": 1.5},
                {"category": "sov", "time": _pm_period, "value": 2.5},
                {"category": "hov2", "time": _am_period, "value": 1},
                {"category": "hov2", "time": _pm_period, "value": 2},
            ],
        },
    }

    net = copy.deepcopy(stpaul_net)

    WranglerLogger.info(f"      start: select_roadway_features")
    _selected_link_idx = net.select_roadway_features(_facility)

    _p_to_track = [p["property"] for p in _properties]
    WranglerLogger.debug(f"_p_to_track: {_p_to_track}")

    _orig_links = copy.deepcopy(
        net.links_df.loc[
            _selected_link_idx, net.links_df.columns.intersection(_p_to_track)
        ]
    )
    WranglerLogger.debug(f"_orig_links:\n{_orig_links}")

    # apply change
    WranglerLogger.info(f"      start: apply_managed_lane_feature_change")
    net = net.apply_managed_lane_feature_change(
        net.select_roadway_features(_facility), _properties
    )

    _rev_links = net.links_df.loc[
        _selected_link_idx, list(_expected_property_values.keys())
    ]
    WranglerLogger.debug(f"_rev_links:\n{_rev_links.iloc[0]}")

    for p, _expected_value in _expected_property_values.items():
        WranglerLogger.debug(f"Expected_value of {p}:\n{_expected_value}")
        WranglerLogger.debug(f"Actual Values of {p}:\n{_rev_links[p].iloc[0]}")
        assert _rev_links[p].eq(_expected_value).all()

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_add_managed_lane_complex(request, stpaul_net, stpaul_ex_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    _expected_property_values = {
        "managed": 1,
        "segment_id": 5,
        "ML_segment_id": 5,
        "ML_lanes": {
            "default": 0,
            "timeofday": [
                {"time": _am_period, "value": 1},
            ],
        },
    }

    net = copy.deepcopy(stpaul_net)
    project_card_name = "broken_parallel_managed_lane.yml"
    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path)

    WranglerLogger.info(f"      start: select_roadway_features")
    _selected_link_idx = net.select_roadway_features(project_card.facility)

    _p_to_track = [p["property"] for p in project_card.properties]

    _orig_links = copy.deepcopy(
        net.links_df.loc[
            _selected_link_idx, net.links_df.columns.intersection(_p_to_track)
        ]
    )
    WranglerLogger.debug(f"_orig_links:\n{_orig_links}")

    WranglerLogger.info(f"      start: apply_managed_lane_feature_change")
    net = net.apply_managed_lane_feature_change(
        net.select_roadway_features(project_card.facility), project_card.properties
    )

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

    project_card_name = "test_managed_lanes_change_keyword.yml"
    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path, validate=False)

    WranglerLogger.info(f"      start: select_roadway_features")
    _selected_link_idx = net.select_roadway_features(project_card.facility)

    attributes_to_update = [p["property"] for p in project_card.properties]

    _orig_links = copy.deepcopy(
        net.links_df.loc[
            _selected_link_idx, net.links_df.columns.intersection(attributes_to_update)
        ]
    )
    WranglerLogger.debug(f"_orig_links:\n{_orig_links}")
    WranglerLogger.info(f"      start: apply_managed_lane_feature_change")
    net = net.apply_managed_lane_feature_change(
        net.select_roadway_features(project_card.facility), project_card.properties
    )

    _rev_links = net.links_df.loc[
        _selected_link_idx, list(_expected_property_values.keys())
    ]
    WranglerLogger.debug(f"_rev_links:\n{_rev_links.iloc[0]}")
    WranglerLogger.info(f"      start: eval expected")
    for p, _expected_value in _expected_property_values.items():
        WranglerLogger.debug(f"Expected_value of {p}:\n{_expected_value}")
        WranglerLogger.debug(f"Actual Values of {p}:\n{_rev_links[p].iloc[0]}")
        assert _rev_links[p].eq(_expected_value).all()

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_existing_managed_lane_apply(request, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(stpaul_net)

    _facility_1 = {
        "links": [
            {
                "model_link_id": [
                    390975,
                    391203,
                    394205,
                ],
            }
        ],
    }

    _facility_2 = {
        "links": [
            {
                "model_link_id": [
                    394208,
                    394214,
                ],
            }
        ],
    }

    _properties = SIMPLE_MANAGED_LANE_PROPERTIES

    _1_selected_link_idx = net.select_roadway_features(_facility_1)
    _2_selected_link_idx = net.select_roadway_features(_facility_2)

    _base_ML_links = copy.deepcopy(net.num_managed_lane_links)
    WranglerLogger.info(f"Initial # of managed lane links: {_base_ML_links}")

    WranglerLogger.info(f"      start: apply_managed_lane_feature_change")
    net = net.apply_managed_lane_feature_change(
        net.select_roadway_features(_facility_1), _properties
    )
    _change_1_ML_links = copy.deepcopy(net.num_managed_lane_links)
    WranglerLogger.info(f"Interim # of managed lane links: {_change_1_ML_links}")

    WranglerLogger.info(f"      start: apply_managed_lane_feature_change")
    net = net.apply_managed_lane_feature_change(
        net.select_roadway_features(_facility_2), _properties
    )

    _change_2_ML_links = copy.deepcopy(net.num_managed_lane_links)
    WranglerLogger.info(f"Final # of managed lane links: {_change_2_ML_links}")

    assert _change_1_ML_links == _base_ML_links + len(_1_selected_link_idx)
    assert _change_2_ML_links == _change_1_ML_links + len(_2_selected_link_idx)

    WranglerLogger.info(f"--Finished: {request.node.name}")
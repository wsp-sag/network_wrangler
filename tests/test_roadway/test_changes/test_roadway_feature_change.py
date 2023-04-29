import copy
import os

import pytest

import pandas as pd

from network_wrangler import WranglerLogger
from projectcard import ProjectCard

"""
Usage `pytest tests/test_roadway/test_changes/test_feature_change.py`
"""


def test_change_roadway_existing_and_change_single_link(request, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(stpaul_net)

    # Set facility selection
    _facility = {
        "links": [{"osm_link_id": ["223371529"]}],
        "A": {"osm_node_id": "187854529"},  # Jackson St
        "B": {"osm_node_id": "187899923"},  # Robert St N
    }
    _properties = [
        {
            "property": "lanes",  # changes number of lanes 3 to 2 (reduction of 1)
            "existing": 2,
            "change": -1,
        }
    ]

    _selected_link_idx = net.select_roadway_features(_facility)
    _p_to_track = ["name"] + [p["property"] for p in _properties]

    WranglerLogger.debug(f"_p_to_track: {_p_to_track}")

    _orig_links = pd.DataFrame(copy.deepcopy(net.links_df))
    _orig_links = _orig_links.loc[_selected_link_idx, _p_to_track]
    WranglerLogger.debug(f"_orig_links:\n{_orig_links}")

    # apply change
    net = net.apply_roadway_feature_change(_selected_link_idx, _properties)

    _rev_links = pd.DataFrame(net.links_df)
    _rev_links = _rev_links.loc[_selected_link_idx, _p_to_track]
    WranglerLogger.debug(f"_rev_links:\n{_rev_links}")

    WranglerLogger.debug(
        f"ORIGINAL to REVISED Comparison\n {_orig_links.compare(_rev_links)}"
    )

    for p in _properties:
        _expected_value = p["existing"] + p["change"]
        WranglerLogger.debug(f"Expected_value of {p['property']}:{_expected_value}")
        assert _rev_links[p["property"]].eq(_expected_value).all()

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_change_multiple_properties_multiple_links(request, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(stpaul_net)
    # Set facility selection
    _facility = {
        "links": [{"name": ["6th", "Sixth", "sixth"]}],
        "A": {"osm_node_id": "187899923"},  # Jackson St
        "B": {"osm_node_id": "187865924"},  # Robert St N
    }
    _properties = [
        {
            "property": "lanes",
            "set": 2,
        },
        {
            "property": "bus_only",
            "set": 1,
        },
        {
            "property": "drive_access",
            "set": 0,
        },
    ]

    _selected_link_idx = net.select_roadway_features(_facility)
    _p_to_track = ["name"] + [p["property"] for p in _properties]

    WranglerLogger.debug(f"_p_to_track: {_p_to_track}")

    _orig_links = pd.DataFrame(copy.deepcopy(net.links_df))
    _orig_links = _orig_links.loc[_selected_link_idx, _p_to_track]
    WranglerLogger.debug(f"_orig_links:\n{_orig_links}")

    # apply change
    net = net.apply_roadway_feature_change(_selected_link_idx, _properties)

    _rev_links = pd.DataFrame(net.links_df)
    _rev_links = _rev_links.loc[_selected_link_idx, _p_to_track]
    WranglerLogger.debug(f"_rev_links:\n{_rev_links}")

    WranglerLogger.debug(
        f"ORIGINAL to REVISED Comparison\n {_orig_links.compare(_rev_links)}"
    )

    for p in _properties:
        _expected_value = p["set"]
        WranglerLogger.debug(f"Expected_value of {p['property']}:{_expected_value}")
        assert _rev_links[p["property"]].eq(_expected_value).all()
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_change_multiple_properties_multiple_links_existing_set(request, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(stpaul_net)
    # Set facility selection
    _facility = {
        "links": [{"name": ["6th", "Sixth", "sixth"]}],
        "A": {"osm_node_id": "187899923"},  # Jackson St
        "B": {"osm_node_id": "187865924"},  # Robert St N
    }
    _properties = [
        {
            "property": "lanes",
            "existing": 1,
            "set": 2,
        },
        {
            "property": "bus_only",
            "set": 1,
        },
        {
            "property": "drive_access",
            "set": 0,
        },
    ]

    _selected_link_idx = net.select_roadway_features(_facility)
    _p_to_track = ["name"] + [p["property"] for p in _properties]

    WranglerLogger.debug(f"_p_to_track: {_p_to_track}")

    _orig_links = pd.DataFrame(copy.deepcopy(net.links_df))
    _orig_links = _orig_links.loc[_selected_link_idx, _p_to_track]
    WranglerLogger.debug(f"_orig_links:\n{_orig_links}")

    # apply change
    net = net.apply_roadway_feature_change(_selected_link_idx, _properties)

    _rev_links = pd.DataFrame(net.links_df)
    _rev_links = _rev_links.loc[_selected_link_idx, _p_to_track]
    WranglerLogger.debug(f"_rev_links:\n{_rev_links}")

    WranglerLogger.debug(
        f"ORIGINAL to REVISED Comparison\n {_orig_links.compare(_rev_links)}"
    )

    for p in _properties:
        _expected_value = p["set"]
        WranglerLogger.debug(f"Expected_value of {p['property']}:{_expected_value}")
        assert _rev_links[p["property"]].eq(_expected_value).all()


def test_add_adhoc_field(request, small_net):
    """
    Makes sure new fields can be added in the API and be saved and read in again.
    """
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(small_net)
    net.links_df["my_ad_hoc_field"] = 22.5

    WranglerLogger.debug(
        f"Network with field...\n{net.links_df['my_ad_hoc_field'].iloc[0:5]}"
    )

    assert net.links_df["my_ad_hoc_field"].iloc[0] == 22.5
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_add_default_value(request, stpaul_net, stpaul_ex_dir):
    """
    Makes sure we can add a new field with a default value
    """
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(stpaul_net)
    project_card_name = "select_all_project_card.yml"

    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path)

    net = net.apply_roadway_feature_change(
        net.select_roadway_features(project_card.facility), project_card.properties
    )

    WranglerLogger.debug(f"{net.links_df['my_ad_hoc_field_float'].value_counts()}")
    WranglerLogger.debug(f"{net.links_df['my_ad_hoc_field_integer'].value_counts()}")
    WranglerLogger.debug(f"{net.links_df['my_ad_hoc_field_string'].value_counts()}")

    assert (
        net.links_df.loc[net.links_df["my_ad_hoc_field_float"] == 1.2345].shape
        == net.links_df.shape
    )
    assert (
        net.links_df.loc[net.links_df["my_ad_hoc_field_integer"] == 10].shape
        == net.links_df.shape
    )
    assert (
        net.links_df.loc[
            net.links_df["my_ad_hoc_field_string"] == "default value"
        ].shape
        == net.links_df.shape
    )

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_add_adhoc_field_from_card(request, stpaul_net, stpaul_ex_dir):
    """
    Makes sure new fields can be added from a project card and that
    they will be the right type.
    """
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(stpaul_net)
    project_card_name = "new_fields_project_card.yml"

    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path)

    selected_link_indices = net.select_roadway_features(project_card.facility)

    attributes_to_update = [p["property"] for p in project_card.properties]

    net = net.apply_roadway_feature_change(
        net.select_roadway_features(project_card.facility), project_card.properties
    )

    rev_links = net.links_df.loc[selected_link_indices, attributes_to_update]
    rev_types = [(a, net.links_df[a].dtypes) for a in attributes_to_update]

    WranglerLogger.debug(
        f"Revised Links:\n{rev_links}\nNew Property Types:\n{rev_types}"
    )

    assert net.links_df.loc[selected_link_indices[0], "my_ad_hoc_field_float"] == 1.1
    assert net.links_df.loc[selected_link_indices[0], "my_ad_hoc_field_integer"] == 2
    assert (
        net.links_df.loc[selected_link_indices[0], "my_ad_hoc_field_string"] == "three"
    )
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_bad_properties_statements(request, small_net):
    """
    Makes sure new fields can be added from a project card and that
    they will be the right type.
    """

    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(small_net)
    ok_properties_change = [{"property": "lanes", "change": 1}]
    bad_properties_change = [{"property": "my_random_var", "change": 1}]
    bad_properties_existing = [{"property": "my_random_var", "existing": 1}]

    with pytest.raises(ValueError):
        net.validate_properties(net.links_df, bad_properties_change)

    with pytest.raises(ValueError):
        net.validate_properties(
            net.links_df, ok_properties_change, require_existing_for_change=True
        )

    with pytest.raises(ValueError):
        net.validate_properties(
            net.links_df, bad_properties_existing, ignore_existing=False
        )

    WranglerLogger.info(f"--Finished: {request.node.name}")

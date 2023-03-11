import copy
import os

import pytest

import pandas as pd

from network_wrangler import WranglerLogger
from network_wrangler import ProjectCard

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 50000)

"""
Run just the tests labeled basic using `pytest tests/test_roadway/test_changes/test_pycode.py`
"""


def test_read_dot_wrangler_roadway(request, stpaul_ex_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    project_card_name = "add_highway_lanes.wrangler"
    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path, validate=False)
    WranglerLogger.debug(f"project_card:\n{project_card}")
    assert (
        "self.links_df.loc[self.links_df['lanes'] == 4, 'lanes'] = 12"
        in project_card.pycode
    )
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_apply_pycode_roadway(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(small_net)
    _pycode = "self.links_df.loc[self.links_df['lanes'] == 5, 'lanes'] = 12"
    _link_sel = net.links_df.loc[net.links_df["lanes"] == 5]
    _link_sel_idx = _link_sel["model_link_id"].squeeze()
    _expected_value = 12
    _show_fields = ["model_link_id", "lanes"]
 
    WranglerLogger.debug(f"Before Change:\n{_link_sel[_show_fields]}")

    net = net.apply(
        {
            "category": "Calculated Roadway",
            "project": "megaroads",
            "pycode": _pycode,
        }
    )
    _link_sel = net.links_df.loc[net.links_df["model_link_id"] == _link_sel_idx]
    WranglerLogger.debug(f"After Change:\n{_link_sel[_show_fields]}")

    assert _link_sel["lanes"].eq(_expected_value).all()
    WranglerLogger.info(f"--Finished: {request.node.name}")

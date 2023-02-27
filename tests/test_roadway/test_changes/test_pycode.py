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
To run with print statments, use `pytest -s tests/test_roadway/test_changes/test_pycode.py`
"""

def test_dot_wrangler_roadway(request,stpaul_ex_dir):
    print("\n--Starting:", request.node.name)

    print("Reading .wrangler project card ...")
    project_card_name = "add_highway_lanes.wrangler"
    project_card_path = os.path.join(stpaul_ex_dir, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path, validate=False)
    print(project_card)
    assert (
        "self.links_df.loc[self.links_df['lanes'] == 4, 'lanes'] = 12"
        in project_card.pycode
    )


def test_apply_pycode_roadway(request,stpaul_net):
    print("\n--Starting:", request.node.name)

    print("Reading network ...")
    net = copy.deepcopy(stpaul_net)

    print("Apply pycode ...")
    print(
        "BEFORE CHANGE...\n",
        net.links_df.loc[net.links_df["lanes"] == 4, ["model_link_id", "lanes"]],
    )
    net = net.apply(
        {
            "category": "Calculated Roadway",
            "project": "megaroads",
            "pycode": "self.links_df.loc[self.links_df['lanes'] == 4, 'lanes'] = 12",
        }
    )
    print(
        "AFTER CHANGE...\n",
        net.links_df.loc[net.links_df["lanes"] == 12, ["model_link_id", "lanes"]],
    )

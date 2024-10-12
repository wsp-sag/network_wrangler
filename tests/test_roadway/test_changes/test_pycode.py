import copy

import pytest
from projectcard import read_card

from network_wrangler import WranglerLogger
from network_wrangler.errors import ProjectCardError

"""
Usage:   `pytest tests/test_roadway/test_changes/test_pycode.py`
"""


def test_read_dot_wrangler_roadway(request, stpaul_ex_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    project_card_name = "road.prop_change.wrangler"
    project_card_path = stpaul_ex_dir / "project_cards" / project_card_name
    project_card = read_card(project_card_path, validate=False)
    WranglerLogger.debug(f"project_card: \n{project_card}")
    assert (
        "roadway_net.links_df.loc[roadway_net.links_df['lanes'] == 4, 'lanes'] = 12"
        in project_card.pycode
    )
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_apply_pycode_roadway(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(small_net)
    _pycode = "roadway_net.links_df.loc[roadway_net.links_df['lanes'] == 5, 'lanes'] = 12"
    _link_sel = net.links_df.loc[net.links_df["lanes"] == 5]
    _link_sel_idx = _link_sel["model_link_id"].squeeze()
    _expected_value = 12
    _show_fields = ["model_link_id", "lanes"]

    WranglerLogger.debug(f"Before Change: \n{_link_sel[_show_fields]}")

    net = net.apply(
        {
            "project": "megaroads",
            "pycode": _pycode,
        }
    )
    WranglerLogger.debug(f"RoadwayNetwork type after apply: {type(net)}")
    assert "RoadwayNetwork" in str(type(net))
    _link_sel = net.links_df.loc[_link_sel_idx]
    WranglerLogger.debug(f"After Change: \n{_link_sel[_show_fields]}")

    assert _link_sel["lanes"].eq(_expected_value).all()
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_apply_bad_pycode_roadway(request, small_net):
    """Make sure bad pycode syntax will raise an error."""
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = copy.deepcopy(small_net)
    _pycode = "roadway_net.links_df.loc[[roadway_net.links_df['lanes'] == 5, 'lanes'] = 12"

    with pytest.raises(ProjectCardError):
        net.apply(
            {
                "project": "megaroads",
                "pycode": _pycode,
            }
        )

"""Run just the tests using `pytest tests/test_transit/test_changes/test_transit_route_changes.py`."""

import copy
import pytest

from projectcard import read_card

from network_wrangler import WranglerLogger
from network_wrangler.roadway.network import RoadwayNetwork
from network_wrangler.transit.network import TransitNetwork
from network_wrangler.transit.projects.add_route import (
    apply_add_transit_route_change,
    _add_route_to_feed,
)


def test_add_route_to_feed_dict(
    request,
    small_transit_net: TransitNetwork,
):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    feed = copy.deepcopy(small_transit_net.feed)
    # TODO: Add test for adding route to small tiny network using a dict
    updated_feed =  _add_route_to_feed(feed, add_route_change)
    # TODO Add assertions
    WranglerLogger.info(f"--Finished: {request.node.name}")



def test_add_route_to_net_dict(
    request,
    small_transit_net: TransitNetwork,
):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    transit_net = copy.deepcopy(small_transit_net)
    # TODO: Add test for adding route to small tiny network using a dict
    updated_transit_net = apply_add_transit_route_change(transit_net, add_route_change)
    # TODO Add assertions
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_add_route_project_card(
    request,
    stpaul_net: RoadwayNetwork,
    stpaul_card_dir: str,
    stpaul_transit_net: TransitNetwork,
):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    transit_net = copy.deepcopy(stpaul_transit_net)
    project_card = read_card(stpaul_card_dir / "transit.routing_change.yml")
    # TODO: Add test for adding route to stpaul network using a project card
    transit_net = transit_net.apply(project_card, reference_road_net=stpaul_net)
    # TODO: add assertions
    WranglerLogger.info(f"--Finished: {request.node.name}")

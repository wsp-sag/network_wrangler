"""Run just this `pytest tests/test_transit/test_changes/test_transit_delete_service.py`."""

import copy

from pandas.testing import assert_frame_equal
from projectcard import ProjectCard

from network_wrangler import WranglerLogger
from network_wrangler.transit.network import TransitNetwork

delete_service_change_small_net = {
    "project": "Delete Transit",
    "transit_service_deletion": {"service": {"trip_properties": {"trip_id": ["blue-1"]}}},
}


def test_delete_service_not_shape(
    request,
    small_transit_net: TransitNetwork,
):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    card = ProjectCard(delete_service_change_small_net)
    net = copy.deepcopy(small_transit_net)
    updated_net = net.apply(card)

    # check trips have been deleted
    assert updated_net.feed.trips.trip_id.loc[
        updated_net.feed.trips.trip_id.isin(["blue-1"])
    ].empty

    # check shapes, shouldn't have changed
    assert_frame_equal(small_transit_net.feed.shapes, updated_net.feed.shapes)


delete_service_change_small_net_clean_shapes = {
    "project": "Delete Transit",
    "transit_service_deletion": {
        "service": {"trip_properties": {"trip_id": ["blue-1"]}},
        "clean_shapes": True,
    },
}


def test_delete_service_and_shape(
    request,
    small_transit_net: TransitNetwork,
):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    card = ProjectCard(delete_service_change_small_net)
    net = copy.deepcopy(small_transit_net)
    updated_net = net.apply(card)

    # check trips
    assert updated_net.feed.trips.trip_id.loc[
        updated_net.feed.trips.trip_id.isin(["blue-1"])
    ].empty

    # check shapes, should have been removed
    shape_ids = small_transit_net.feed.trips.shape_id.unique()
    assert updated_net.feed.trips.trip_id.loc[updated_net.feed.trips.trip_id.isin(shape_ids)].empty

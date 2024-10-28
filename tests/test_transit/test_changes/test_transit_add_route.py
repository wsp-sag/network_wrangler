"""Run just the tests using `pytest tests/test_transit/test_changes/test_transit_add_route.py`."""

import copy

import pytest
from projectcard import read_card

from network_wrangler import WranglerLogger
from network_wrangler.roadway.network import RoadwayNetwork
from network_wrangler.transit.network import TransitNetwork
from network_wrangler.transit.projects.add_route import (
    apply_transit_route_addition,
)
from network_wrangler.utils.time import str_to_time

add_route_change = {
    "project": "New Green Transit",
    "transit_route_addition": {
        "routes": [
            {
                "route_id": "abc",
                "route_long_name": "green_line",
                "route_short_name": "green",
                "route_type": 3,
                "agency_id": "The Bus",
                "trips": [
                    {
                        "direction_id": 0,
                        "headway_secs": [
                            {"('6:00', '12:00')": 600},
                            {"('12:00', '13:00')": 900},
                        ],
                        "routing": [
                            {"1": {"stop": True}},
                            2,
                            3,
                            {"4": {"stop": True, "alight": False}},
                            5,
                            {"6": {"stop": True}},
                        ],
                    }
                ],
            }
        ]
    },
}


def test_add_route_to_feed_dict(
    request,
    small_transit_net: TransitNetwork,
    small_net: RoadwayNetwork,
):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    small_transit_net = copy.deepcopy(small_transit_net)
    updated_transit_net = apply_transit_route_addition(
        small_transit_net, add_route_change["transit_route_addition"], small_net
    )
    updated_feed = updated_transit_net.feed

    # check routes
    new_routes = updated_feed.routes.loc[updated_feed.routes.route_id.isin(["abc"])]
    assert len(new_routes) == 1
    assert new_routes.route_long_name.loc[new_routes.route_long_name == "green_line"].all()

    # check trips
    new_trips = updated_feed.trips.loc[updated_feed.trips.route_id.isin(["abc"])]
    new_trip_ids = new_trips.trip_id.to_list()
    assert len(new_trip_ids) == 2

    # check stops
    existing_stops = [1, 2, 3, 4]
    new_stops = [1, 4, 6]
    all_expected_stops = list(set(existing_stops + new_stops))

    assert updated_feed.stops.stop_id.isin(all_expected_stops).all()
    assert updated_feed.stop_times.stop_id.isin(all_expected_stops).all()

    # check stoptimes
    new_stop_times = updated_feed.stop_times.loc[
        updated_feed.stop_times.trip_id.isin(new_trip_ids)
    ]
    assert new_stop_times.stop_id.isin(new_stops).all()
    assert new_stop_times.loc[new_stop_times.stop_id == 4, "drop_off_type"].iloc[0] == 1
    assert new_stop_times.loc[new_stop_times.stop_id == 4, "pickup_type"].iloc[0] == 0
    assert new_stop_times.loc[new_stop_times.stop_id == 6, "drop_off_type"].iloc[0] == 0
    assert new_stop_times.loc[new_stop_times.stop_id == 6, "pickup_type"].iloc[0] == 0
    assert new_stop_times.loc[new_stop_times.stop_id == 1, "drop_off_type"].iloc[0] == 0
    assert new_stop_times.loc[new_stop_times.stop_id == 1, "pickup_type"].iloc[0] == 0

    # check shapes
    new_shape_ids = new_trips.shape_id.unique()
    new_shapes = updated_feed.shapes.loc[updated_feed.shapes.shape_id.isin(new_shape_ids)]
    assert len(new_shapes) == 6
    expected_shape_modeL_node_ids = [1, 2, 3, 4, 5, 6]
    assert new_shapes.shape_model_node_id.isin(expected_shape_modeL_node_ids).all()

    # check frequencies
    new_frequencies = updated_feed.frequencies.loc[
        updated_feed.frequencies.trip_id.isin(new_trip_ids)
    ]
    assert len(new_frequencies) == 2
    assert new_frequencies.headway_secs.isin([600, 900]).all()
    assert new_frequencies.start_time.isin([str_to_time("6:00"), str_to_time("12:00")]).all()
    WranglerLogger.info(f"--Finished: {request.node.name}")


@pytest.mark.skip("Not implemented")
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

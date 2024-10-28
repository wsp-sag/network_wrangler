"""Run just the tests using `pytest tests/test_transit/test_changes/test_transit_route_changes.py`."""

import copy

import pytest
from projectcard import read_card

from network_wrangler import WranglerLogger
from network_wrangler.roadway.network import RoadwayNetwork
from network_wrangler.transit.feed.shapes import shapes_for_trip_id
from network_wrangler.transit.feed.stop_times import stop_times_for_trip_id
from network_wrangler.transit.network import TransitNetwork

TEST_ROUTING_REPLACEMENT = [
    {
        "name": "insert single-block detour in middle of route",
        "existing_routing": [2, 3],
        "set_routing": [2, 7, 6, 3],
        "expected_routing": [1, 2, 7, 6, 3, 4],
    },
    {
        "name": "insert multi-block detour in middle of route",
        "existing_routing": [2, 4],
        "set_routing": [2, 7, 6, 5, 4],
        "expected_routing": [1, 2, 7, 6, 5, 4],
    },
    {
        "name": "single-node add on to end of route",
        "existing_routing": [4],
        "set_routing": [4, 5, 6],
        "expected_routing": [1, 2, 3, 4, 5, 6],
    },
    {
        "name": "multi-node existing add on to end of route",
        "existing_routing": [3, 4],
        "set_routing": [3, 4, 5, 6],
        "expected_routing": [1, 2, 3, 4, 5, 6],
    },
    {
        "name": "single-node existing add on to start of route",
        "existing_routing": [1],
        "set_routing": [8, 1],
        "expected_routing": [8, 1, 2, 3, 4],
    },
    {
        "name": "multi-node existing add on to start of route",
        "existing_routing": [1, 2],
        "set_routing": [8, 1, 2],
        "expected_routing": [8, 1, 2, 3, 4],
    },
]


@pytest.mark.parametrize("test_routing", TEST_ROUTING_REPLACEMENT)
def test_replace_shapes_segment(request, test_routing, small_transit_net, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    WranglerLogger.info(f"...evaluating {test_routing['name']}")
    from network_wrangler.transit.projects.edit_routing import _replace_shapes_segment

    net = copy.deepcopy(small_transit_net)
    shape_id = "shape2"
    shapes_df = _replace_shapes_segment(
        test_routing["existing_routing"],
        shape_id,
        test_routing["set_routing"],
        net.feed,
        small_net,
    )
    WranglerLogger.debug(f"Updated shapes_df\n {shapes_df}")
    result_routing = shapes_df["shape_model_node_id"].to_list()

    assert test_routing["expected_routing"] == result_routing

    WranglerLogger.info(f"--Finished: {request.node.name}")


TEST_ROUTING_CHANGES = [
    {
        "name": "Replace",
        "service": {"trip_properties": {"shape_id": ["shape2"]}},
        "routing_change": {
            "set": [5, 6, 7],
        },
        "expected_routing": [5, 6, 7],
    },
    {
        "name": "Extend begining",
        "service": {"trip_properties": {"shape_id": ["shape2"]}},
        "routing_change": {
            "existing": [0, 3],
            "set": [8, 1, -2, 3],
        },
        "expected_routing": [8, 1, -2, 3, 4],
    },
    {
        "name": "Truncate start",
        "service": {"trip_properties": {"shape_id": ["shape2"]}},
        "routing_change": {
            "existing": [1, 3],
            "set": [2, 3],
        },
        "expected_routing": [2, 3, 4],
    },
    {
        "name": "Truncate start",
        "service": {"trip_properties": {"shape_id": ["shape2"]}},
        "routing_change": {
            "existing": [1, 3],
            "set": [2, 3],
        },
        "expected_routing": [2, 3, 4],
    },
    {
        "name": "Truncate end",
        "service": {"trip_properties": {"shape_id": ["shape2"]}},
        "routing_change": {
            "existing": [3, 4],
            "set": [3],
        },
        "expected_routing": [1, -2, 3],
    },
    {
        "name": "Change Middle",
        "service": {"trip_properties": {"shape_id": ["shape2"]}},
        "routing_change": {
            "existing": [3, 4],
            "set": [3, 6, 5, 4],
        },
        "expected_routing": [1, -2, 3, 6, 5, 4],
    },
]

TEST_STOP_CHANGES = [
    {
        "name": "Add Stop starting with negative",
        "service": {"trip_properties": {"trip_id": ["blue-2"]}},
        "routing_change": {
            "existing": [-2],
            "set": [2],
        },
        "expected_stops": [1, 2, 3, 4],
    },
    {
        "name": "Add Stop",
        "service": {"trip_properties": {"trip_id": ["blue-2"]}},
        "routing_change": {
            "existing": [2],
            "set": [2],
        },
        "expected_stops": [1, 2, 3, 4],
    },
    {
        "name": "Delete stop that doesn't exist and warn rather than fail",
        "service": {"trip_properties": {"trip_id": ["blue-2"]}},
        "routing_change": {
            "existing": [2],
            "set": [-2],
        },
        "expected_stops": [1, 3, 4],
    },
    {
        "name": "Remove Stop",
        "service": {"trip_properties": {"trip_id": ["blue-2"]}},
        "routing_change": {
            "existing": [3],
            "set": [-3],
        },
        "expected_stops": [1, 4],
    },
]


@pytest.mark.parametrize("test_routing", TEST_ROUTING_CHANGES + TEST_STOP_CHANGES)
def test_route_changes(request, small_transit_net, small_net, test_routing):
    WranglerLogger.info(f"--Starting: {request.node.name} - {test_routing['name']}")
    from network_wrangler.transit.projects.edit_routing import (
        apply_transit_routing_change,
    )

    net = small_transit_net.deepcopy()
    net.road_net = small_net
    WranglerLogger.info(f"Feed tables: {net.feed.table_names}")

    sel = net.get_selection(test_routing["service"])
    repr_trip_id = sel.selected_trips[0]

    net = apply_transit_routing_change(net, sel, test_routing["routing_change"])

    # Select a representative trip id to test
    trip_shape_nodes = shapes_for_trip_id(net.feed.shapes, net.feed.trips, repr_trip_id)[
        "shape_model_node_id"
    ].to_list()
    trip_stop_times_nodes = stop_times_for_trip_id(
        net.feed.stop_times, repr_trip_id
    ).stop_id.to_list()

    if "expected_routing" in test_routing:
        expected_shape = [abs(int(x)) for x in test_routing["expected_routing"]]
        missing_routing = list(set(expected_shape) - set(trip_shape_nodes))
        extra_routing = list(set(trip_shape_nodes) - set(expected_shape))
        if missing_routing:
            WranglerLogger.error(f"Missing shape records: {missing_routing}")
        if extra_routing:
            WranglerLogger.error(f"Extra unexpected shape records: {extra_routing}")
        assert expected_shape == trip_shape_nodes

        expected_stops_nodes = [n for n in test_routing["expected_routing"] if n > 0]
    elif "expected_stops" in test_routing:
        expected_stops_nodes = test_routing["expected_stops"]

    missing_stop_times_nodes = list(set(expected_stops_nodes) - set(trip_stop_times_nodes))
    extra_stop_times_nodes = list(set(trip_stop_times_nodes) - set(expected_stops_nodes))
    if missing_stop_times_nodes:
        WranglerLogger.error(f"Missing stop_times: {missing_stop_times_nodes}")
    if missing_stop_times_nodes:
        WranglerLogger.error(f"Extra unexpected stop_times: {extra_stop_times_nodes}")
    assert expected_stops_nodes == trip_stop_times_nodes

    missing_in_stops = list(set(expected_stops_nodes) - set(net.feed.stops.stop_id))
    if missing_in_stops:
        WranglerLogger.debug(f"stops.stop_id: \n{net.feed.stops[['stop_id']]}")
        WranglerLogger.error(f"Stops missing in stops.txt: {missing_in_stops}")
    assert not missing_in_stops
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_route_changes_project_card(
    request,
    stpaul_net: RoadwayNetwork,
    stpaul_card_dir: str,
    stpaul_transit_net: TransitNetwork,
):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    transit_net = copy.deepcopy(stpaul_transit_net)
    project_card = read_card(stpaul_card_dir / "transit.routing_change.yml")

    WranglerLogger.debug(f"Project Card: {project_card.__dict__}")
    WranglerLogger.debug(f"Types: {project_card.change_types}")
    transit_net = transit_net.apply(project_card, reference_road_net=stpaul_net)

    sel = transit_net.get_selection(project_card.transit_routing_change["service"])
    sel_trip = sel.selected_trips[0]
    trips_df = transit_net.feed.trips
    shapes_df = transit_net.feed.shapes
    shape_id = trips_df.loc[trips_df.trip_id == sel_trip, "shape_id"].values[0]
    # Shapes
    result = shapes_df[shapes_df.shape_id == shape_id]["shape_model_node_id"].tolist()
    answer = [
        37582,
        37574,
        4761,
        4763,
        4764,
        98429,
        45985,
        57483,
        126324,
        57484,
        150855,
        11188,
        84899,
        46666,
        46665,
        46663,
        81820,
        76167,
        77077,
        68609,
        39425,
        62146,
        41991,
        70841,
        45691,
        69793,
        45683,
        45685,
        7688,
        45687,
        100784,
        100782,
        45688,
        37609,
        19077,
        38696,
    ]
    assert result == answer

    # Stops
    result = transit_net.feed.stop_times[
        transit_net.feed.stop_times["trip_id"] == "14944022-JUN19-MVS-BUS-Weekday-01"
    ]["stop_id"].tolist()
    result_tail = result[-5:]
    answer_tail = [51814, 75787, 75122, 75788, 123002]
    assert result_tail == answer_tail

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_wo_existing(request, stpaul_net: RoadwayNetwork, stpaul_transit_net: TransitNetwork):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    transit_net = copy.deepcopy(stpaul_transit_net)

    selection_dict = {"trip_properties": {"trip_id": ["14986385-JUN19-MVS-BUS-Weekday-01"]}}
    change_dict = {
        "project": "test_wo_existing",
        "transit_routing_change": {
            "routing": {"set": [75318]},
            "service": selection_dict,
        },
    }

    transit_net = transit_net.apply(change_dict, reference_road_net=stpaul_net)

    # Stops
    result = transit_net.feed.stop_times[
        transit_net.feed.stop_times["trip_id"] == "14986385-JUN19-MVS-BUS-Weekday-01"
    ]["stop_id"].tolist()

    answer = [75318]
    assert set(result).issubset(answer)

    WranglerLogger.info(f"--Finished: {request.node.name}")

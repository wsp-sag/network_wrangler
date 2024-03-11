"""
Run just the tests using `pytest tests/test_transit/test_changes/test_transit_route_changes.py`
"""
import copy
import os

import pytest

import pandas as pd

from projectcard import read_card
from network_wrangler import RoadwayNetwork
from network_wrangler import TransitNetwork
from network_wrangler import WranglerLogger

TEST_ROUTING_REPLACEMENT = [
    {
        "base_routing": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "existing_routing": [4, 5, 6],
        "set_routing": [4, 41, 42, 63, 6],
        "expected_routing": [1, 2, 3, 4, 41, 42, 63, 6, 7, 8, 9, 10],
    },
    {
        "base_routing": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "existing_routing": [9, 10],
        "set_routing": [9, 99, 999],
        "expected_routing": [1, 2, 3, 4, 5, 6, 7, 8, 9, 99, 999],
    },
    {
        "base_routing": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "existing_routing": [1, 2],
        "set_routing": [11, 22],
        "expected_routing": [11, 22, 3, 4, 5, 6, 7, 8, 9, 10],
    },
]


@pytest.mark.parametrize("test_routing", TEST_ROUTING_REPLACEMENT)
def test_replace_shape_segment(request, test_routing):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    from network_wrangler.projects.transit_routing_change import _replace_shape_segment

    node_col = "shape_model_node_id"
    existing_routing = test_routing["existing_routing"]
    existing_shape_df = pd.DataFrame(
        {
            "shape_id": [1] * len(test_routing["base_routing"]),
            node_col: test_routing["base_routing"],
            "shape_pt_lat": [1] * len(test_routing["base_routing"]),
            "shape_pt_lon": [1] * len(test_routing["base_routing"]),
            "shape_pt_sequence": range(1, len(test_routing["base_routing"]) + 1),
        }
    )
    segment_shapes_df = pd.DataFrame(
        {
            "shape_id": [1] * len(test_routing["set_routing"]),
            node_col: test_routing["set_routing"],
            "shape_pt_lat": [1] * len(test_routing["set_routing"]),
            "shape_pt_lon": [1] * len(test_routing["set_routing"]),
            "shape_pt_sequence": range(1, len(test_routing["set_routing"]) + 1),
        }
    )

    shapes_df = _replace_shape_segment(
        existing_routing, existing_shape_df, segment_shapes_df, node_col
    )
    WranglerLogger.debug(f"Updated shapes_df\n {shapes_df}")
    result_routing = shapes_df[node_col].to_list()

    assert test_routing["expected_routing"] == result_routing

    WranglerLogger.info(f"--Finished: {request.node.name}")


TEST_ROUTING_CHANGES = [
    {
        "name": "Replace",
        "service": {"shape_id": "700004"},
        "routing_change": {
            "set": [41990, 39430, 46665],
        },
        "expected_routing": [41990, 39430, 46665],
    },
    {
        "name": "Truncate start",
        "service": {"shape_id": "700004"},
        "routing_change": {
            "existing": [41990, 76167],
            "set": [76167],
        },
        "expected_routing": [
            76167,
            -46665,
            -150855,
            57484,
            -126324,
            -57483,
            45985,
            98429,
            -4764,
            -4785,
            -4779,
            -41489,
            41487,
            -45957,
            55555,
            62186,
            61203,
            62188,
            59015,
            -59014,
            59013,
            -59012,
        ],
    },
    {
        "name": "Truncate end",
        "service": {"shape_id": "700004"},
        "routing_change": {
            "existing": [62188, 59013],
            "set": [62188],
        },
        "expected_routing": [
            41990,
            -62145,
            39430,
            -68608,
            76167,
            -46665,
            -150855,
            57484,
            -126324,
            -57483,
            45985,
            98429,
            -4764,
            -4785,
            -4779,
            -41489,
            41487,
            -45957,
            55555,
            62186,
            61203,
            62188,
        ],
    },
    {
        "name": "Change Middle",
        "service": {"shape_id": "700004"},
        "routing_change": {
            "existing": [76167, 57484],
            "set": [76167, 46665, 150855, 57484],
        },
        "expected_routing": [
            41990,
            -62145,
            39430,
            -68608,
            76167,
            46665,
            150855,
            57484,
            -126324,
            -57483,
            45985,
            98429,
            -4764,
            -4785,
            -4779,
            -41489,
            41487,
            -45957,
            55555,
            62186,
            61203,
            62188,
            59015,
            -59014,
            59013,
            -59012,
        ],
    },
]

TEST_STOP_CHANGES = [
    {
        "name": "Add Stop",
        "service": {"trip_id": "14947879-JUN19-MVS-BUS-Weekday-01"},
        "routing_change": {
            "existing": [-40253],
            "set": [40253],
        },
        "expected_stops": [
            40253,
            40251,
            40250,
            40244,
            12520,
            40240,
            12533,
            12540,
            12549,
            12551,
            67033,
            67033,
            5149,
            75307,
            57979,
            45946,
            45691,
            45691,
            100806,
            51941,
        ],
    },
    {
        "name": "Remove Stop",
        "service": {"trip_id": "14947879-JUN19-MVS-BUS-Weekday-01"},
        "routing_change": {
            "existing": [100806],
            "set": [-100806],
        },
        "expected_stops": [
            11440,
            40251,
            40250,
            40244,
            12520,
            40240,
            12533,
            12540,
            12549,
            12551,
            67033,
            67033,
            5149,
            75307,
            57979,
            45946,
            45691,
            45691,
            51941,
        ],
    },
]


@pytest.mark.menow
@pytest.mark.parametrize("test_routing", TEST_ROUTING_CHANGES + TEST_STOP_CHANGES)
def test_route_changes(request, stpaul_transit_net: TransitNetwork, test_routing):
    WranglerLogger.info(f"--Starting: {request.node.name} - {test_routing['name']}")
    from network_wrangler.projects.transit_routing_change import (
        apply_transit_routing_change,
    )

    net = stpaul_transit_net.deepcopy()
    WranglerLogger.info(f"Feed tables: {net.feed.table_names}")

    sel = net.get_selection(test_routing["service"])

    net = apply_transit_routing_change(net, sel, test_routing["routing_change"])

    # Select a representative trip id to test
    repr_trip_id = sel.selected_trips[0]
    trip_shape = net.feed.trip_shape(repr_trip_id)
    trip_shape_nodes = trip_shape["shape_model_node_id"].to_list()
    trip_stop_times_df = net.feed.trip_stop_times(repr_trip_id)
    trip_stop_times_nodes = trip_stop_times_df.model_node_id.to_list()

    _show_col = [
        "trip_id",
        "stop_id",
        net.feed.stops_node_id,
        "stop_sequence",
        "departure_time",
        "arrival_time",
    ]

    WranglerLogger.debug(f"trip_stop_times_df:\n{trip_stop_times_df[_show_col]}")
    WranglerLogger.debug(f"trip_shape_df:\n{trip_shape['shape_model_node_id']}")

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

    missing_stop_times_nodes = list(
        set(expected_stops_nodes) - set(trip_stop_times_nodes)
    )
    extra_stop_times_nodes = list(
        set(trip_stop_times_nodes) - set(expected_stops_nodes)
    )
    if missing_stop_times_nodes:
        WranglerLogger.error(f"Missing stop_times: {missing_stop_times_nodes}")
    if missing_stop_times_nodes:
        WranglerLogger.error(f"Extra unexpected stop_times: {extra_stop_times_nodes}")
    assert expected_stops_nodes == trip_stop_times_nodes

    missing_in_stops = list(
        set(expected_stops_nodes) - set(net.feed.stops.model_node_id)
    )
    if missing_in_stops:
        WranglerLogger.debug(
            f"stops.model_node_id:\n{net.feed.stops[['model_node_id','stop_id']]}"
        )
        WranglerLogger.error(f"Stops missing in stops.txt: {missing_in_stops}")
    assert not missing_in_stops
    WranglerLogger.info(f"--Finished: {request.node.name}")


@pytest.mark.failing
def test_route_changes_project_card(
    request,
    stpaul_net: RoadwayNetwork,
    stpaul_card_dir: str,
    stpaul_transit_net: TransitNetwork,
):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    transit_net = copy.deepcopy(stpaul_transit_net)
    transit_net.road_net = stpaul_net
    project_card = read_card(
        os.path.join(stpaul_card_dir, "transit.routing_change.yml")
    )

    WranglerLogger.debug(f"Project Card: {project_card.__dict__}")
    WranglerLogger.debug(f"Types: {project_card.types}")
    transit_net = transit_net.apply(project_card)

    sel = transit_net.get_selection(project_card.service)
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
    answer_tail = ["17013", "17010", "17009", "17006", "17005"]
    assert result_tail == answer_tail

    WranglerLogger.info(f"--Finished: {request.node.name}")


@pytest.mark.failing
def test_wo_existing(
    request, stpaul_net: RoadwayNetwork, stpaul_transit_net: TransitNetwork
):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    transit_net = copy.deepcopy(stpaul_transit_net)
    transit_net.road_net = stpaul_net

    selection_dict = {"trip_id": ["14986385-JUN19-MVS-BUS-Weekday-01"]}
    change_dict = {"transit_routing_change": {"routing": {"set": [75318]}}}

    transit_net = transit_net.apply(
        transit_net.get_selection(selection_dict), change_dict
    )

    # Stops
    result = transit_net.feed.stop_times[
        transit_net.feed.stop_times["trip_id"] == "14986385-JUN19-MVS-BUS-Weekday-01"
    ]["stop_id"].tolist()

    answer = ["2609"]  # first matching stop_id in stops.txt
    assert result == answer

    WranglerLogger.info(f"--Finished: {request.node.name}")

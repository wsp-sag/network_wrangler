"""
Run just the tests using `pytest tests/test_transit/test_feed.py`
"""
import pytest

import pandas as pd
from network_wrangler import WranglerLogger


def test_tables_with_property(request, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    result_1 = stpaul_transit_net.feed.tables_with_property("route_short_name")
    result_2 = stpaul_transit_net.feed.tables_with_property("agency_id")
    result_3 = stpaul_transit_net.feed.tables_with_property("wheelchair_boarding")

    assert result_1 == ["routes"]
    assert result_2 == ["agency", "routes"]
    assert result_3 == ["stops"]

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_trip_stop_times(request, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    trip_id = "048-RD-489.1S-0639-20190216-Weekday-04"
    stop_times = stpaul_transit_net.feed.trip_stop_times(trip_id)

    result = stop_times.stop_id.to_list()
    expected = [
        "01612",
        "01583",
        "01753",
        "00801",
        "00802",
        "00803",
        "00804",
        "00805",
        "00806",
        "01452",
    ]

    assert result == expected

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_trip_shape_id(request, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    trip_id = "14946257-JUN19-MVS-BUS-Weekday-01"
    result = stpaul_transit_net.feed.trip_shape_id(trip_id)
    WranglerLogger.debug(f"test_trip_shape_id result: {result}")
    expected = "3550003"

    assert result == expected

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_trip_shape(request, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    trip_id = "14969944-JUN19-RAIL-Weekday-01"
    shape = stpaul_transit_net.feed.trip_shape(trip_id)

    # shape_id is "9020001"
    result = shape.shape_model_node_id.to_list()
    expected = [171260, 171261, 171262, 171263, 171264, 171265, 171266, 171267, 171268]

    assert result == expected

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_trip_stop_pattern(request, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    trip_id = "14943415-JUN19-MVS-BUS-Weekday-01"
    result_1 = stpaul_transit_net.feed.trip_stop_pattern(trip_id, pickup_type="either")
    result_2 = stpaul_transit_net.feed.trip_stop_pattern(trip_id, pickup_type="both")
    result_3 = stpaul_transit_net.feed.trip_stop_pattern(
        trip_id, pickup_type="pickup_only"
    )
    result_4 = stpaul_transit_net.feed.trip_stop_pattern(
        trip_id, pickup_type="dropoff_only"
    )

    expected_1 = [
        "52761",
        "52758",
        "11834",
        "3142",
        "48302",
        "11837",
        "11838",
        "11840",
        "48338",
    ]
    expected_2 = ["52761", "48338"]
    expected_3 = []
    expected_4 = ["52758", "11834", "3142", "48302", "11837", "11838", "11840"]

    assert result_1 == expected_1
    assert result_2 == expected_2
    assert result_3 == expected_3

    assert result_4 == expected_4
    WranglerLogger.info(f"--Finished: {request.node.name}")

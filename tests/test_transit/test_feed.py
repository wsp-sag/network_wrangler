"""
Run just the tests using `pytest tests/test_transit/test_feed.py`
"""
import pytest

import pandas as pd
from network_wrangler import WranglerLogger

TEST_TABLES_W_PROP = [
    ("route_short_name", ["routes"]),
    ("agency_id", ["agency", "routes"]),
    ("wheelchair_boarding", ["stops"]),
]


@pytest.mark.parametrize("prop, expected_tables", TEST_TABLES_W_PROP)
def test_tables_with_property(request, stpaul_transit_net, prop, expected_tables):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    tables = stpaul_transit_net.feed.tables_with_property(prop)
    assert tables == expected_tables

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


TEST_TRIP_PATTERNS = [
    {
        "trip_id": "14940701-JUN19-MVS-BUS-Weekday-01",
        "pickup_type": "either",
        "answer": [
            "52761",
            "52758",
            "11834",
            "3142",
            "48302",
            "11837",
            "11838",
            "11840",
            "48338",
        ],
    },
    {
        "trip_id": "14940701-JUN19-MVS-BUS-Weekday-01",
        "pickup_type": "both",
        "answer": ["52761", "48338"],
    },
    {
        "trip_id": "14940701-JUN19-MVS-BUS-Weekday-01",
        "pickup_type": "pickup_only",
        "answer": [],
    },
    {
        "trip_id": "14940701-JUN19-MVS-BUS-Weekday-01",
        "pickup_type": "dropoff_only",
        "answer": ["52758", "11834", "3142", "48302", "11837", "11838", "11840"],
    },
]


@pytest.mark.parametrize("tpat_test", TEST_TRIP_PATTERNS)
def test_trip_stop_pattern(request, tpat_test, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    result = stpaul_transit_net.feed.trip_stop_pattern(
        tpat_test["trip_id"], tpat_test["pickup_type"]
    )

    assert result == tpat_test["answer"]

    WranglerLogger.info(f"--Finished: {request.node.name}")

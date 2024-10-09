"""Tests for transit selection.

Run just these tests using `pytesttests/test_transit/test_selections.py`
"""

import pytest
from pydantic import ValidationError

from network_wrangler import WranglerLogger
from network_wrangler.transit.selection import (
    TransitSelectionEmptyError,
    TransitSelectionNetworkConsistencyError,
)

TEST_SELECTIONS = [
    {
        "name": "0. simple trip_id",
        "service": {"trip_properties": {"trip_id": ["14940701-JUN19-MVS-BUS-Weekday-01"]}},
        "answer": ["14940701-JUN19-MVS-BUS-Weekday-01"],
    },
    {
        "name": "1. trip_id + time",
        "service": {
            "trip_properties": {"trip_id": ["14940701-JUN19-MVS-BUS-Weekday-01"]},
            "timespans": [["06:00:00", "09:00:00"]],
        },
        "answer": ["14940701-JUN19-MVS-BUS-Weekday-01"],
    },
    {
        "name": "2. multiple trip_id",
        "service": {
            "trip_properties": {
                "trip_id": [
                    "14969841-JUN19-RAIL-Weekday-01",  # unordered
                    "14940701-JUN19-MVS-BUS-Weekday-01",
                ]
            }
        },
        "answer": [
            "14940701-JUN19-MVS-BUS-Weekday-01",
            "14969841-JUN19-RAIL-Weekday-01",
        ],
    },
    {
        "name": "3. multiple trip_id + time",
        "service": {
            "trip_properties": {
                "trip_id": [
                    "14940701-JUN19-MVS-BUS-Weekday-01",
                    "14948032-JUN19-MVS-BUS-Weekday-01",
                ]
            },
            "timespans": [["06:00:00", "09:00:00"]],
        },
        "answer": [
            "14940701-JUN19-MVS-BUS-Weekday-01",
        ],
    },
    {
        "name": "4. route_id",
        "service": {"trip_properties": {"route_id": ["365-111"]}},
        "answer": ["14947182-JUN19-MVS-BUS-Weekday-01"],
    },
    {
        "name": "5. route_id + time",
        "service": {
            "trip_properties": {"route_id": ["21-111"]},
            "timespans": [["09:00", "15:00"]],
        },
        "answer": [
            "14944012-JUN19-MVS-BUS-Weekday-01",
            "14944019-JUN19-MVS-BUS-Weekday-01",
        ],
    },
    {
        "name": "6. multiple route_id + time",
        "service": {
            "trip_properties": {"route_id": ["21-111", "53-111"]},
            "timespans": [["08:00", "15:00"]],
        },
        "answer": [
            "14944012-JUN19-MVS-BUS-Weekday-01",
            "14944018-JUN19-MVS-BUS-Weekday-01",
            "14944019-JUN19-MVS-BUS-Weekday-01",
            "14944022-JUN19-MVS-BUS-Weekday-01",
            "14948211-JUN19-MVS-BUS-Weekday-01",  # additional for 53-111
            "14948218-JUN19-MVS-BUS-Weekday-01",
        ],
    },
    {
        "name": "7. route long name contains",
        "service": {"route_properties": {"route_long_name": ["Express"]}},
        "answer": [
            "14940701-JUN19-MVS-BUS-Weekday-01",
            "14943414-JUN19-MVS-BUS-Weekday-01",
            "14943415-JUN19-MVS-BUS-Weekday-01",
            "14946111-JUN19-MVS-BUS-Weekday-01",
            "14946257-JUN19-MVS-BUS-Weekday-01",
            "14946470-JUN19-MVS-BUS-Weekday-01",
            "14946471-JUN19-MVS-BUS-Weekday-01",
            "14946480-JUN19-MVS-BUS-Weekday-01",
            "14946521-JUN19-MVS-BUS-Weekday-01",
            "14947182-JUN19-MVS-BUS-Weekday-01",
            "14947504-JUN19-MVS-BUS-Weekday-01",
            "14947734-JUN19-MVS-BUS-Weekday-01",
            "14947755-JUN19-MVS-BUS-Weekday-01",
            "14978409-JUN19-MVS-BUS-Weekday-01",
            "14981028-JUN19-MVS-BUS-Weekday-01",
            "14981029-JUN19-MVS-BUS-Weekday-01",
            "14986383-JUN19-MVS-BUS-Weekday-01",
            "14986385-JUN19-MVS-BUS-Weekday-01",
        ],
    },
    {
        "name": "8. multiple route long name",
        "service": {"route_properties": {"route_long_name": ["Express", "Ltd Stop"]}},
        "answer": [
            "14940701-JUN19-MVS-BUS-Weekday-01",
            "14943414-JUN19-MVS-BUS-Weekday-01",
            "14943415-JUN19-MVS-BUS-Weekday-01",
            "14946111-JUN19-MVS-BUS-Weekday-01",
            "14946257-JUN19-MVS-BUS-Weekday-01",
            "14946470-JUN19-MVS-BUS-Weekday-01",
            "14946471-JUN19-MVS-BUS-Weekday-01",
            "14946480-JUN19-MVS-BUS-Weekday-01",
            "14946521-JUN19-MVS-BUS-Weekday-01",
            "14947182-JUN19-MVS-BUS-Weekday-01",
            "14947504-JUN19-MVS-BUS-Weekday-01",
            "14947734-JUN19-MVS-BUS-Weekday-01",
            "14947755-JUN19-MVS-BUS-Weekday-01",
            "14978409-JUN19-MVS-BUS-Weekday-01",
            "14981028-JUN19-MVS-BUS-Weekday-01",
            "14981029-JUN19-MVS-BUS-Weekday-01",
            "14986383-JUN19-MVS-BUS-Weekday-01",
            "14986385-JUN19-MVS-BUS-Weekday-01",
            "14946199-JUN19-MVS-BUS-Weekday-01",  # add below for Ltd Stop
            "14947598-JUN19-MVS-BUS-Weekday-01",
            "14948211-JUN19-MVS-BUS-Weekday-01",
            "14948218-JUN19-MVS-BUS-Weekday-01",
        ],
    },
]


@pytest.mark.parametrize("selection", TEST_SELECTIONS)
def test_select_transit_features_by_properties(
    request,
    selection,
    stpaul_transit_net,
):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    sel = selection["service"]
    WranglerLogger.info(f"     Name: {selection['name']}")
    WranglerLogger.debug(f"     Service: {sel}")

    selected_trips = set(stpaul_transit_net.get_selection(sel).selected_trips)
    answer = set(selection["answer"])
    if selected_trips - answer:
        WranglerLogger.error(f"!!! Trips overselected: \n   {selected_trips - answer}")
    if answer - selected_trips:
        WranglerLogger.error(f"!!! Trips missing in selection: \n   {answer - selected_trips}")

    assert selected_trips == answer

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_zero_valid_facilities(request, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    with pytest.raises(TransitSelectionEmptyError):
        sel_trips = stpaul_transit_net.get_selection(
            {
                "trip_properties": {
                    "trip_id": ["14941433-JUN19-MVS-BUS-Weekday-01"],
                },
                "timespans": [["06:00:00", "09:00:00"]],
            }
        ).selected_trips

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_invalid_selection_key(request, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    with pytest.raises(ValidationError):
        # trump_properties rather than trip_properties should fail
        stpaul_transit_net.get_selection(
            {"trump_properties": {"trip_ids": ["14941433-JUN19-MVS-BUS-Weekday-01"]}}
        )

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_invalid_selection_property_format(request, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    with pytest.raises(TransitSelectionNetworkConsistencyError):
        # agency_iden intsead of agency_id shoul dfail
        stpaul_transit_net.get_selection(
            {
                "timespans": [["12:00", "1:00"]],
                "route_properties": {"agency_ident": "1"},
            }
        )

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_invalid_selection_property(request, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    with pytest.raises(TransitSelectionNetworkConsistencyError):
        # trip_ids rather than trip_id should fail
        stpaul_transit_net.get_selection(
            {"trip_properties": {"trip_ids": ["14941433-JUN19-MVS-BUS-Weekday-01"]}}
        )

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_invalid_optional_selection_variable(request, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    with pytest.raises(TransitSelectionNetworkConsistencyError):
        # `wheelchair` rather than `wheelchair_accessible`
        stpaul_transit_net.get_selection(
            {
                "trip_properties": {
                    "trip_id": ["14940701-JUN19-MVS-BUS-Weekday-01"],
                    "trips.wheelchair": 0,
                }
            }
        )
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_correct_optional_selection_variable(request, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    # Correct trip variable
    sel = stpaul_transit_net.get_selection(
        {
            "trip_properties": {
                "trip_id": ["14940701-JUN19-MVS-BUS-Weekday-01"],
                "wheelchair_accessible": 1,
            }
        }
    ).selected_trips
    assert set(sel) == {"14940701-JUN19-MVS-BUS-Weekday-01"}

    # Correct route variable
    sel = stpaul_transit_net.get_selection(
        {"route_properties": {"route_long_name": ["Express"], "agency_id": ["2"]}}
    ).selected_trips
    assert set(sel) == {"14978409-JUN19-MVS-BUS-Weekday-01"}

    WranglerLogger.info(f"--Finished: {request.node.name}")


TEST_NODE_SELECTIONS = [
    {
        "name": "Any of the listed nodes - default to any",
        # check that it will coerce the strings to ints and that it will default to "any"
        "service": {
            "nodes": {
                "model_node_id": ["75520", "66380", "57530"],
            }
        },
        "answer": [
            "14941148-JUN19-MVS-BUS-Weekday-01",
            "14941151-JUN19-MVS-BUS-Weekday-01",
            "14941153-JUN19-MVS-BUS-Weekday-01",
            "14941163-JUN19-MVS-BUS-Weekday-01",
            "14944379-JUN19-MVS-BUS-Weekday-01",
            "14944386-JUN19-MVS-BUS-Weekday-01",
            "14944413-JUN19-MVS-BUS-Weekday-01",
            "14944416-JUN19-MVS-BUS-Weekday-01",
        ],
    },
    {
        "name": "All of listed nodes",
        # check that "require":"all" functions as expected
        "service": {"nodes": {"model_node_id": ["75520", "66380"], "require": "all"}},
        "answer": [
            "14941148-JUN19-MVS-BUS-Weekday-01",
            "14941151-JUN19-MVS-BUS-Weekday-01",
            "14941153-JUN19-MVS-BUS-Weekday-01",
            "14941163-JUN19-MVS-BUS-Weekday-01",
        ],
    },
]


@pytest.mark.parametrize("selection", TEST_NODE_SELECTIONS)
def test_select_transit_features_by_nodes(
    request,
    selection,
    stpaul_transit_net,
):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    sel = selection["service"]
    WranglerLogger.info(f"     Name: {selection['name']}")
    WranglerLogger.debug(f"     Service: {sel}")

    selected_trips = set(stpaul_transit_net.get_selection(sel).selected_trips)
    answer = set(selection["answer"])
    if selected_trips - answer:
        WranglerLogger.error(f"!!! Trips overselected: \n   {selected_trips - answer}")
    if answer - selected_trips:
        WranglerLogger.error(f"!!! Trips missing in selection: \n   {answer - selected_trips}")

    assert selected_trips == answer

    WranglerLogger.info(f"--Finished: {request.node.name}")


TEST_LINK_SELECTIONS = [
    {
        "name": "All Links",
        "service": {
            "links": {"ab_nodes": [{"A": "75520", "B": "66380"}], "require": "all"},
        },
        "answer": [
            "14941148-JUN19-MVS-BUS-Weekday-01",
            "14941151-JUN19-MVS-BUS-Weekday-01",
            "14941153-JUN19-MVS-BUS-Weekday-01",
            "14941163-JUN19-MVS-BUS-Weekday-01",
        ],
    },
    {
        "name": "Any Links",
        "service": {
            "links": {
                "ab_nodes": [
                    {"A": "75520", "B": "66380"},
                    {"A": "66380", "B": "75520"},
                ],
                "require": "any",
            },
        },
        "answer": [
            "14941148-JUN19-MVS-BUS-Weekday-01",
            "14941151-JUN19-MVS-BUS-Weekday-01",
            "14941153-JUN19-MVS-BUS-Weekday-01",
            "14941163-JUN19-MVS-BUS-Weekday-01",
        ],
    },
]


@pytest.mark.parametrize("selection", TEST_LINK_SELECTIONS)
def test_select_transit_features_by_links(
    request,
    selection,
    stpaul_transit_net,
):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    sel = selection["service"]
    WranglerLogger.info(f"     Name: {selection['name']}")
    WranglerLogger.debug(f"     Service: {sel}")

    with pytest.raises(NotImplementedError):
        selected_trips = set(stpaul_transit_net.get_selection(sel).selected_trips)

    WranglerLogger.info(f"--Finished: {request.node.name}")

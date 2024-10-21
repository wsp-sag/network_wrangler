"""Basic transit testing.

Run just the tests here by running pytest tests/test_transit/test_transit.py`
"""

import copy

import pytest
from projectcard import read_card

from network_wrangler import WranglerLogger

TEST_PROJECT_CARDS = [
    {
        "file": "transit.prop_change.trip_time.yml",
        "answer": {
            "trip_ids": ["14940701-JUN19-MVS-BUS-Weekday-01"],
            "headway_secs": [900],
        },
    },
    {
        "file": "transit.prop_change.route_time.yml",
        "answer": {
            "trip_ids": [
                "14944012-JUN19-MVS-BUS-Weekday-01",
                "14944019-JUN19-MVS-BUS-Weekday-01",
            ],
            "headway_secs": [253, 226],
        },
    },
    {
        "file": "transit.prop_change.route_name_contains.yml",
        "answer": {
            "trip_ids": [
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
            "headway_secs": [1800],
        },
    },
]


@pytest.mark.parametrize("test_project", TEST_PROJECT_CARDS)
def test_apply_transit_feature_change_from_projectcard(
    request, stpaul_transit_net, stpaul_card_dir, test_project
):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    WranglerLogger.debug("   File:  test_project['file']")

    project_card_path = stpaul_card_dir / test_project["file"]
    project_card = read_card(project_card_path)
    stpaul_transit_net = stpaul_transit_net.apply(project_card)

    freq = stpaul_transit_net.feed.frequencies
    answers = test_project["answer"]["headway_secs"]

    for i, answer in enumerate(answers):
        match = freq.trip_id == test_project["answer"]["trip_ids"][i]
        result = freq[match]["headway_secs"].values[0]
        assert result == answer

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_wrong_existing(request, small_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    my_project_name = "my_wrong_existing_project"
    tpc = {
        "project": my_project_name,
        "transit_property_change": {
            "service": {
                "trip_properties": {
                    "trip_id": [
                        "blue-2",
                    ]
                }
            },
            "property_changes": {
                "wheelchair_accessible": {"set": 0},
                "headway_secs": {
                    "existing": 553,
                    "set": 321,
                },
            },
        },
    }
    # Default behavior is warn, so it should still aply the project.
    net = copy.deepcopy(small_transit_net)
    net.apply(tpc)
    assert my_project_name in net.applied_projects
    assert (
        net.feed.frequencies.loc[net.feed.frequencies.trip_id == "blue-2", "headway_secs"].values[
            0
        ]
        == 321
    )
    assert (
        net.feed.trips.loc[net.feed.trips.trip_id == "blue-2", "wheelchair_accessible"].values[0]
        == 0
    )

    # Now we will set the behavior to error, so it should raise an error.
    from network_wrangler.transit.projects.edit_property import TransitPropertyChangeError

    tpc["transit_property_change"]["property_changes"]["headway_secs"][
        "existing_value_conflict"
    ] = "error"
    net = copy.deepcopy(small_transit_net)
    with pytest.raises(TransitPropertyChangeError):
        net.apply(tpc)

    # Now we will set the behavior to skip, so it should not apply that change.
    tpc["transit_property_change"]["property_changes"]["headway_secs"][
        "existing_value_conflict"
    ] = "skip"
    net = copy.deepcopy(small_transit_net)
    existing_wheelchair = net.feed.trips.loc[
        net.feed.trips.trip_id == "blue-2", "wheelchair_accessible"
    ].values[0]
    net.apply(tpc)
    assert my_project_name in net.applied_projects
    assert (
        net.feed.trips.loc[net.feed.trips.trip_id == "blue-2", "wheelchair_accessible"].values[0]
        == 0
    )
    assert (
        net.feed.frequencies.loc[net.feed.frequencies.trip_id == "blue-2", "headway_secs"].values[
            0
        ]
        == 900
    )

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_transit_road_consistencies(request, stpaul_transit_net, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    stpaul_transit_net.road_net = stpaul_net
    assert stpaul_transit_net.consistent_with_road_net

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_transit_gdfs(request, stpaul_transit_net, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    transit_net = copy.deepcopy(stpaul_transit_net)
    transit_net.road_net = stpaul_net
    assert not transit_net.stop_time_links_gdf.empty
    assert not transit_net.stop_times_points_gdf.empty
    assert not transit_net.shapes_gdf.empty
    assert not transit_net.shape_links_gdf.empty
    assert not transit_net.stops_gdf.empty
    WranglerLogger.info(f"--Finished: {request.node.name}")

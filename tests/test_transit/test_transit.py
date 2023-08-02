import os
import pytest
from projectcard import read_card
from network_wrangler import WranglerLogger

"""
Run just the tests here by running pytest tests/test_transit/test_transit.py`
"""
TEST_PROJECT_CARDS = [
    {
        "file": "7_simple_transit_attribute_change.yml",
        "answer": {
            "trip_ids": ["14940701-JUN19-MVS-BUS-Weekday-01"],
            "headway_secs": [900],
        },
    },
    {
        "file": "8_simple_transit_attribute_change.yml",
        "answer": {
            "trip_ids": [
                "14944012-JUN19-MVS-BUS-Weekday-01",
                "14944019-JUN19-MVS-BUS-Weekday-01",
            ],
            "headway_secs": [253, 226],
        },
    },
    {
        "file": "9_simple_transit_attribute_change.yml",
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

    project_card_path = os.path.join(stpaul_card_dir, test_project["file"])
    project_card = read_card(project_card_path)
    stpaul_transit_net = stpaul_transit_net.apply(project_card)

    freq = stpaul_transit_net.feed.frequencies
    answers = test_project["answer"]["headway_secs"]

    for i, answer in enumerate(answers):
        match = freq.trip_id == test_project["answer"]["trip_ids"][i]
        result = freq[match]["headway_secs"]
        assert set(result) == set([answer])

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_wrong_existing(request, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    selected_trips = stpaul_transit_net.get_selection(
        {
            "trip_id": [
                "14944018-JUN19-MVS-BUS-Weekday-01",
                "14944012-JUN19-MVS-BUS-Weekday-01",
            ]
        }
    ).selected_trips

    with pytest.raises(Exception):
        stpaul_transit_net.apply_transit_feature_change(
            selected_trips, [{"property": "headway_secs", "existing": 553, "set": 900}]
        )

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_transit_road_consistencies(request, stpaul_transit_net, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    stpaul_transit_net.road_net = stpaul_net
    _consistency = stpaul_transit_net._evaluate_consistency_with_road_net()
    WranglerLogger.info(f"Consistency with RoadNet: {_consistency}")

    WranglerLogger.info(f"--Finished: {request.node.name}")

import os
import pytest
from network_wrangler import TransitNetwork
from projectcard import read_card
from network_wrangler import RoadwayNetwork
from network_wrangler import WranglerLogger

"""
Run just the tests labeled transit using `pytest -v -m transit`
"""


def test_transit_read_write(request, stpaul_transit_net,scratch_dir):
    """Checks that reading a network, writing it to a file and then reading it again 
    results in a valid TransitNetwork.
    """
    stpaul_transit_net.write(path=scratch_dir)
    WranglerLogger.debug("Transit Write Directory:", scratch_dir)
    stpaul_transit_net_read_write = TransitNetwork.read(scratch_dir)
    assert isinstance(stpaul_transit_net_read_write, TransitNetwork)

    WranglerLogger.info(f"--Finished: {request.node.name}")

@pytest.mark.menow
def test_apply_transit_feature_change_from_projectcard(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = TransitNetwork.read(STPAUL_DIR)

    test_selections = [
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

    for i, test in enumerate(test_selections):
        print("--->", i)
        print("Reading project card", test["file"], "...")

        project_card_path = os.path.join(STPAUL_DIR, "project_cards", test["file"])
        project_card = read_card(project_card_path)
        net = net.apply_transit_feature_change(
            net.select_transit_features(project_card.facility), project_card.properties
        )

        freq = net.feed.frequencies
        answers = test["answer"]["headway_secs"]
        if len(answers) > 1:
            for i, answer in enumerate(answers):
                match = freq.trip_id == test["answer"]["trip_ids"][i]
                result = freq[match]["headway_secs"]
                assert set(result) == set([answer])
        else:
            matches = freq.trip_id.isin(test["answer"]["trip_ids"])
            results = freq[matches]["headway_secs"].tolist()
            assert set(results) == set(answers)

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_wrong_existing(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = TransitNetwork.read(STPAUL_DIR)

    selected_trips = net.select_transit_features(
        {
            "trip_id": [
                "14944018-JUN19-MVS-BUS-Weekday-01",
                "14944012-JUN19-MVS-BUS-Weekday-01",
            ]
        }
    )

    with pytest.raises(Exception):
        net = net.apply_transit_feature_change(
            selected_trips, [{"property": "headway_secs", "existing": 553, "set": 900}]
        )

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_zero_valid_facilities(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = TransitNetwork.read(STPAUL_DIR)

    with pytest.raises(Exception):
        net.select_transit_features(
            {
                "trip_id": ["14941433-JUN19-MVS-BUS-Weekday-01"],
                "time": ["06:00:00", "09:00:00"],
            }
        )

    print("--Finished:", request.node.name)


def test_invalid_selection_key(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = TransitNetwork.read(STPAUL_DIR)

    with pytest.raises(Exception):
        # trip_ids rather than trip_id should fail
        net.select_transit_features({"trip_ids": ["14941433-JUN19-MVS-BUS-Weekday-01"]})

    print("--Finished:", request.node.name)


def test_invalid_optional_selection_variable(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = TransitNetwork.read(STPAUL_DIR)

    with pytest.raises(Exception):
        # `wheelchair` rather than `wheelchair_accessible`
        net.select_transit_features(
            {"trip_id": "14940701-JUN19-MVS-BUS-Weekday-01", "wheelchair": "0"}
        )

    with pytest.raises(Exception):
        # Missing trip_id, route_id, route_short_name, or route_long_name
        net.select_transit_features({"wheelchair_accessible": "0"})

    # Correct trip variable
    sel = net.select_transit_features(
        {"trip_id": "14940701-JUN19-MVS-BUS-Weekday-01", "wheelchair_accessible": 1}
    )
    assert set(sel) == set(["14940701-JUN19-MVS-BUS-Weekday-01"])

    # Correct route variable
    sel = net.select_transit_features({"route_long_name": "Express", "agency_id": "2"})
    assert set(sel) == set(["14978409-JUN19-MVS-BUS-Weekday-01"])

    WranglerLogger.info(f"--Finished: {request.node.name}")

@pytest.mark.menow
def test_transit_road_consistencies(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = TransitNetwork.read(STPAUL_DIR)

    STPAUL_SHAPE_FILE = os.path.join(STPAUL_DIR, "shape.geojson")
    STPAUL_LINK_FILE = os.path.join(STPAUL_DIR, "link.json")
    STPAUL_NODE_FILE = os.path.join(STPAUL_DIR, "node.geojson")

    road_net = RoadwayNetwork.read(
        links_file=STPAUL_LINK_FILE,
        nodes_file=STPAUL_NODE_FILE,
        shapes_file=STPAUL_SHAPE_FILE,
    )

    net.set_roadnet(road_net=road_net)

    net.validate_road_network_consistencies()
    print(net.validated_road_network_consistency)
    WranglerLogger.info(f"--Finished: {request.node.name}")


if __name__ == "__main__":
    test_transit_read_write()

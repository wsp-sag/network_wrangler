import os
import json
import pytest
from network_wrangler import RoadwayNetwork
from network_wrangler import TransitNetwork
from network_wrangler import ProjectCard


"""
Run just the tests labeled transit using `pytest -v -m transit`
"""

STPAUL_DIR = os.path.join(os.getcwd(), "examples", "stpaul")
SCRATCH_DIR = os.path.join(os.getcwd(), "scratch")


@pytest.mark.roadway
@pytest.mark.transit
@pytest.mark.travis
def test_set_roadnet(request):
    print("\n--Starting:", request.node.name)

    road_net = RoadwayNetwork.read(
        link_file=os.path.join(STPAUL_DIR, "link.json"),
        node_file=os.path.join(STPAUL_DIR, "node.geojson"),
        shape_file=os.path.join(STPAUL_DIR, "shape.geojson"),
        fast=True,
    )
    transit_net = TransitNetwork.read(STPAUL_DIR)
    transit_net.set_roadnet(road_net)

    print("--Finished:", request.node.name)


@pytest.mark.roadway
@pytest.mark.transit
@pytest.mark.travis
# @pytest.mark.skip("")
def test_project_card(request):
    print("\n--Starting:", request.node.name)

    road_net = RoadwayNetwork.read(
        link_file=os.path.join(STPAUL_DIR, "link.json"),
        node_file=os.path.join(STPAUL_DIR, "node.geojson"),
        shape_file=os.path.join(STPAUL_DIR, "shape.geojson"),
        fast=True,
    )
    transit_net = TransitNetwork.read(STPAUL_DIR)
    transit_net.road_net = road_net
    project_card_path = os.path.join(
        STPAUL_DIR, "project_cards", "12_transit_shape_change.yml"
    )
    project_card = ProjectCard.read(project_card_path)
    transit_net.apply_transit_feature_change(
        transit_net.select_transit_features(project_card.facility),
        project_card.properties,
    )

    # Shapes
    result = transit_net.feed.shapes[transit_net.feed.shapes["shape_id"] == "2940002"][
        "shape_model_node_id"
    ].tolist()
    answer = [
        "37582",
        "37574",
        "4761",
        "4763",
        "4764",
        "98429",
        "45985",
        "57483",
        "126324",
        "57484",
        "150855",
        "11188",
        "84899",
        "46666",
        "46665",
        "46663",
        "81820",
        "76167",
        "77077",
        "68609",
        "39425",
        "62146",
        "41991",
        "70841",
        "45691",
        "69793",
        "45683",
        "45685",
        "7688",
        "45687",
        "100784",
        "100782",
        "45688",
        "37609",
        "19077",
        "38696",
    ]
    assert result == answer

    # Stops
    result = transit_net.feed.stop_times[
        transit_net.feed.stop_times["trip_id"] == "14944022-JUN19-MVS-BUS-Weekday-01"
    ]["stop_id"].tolist()
    result_tail = result[-5:]
    answer_tail = ["17013", "17010", "17009", "17006", "17005"]
    assert result_tail == answer_tail

    print("--Finished:", request.node.name)


@pytest.mark.roadway
@pytest.mark.transit
@pytest.mark.travis
# @pytest.mark.skip("need to allow for creating new stops if they don't already exist in stops.txt")
def test_wo_existing(request):
    print("\n--Starting:", request.node.name)

    road_net = RoadwayNetwork.read(
        link_file=os.path.join(STPAUL_DIR, "link.json"),
        node_file=os.path.join(STPAUL_DIR, "node.geojson"),
        shape_file=os.path.join(STPAUL_DIR, "shape.geojson"),
        fast=True,
    )
    transit_net = TransitNetwork.read(STPAUL_DIR)
    transit_net.road_net = road_net
    # A new node ID (not in stops.txt) should fail right now
    """with pytest.raises(Exception):
        transit_net.apply_transit_feature_change(
            trip_ids=transit_net.select_transit_features(
                {"trip_id": ["14944022-JUN19-MVS-BUS-Weekday-01"]}
            ),
            properties=[
                {
                    "property": "routing",
                    "set": [1]
                }
            ]
        )"""

    transit_net.apply_transit_feature_change(
        trip_ids=transit_net.select_transit_features(
            {"trip_id": ["14986385-JUN19-MVS-BUS-Weekday-01"]}
        ),
        properties=[{"property": "routing", "set": [75318]}],
    )

    # Shapes
    """result = transit_net.feed.shapes[
        transit_net.feed.shapes["shape_id"] == "210005"
    ]["shape_model_node_id"].tolist()
    answer = ["1"]
    assert result == answer"""

    # Stops
    result = transit_net.feed.stop_times[
        transit_net.feed.stop_times["trip_id"] == "14986385-JUN19-MVS-BUS-Weekday-01"
    ]["stop_id"].tolist()
    answer = ["2609"]  # first matching stop_id in stops.txt
    assert result == answer

    print("--Finished:", request.node.name)


@pytest.mark.roadway
@pytest.mark.transit
@pytest.mark.travis
# @pytest.mark.skip("need to update trips and nodes")
def test_select_transit_features_by_nodes(request):
    print("\n--Starting:", request.node.name)

    transit_net = TransitNetwork.read(STPAUL_DIR)

    # Any nodes
    trip_ids = transit_net.select_transit_features_by_nodes(
        node_ids=["75520", "66380", "57530"]
    )
    print(trip_ids)
    assert set(trip_ids) == set(
        [
            "14941148-JUN19-MVS-BUS-Weekday-01",
            "14941151-JUN19-MVS-BUS-Weekday-01",
            "14941153-JUN19-MVS-BUS-Weekday-01",
            "14941163-JUN19-MVS-BUS-Weekday-01",
            "14944379-JUN19-MVS-BUS-Weekday-01",
            "14944386-JUN19-MVS-BUS-Weekday-01",
            "14944413-JUN19-MVS-BUS-Weekday-01",
            "14944416-JUN19-MVS-BUS-Weekday-01",
        ]
    )

    # All nodes
    trip_ids = transit_net.select_transit_features_by_nodes(
        node_ids=["75520", "66380"], require_all=True
    )
    assert set(trip_ids) == set(
        [
            "14941148-JUN19-MVS-BUS-Weekday-01",
            "14941151-JUN19-MVS-BUS-Weekday-01",
            "14941153-JUN19-MVS-BUS-Weekday-01",
            "14941163-JUN19-MVS-BUS-Weekday-01",
        ]
    )

    print("--Finished:", request.node.name)


@pytest.mark.roadway
@pytest.mark.transit
@pytest.mark.travis
# @pytest.mark.skip("need to update trips and nodes")
def test_select_transit_features_by_nodes(request):
    print("\n--Starting:", request.node.name)

    transit_net = TransitNetwork.read(STPAUL_DIR)

    # Any nodes
    trip_ids = transit_net.select_transit_features_by_nodes(
        node_ids=["75520", "66380", "57530"]
    )
    print(trip_ids)
    assert set(trip_ids) == set(
        [
            "14941148-JUN19-MVS-BUS-Weekday-01",
            "14941151-JUN19-MVS-BUS-Weekday-01",
            "14941153-JUN19-MVS-BUS-Weekday-01",
            "14941163-JUN19-MVS-BUS-Weekday-01",
            "14944379-JUN19-MVS-BUS-Weekday-01",
            "14944386-JUN19-MVS-BUS-Weekday-01",
            "14944413-JUN19-MVS-BUS-Weekday-01",
            "14944416-JUN19-MVS-BUS-Weekday-01",
        ]
    )

    # All nodes
    trip_ids = transit_net.select_transit_features_by_nodes(
        node_ids=["75520", "66380"], require_all=True
    )
    assert set(trip_ids) == set(
        [
            "14941148-JUN19-MVS-BUS-Weekday-01",
            "14941151-JUN19-MVS-BUS-Weekday-01",
            "14941153-JUN19-MVS-BUS-Weekday-01",
            "14941163-JUN19-MVS-BUS-Weekday-01",
        ]
    )

    print("--Finished:", request.node.name)

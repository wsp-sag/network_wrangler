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


@pytest.mark.transit_with_roadnet
@pytest.mark.travis
def test_set_roadnet(request):
    print("\n--Starting:", request.node.name)

    road_net = RoadwayNetwork.read(
        link_file=os.path.join(STPAUL_DIR, "link.json"),
        node_file=os.path.join(STPAUL_DIR, "node.geojson"),
        shape_file=os.path.join(STPAUL_DIR, "shape.geojson"),
        fast=True
    )
    transit_net = TransitNetwork.read(STPAUL_DIR)
    transit_net.set_roadnet(road_net)

    print("--Finished:", request.node.name)


@pytest.mark.transit_with_roadnet
@pytest.mark.travis
#@pytest.mark.skip("need to update transit routing project card with new network")
def test_project_card(request):
    print("\n--Starting:", request.node.name)

    transit_net = TransitNetwork.read(STPAUL_DIR)
    project_card_path = os.path.join(
        STPAUL_DIR, "project_cards", "12_transit_shape_change.yml"
    )
    project_card = ProjectCard.read(project_card_path)
    transit_net.apply_transit_feature_change(
        transit_net.select_transit_features(project_card.facility),
        project_card.properties
    )

    # Shapes
    result = transit_net.feed.shapes[transit_net.feed.shapes["shape_id"] ==
                                     "2940002"]["shape_model_node_id"].tolist()
    answer = ["37582", "37574", "4761", "4763", "4764", "98429", "45985", "57483", "126324",
    "57484", "150855", "11188", "84899", "46666", "46665", "46663", "81820", "76167", "77077",
    "68609", "39425", "62146", "41991", "70841", "45691", "69793", "45683", "45685", "7688",
    "45687", "100784", "100782", "45688", "37609", "19077", "38696"]
    assert result == answer

    # Stops
    result = transit_net.feed.stop_times[transit_net.feed.stop_times["trip_id"] ==
                                         "14944022-JUN19-MVS-BUS-Weekday-01"]["stop_id"].tolist()
    result_tail = result[-5:]
    answer_tail = ["17013", "17010", "17009", "17006", "17005"]
    assert result_tail == answer_tail

    print("--Finished:", request.node.name)


@pytest.mark.transit_with_roadnet
@pytest.mark.travis
def test_shape_used_by_another_nonselected_trip_and_new_stop(request):
    print("\n--Starting:", request.node.name)

    road_net = RoadwayNetwork.read(
        link_file=os.path.join(STPAUL_DIR, "link.json"),
        node_file=os.path.join(STPAUL_DIR, "node.geojson"),
        shape_file=os.path.join(STPAUL_DIR, "shape.geojson"),
        fast=True
    )
    transit_net = TransitNetwork.read(STPAUL_DIR)
    transit_net.set_roadnet(road_net)

    # Setup test with a trip_id that shares its shape_id with another trip not
    # in the selection *and* a new stop that does not already exist in stops.txt
    test_trip_id = "14941643-JUN19-MVS-BUS-Weekday-01"
    test_shape_id = 740006
    test_model_node_id = 353828  # does not exist in stops.txt

    new_stop_id = str(test_model_node_id + TransitNetwork.ID_SCALAR)
    new_shape_id = str(test_shape_id + TransitNetwork.ID_SCALAR)

    transit_net.apply_transit_feature_change(
        trip_ids=transit_net.select_transit_features(
            {"trip_id": [test_trip_id]}
        ),
        properties=[
            {
                "property": "routing",
                "set": [test_model_node_id]
            }
        ]
    )

    # Shapes
    result = transit_net.feed.shapes[
        transit_net.feed.shapes["shape_id"] == new_shape_id
    ]["shape_model_node_id"].tolist()
    answer = [str(test_model_node_id)]
    assert result == answer

    # Stops
    result = transit_net.feed.stop_times[
        transit_net.feed.stop_times["trip_id"] == test_trip_id
    ]["stop_id"].tolist()
    answer = [new_stop_id]
    assert result == answer

    print("--Finished:", request.node.name)


@pytest.mark.transit_with_roadnet
@pytest.mark.travis
@pytest.mark.skip("need to update trips and nodes")
def test_select_transit_features_by_nodes(request):
    print("\n--Starting:", request.node.name)

    transit_net = TransitNetwork.read(STPAUL_DIR)

    # Any nodes
    trip_ids = transit_net.select_transit_features_by_nodes(
        node_ids=["29636", "29666"]
    )
    assert set(trip_ids) == set([
        "14940701-JUN19-MVS-BUS-Weekday-01",
        "14942968-JUN19-MVS-BUS-Weekday-01"
    ])

    # All nodes
    trip_ids = transit_net.select_transit_features_by_nodes(
        node_ids=["29636", "29666"], require_all=True
    )
    assert set(trip_ids) == set([
        "14940701-JUN19-MVS-BUS-Weekday-01"
    ])

    print("--Finished:", request.node.name)


@pytest.mark.transit_with_roadnet
@pytest.mark.travis
@pytest.mark.skip("need to update trips and nodes")
def test_select_transit_features_by_nodes(request):
    print("\n--Starting:", request.node.name)

    transit_net = TransitNetwork.read(STPAUL_DIR)

    # Any nodes
    trip_ids = transit_net.select_transit_features_by_nodes(
        node_ids=["29636", "29666"]
    )
    assert set(trip_ids) == set([
        "14940701-JUN19-MVS-BUS-Weekday-01",
        "14942968-JUN19-MVS-BUS-Weekday-01"
    ])

    # All nodes
    trip_ids = transit_net.select_transit_features_by_nodes(
        node_ids=["29636", "29666"], require_all=True
    )
    assert set(trip_ids) == set([
        "14940701-JUN19-MVS-BUS-Weekday-01"
    ])

    print("--Finished:", request.node.name)

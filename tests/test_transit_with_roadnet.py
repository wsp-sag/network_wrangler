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
@pytest.mark.skip("need to update transit routing project card with new network")
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
    answer = ["45983", "126312", "126316", "46663", "46665", "150855", "11188",
              "84899", "46666", "77077", "68609", "39425", "62146", "41991", "70841",
              "45691", "69793", "45683", "45685", "7688", "45687", "100784", "100782",
              "45688", "37609", "19077", "38696", "91685", "38698", "138152", "91825",
              "71086", "133190", "133187", "133188", "133183", "133179", "133177",
              "44298", "67125", "68417", "72311", "75802", "46083", "75783", "71964",
              "71456", "44190", "61464", "75786", "74898", "73817", "51814", "75787",
              "75122", "75788", "123002", "123003", "39803", "75789", "70300", "68072",
              "68039", "68041", "68043", "68044", "40878", "32484", "32474", "32485",
              "32499", "32532", "32544", "32479", "32491", "32502", "10658", "10655",
              "10651", "10652", "10649", "111619", "10643", "10642", "10640", "74056",
              "70553", "12145", "12147", "56079", "12142", "79492", "76595", "51578",
              "74179", "82208", "41799", "123353", "123351", "51651", "67557", "12136",
              "12135",  "67419", "56842", "80494", "128663", "52195", "12137", "12146",
              "25816", "25809", "115180", "9910", "115159", "115154", "162573", "10409",
              "164501", "170453", "158052", "145473", "25658",  "91126", "90985", "90993",
              "145453", "112931", "157281", "163689",  "163713", "117979", "116895",
              "138571", "87210", "97813", "165113",  "133948", "101458", "105285",
              "100603", "165025", "127797", "7753",  "127801", "28889", "28890", "127804",
              "7754", "127802", "48699",  "125802", "125237", "96386", "96366", "96367",
              "145532", "94231", "6578", "7278", "9831", "10910", "10242", "112268",
              "110085", "114928", "114941", "11694", "11654", "11705", "154299", "11708",
              "11710",  "11704", "11674", "11668", "11677", "9483", "161536", "88542",
              "22645", "19792", "126666", "170579", "145513", "155886", "101864",
              "100562",  "51955", "23270", "23263", "23271", "139930"]
    assert result == answer

    # Stops
    result = transit_net.feed.stop_times[transit_net.feed.stop_times["trip_id"] ==
                                         "14944022-JUN19-MVS-BUS-Weekday-01"]["stop_id"].tolist()
    result_tail = result[-5:]
    answer_tail = ["16842", "16837", "16836", "16833", "17040"]
    assert result_tail == answer_tail

    print("--Finished:", request.node.name)


@pytest.mark.transit_with_roadnet
@pytest.mark.travis
@pytest.mark.skip("need to allow for creating new stops if they don't already exist in stops.txt")
def test_wo_existing(request):
    print("\n--Starting:", request.node.name)

    transit_net = TransitNetwork.read(STPAUL_DIR)

    # A new node ID (not in stops.txt) should fail right now
    with pytest.raises(Exception):
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
        )

    transit_net.apply_transit_feature_change(
        trip_ids=transit_net.select_transit_features(
            {"trip_id": ["14986385-JUN19-MVS-BUS-Weekday-01"]}
        ),
        properties=[
            {
                "property": "routing",
                "set": [75318]
            }
        ]
    )

    # Shapes
    result = transit_net.feed.shapes[
        transit_net.feed.shapes["shape_id"] == "210005"
    ]["shape_model_node_id"].tolist()
    answer = ["1"]
    assert result == answer

    # Stops
    result = transit_net.feed.stop_times[
        transit_net.feed.stop_times["trip_id"] == "14986385-JUN19-MVS-BUS-Weekday-01"
    ]["stop_id"].tolist()
    answer = ["2609"]  # first matching stop_id in stops.txt
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

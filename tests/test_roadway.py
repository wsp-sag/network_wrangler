import os
import json
from geopandas import GeoDataFrame
import pytest
from network_wrangler import RoadwayNetwork
from network_wrangler import ProjectCard
import time
import numpy as np
import pandas as pd
from network_wrangler import offset_lat_lon
from network_wrangler import haversine_distance
import networkx as nx

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 50000)

"""
Run just the tests labeled basic using `pytest -m roadway`
To run with print statments, use `pytest -s -m roadway`
"""

STPAUL_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "examples", "stpaul"
)
STPAUL_SHAPE_FILE = os.path.join(STPAUL_DIR, "shape.geojson")
STPAUL_LINK_FILE = os.path.join(STPAUL_DIR, "link.json")
STPAUL_NODE_FILE = os.path.join(STPAUL_DIR, "node.geojson")

SMALL_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "examples", "single"
)
SMALL_SHAPE_FILE = os.path.join(SMALL_DIR, "shape.geojson")
SMALL_LINK_FILE = os.path.join(SMALL_DIR, "link.json")
SMALL_NODE_FILE = os.path.join(SMALL_DIR, "node.geojson")


def _read_stpaul_net():
    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    return net


SCRATCH_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "scratch"
)


def _read_stpaul_net():
    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    return net


@pytest.mark.roadway
@pytest.mark.travis
def test_roadway_read_write(request):
    print("\n--Starting:", request.node.name)

    out_prefix = "t_readwrite"
    out_shape_file = os.path.join(SCRATCH_DIR, out_prefix + "_" + "shape.geojson")
    out_link_file = os.path.join(SCRATCH_DIR, out_prefix + "_" + "link.json")
    out_node_file = os.path.join(SCRATCH_DIR, out_prefix + "_" + "node.geojson")

    time0 = time.time()

    net = RoadwayNetwork.read(
        link_file=SMALL_LINK_FILE,
        node_file=SMALL_NODE_FILE,
        shape_file=SMALL_SHAPE_FILE,
        fast=True,
    )
    time1 = time.time()
    print("Writing to: {}".format(SCRATCH_DIR))
    net.write(filename=out_prefix, path=SCRATCH_DIR)
    time2 = time.time()
    net_2 = RoadwayNetwork.read(
        link_file=out_link_file, node_file=out_node_file, shape_file=out_shape_file
    )
    time3 = time.time()

    read_time1 = time1 - time0
    read_time2 = time3 - time2
    write_time = time2 - time1

    print("TIME, read (w/out valdiation, with): {},{}".format(read_time1, read_time2))
    print("TIME, write:{}".format(write_time))
    """
    # right now don't have a good way of ignoring differences in rounding
    with open(shape_file, 'r') as s1:
        og_shape = json.loads(s1.read())
        #og_shape.replace('\r', '').replace('\n', '').replace(' ','')
    with open(os.path.join('scratch','t_readwrite_shape.geojson'), 'r')  as s2:
        new_shape = json.loads(s2.read())
        #new_shape.replace('\r', '').replace('\n', '').replace(' ','')
    assert(og_shape==new_shape)
    """


@pytest.mark.roadway
@pytest.mark.travis
def test_quick_roadway_read_write(request):
    print("\n--Starting:", request.node.name)

    out_prefix = "t_readwrite"
    out_shape_file = os.path.join(SCRATCH_DIR, out_prefix + "_" + "shape.geojson")
    out_link_file = os.path.join(SCRATCH_DIR, out_prefix + "_" + "link.json")
    out_node_file = os.path.join(SCRATCH_DIR, out_prefix + "_" + "node.geojson")
    net = RoadwayNetwork.read(
        link_file=SMALL_LINK_FILE,
        node_file=SMALL_NODE_FILE,
        shape_file=SMALL_SHAPE_FILE,
        fast=True,
    )
    net.write(filename=out_prefix, path=SCRATCH_DIR)
    net_2 = RoadwayNetwork.read(
        link_file=out_link_file, node_file=out_node_file, shape_file=out_shape_file
    )
    print("--Finished:", request.node.name)


@pytest.mark.parametrize(
    "selection",
    [
        {
            "link": [{"name": ["6th", "Sixth", "sixth"]}],
            "A": {"osm_node_id": "187899923"},
            "B": {"osm_node_id": "187865924"},
            "answer": ["187899923", "187858777", "187923585", "187865924"],
        },
        {
            "link": [{"name": ["6th", "Sixth", "sixth"]}],
            "A": {"osm_node_id": "187899923"},  # start searching for segments at A
            "B": {"osm_node_id": "187942339"},
        },
        {
            "link": [{"name": ["6th", "Sixth", "sixth"]}, {"lanes": [1, 2]}],
            "A": {"osm_node_id": "187899923"},  # start searching for segments at A
            "B": {"osm_node_id": "187942339"},
        },
        {
            "link": [{"name": ["I 35E"]}],
            "A": {"osm_node_id": "961117623"},  # start searching for segments at A
            "B": {"osm_node_id": "2564047368"},
        },
        {
            "link": [
                {"name": ["6th", "Sixth", "sixth"]},
                {"model_link_id": [2846, 2918]},
                {"lanes": [1, 2]},
            ]
        },
    ],
)
@pytest.mark.roadway
@pytest.mark.travis
def test_select_roadway_features(request, selection):
    print("\n--Starting:", request.node.name)
    net = _read_stpaul_net()
    print("--->", selection)
    net.select_roadway_features(selection)

    print("building a selection key")
    sel_key = net.build_selection_key(selection)

    print("Features selected:", len(net.selections[sel_key]["selected_links"]))
    selected_link_indices = net.selections[sel_key]["selected_links"].index.tolist()
    if "answer" in selection.keys():
        print("Comparing answer")
        selected_nodes = [str(selection["A"]["osm_node_id"])] + net.links_df.loc[
            selected_link_indices, "v"
        ].tolist()
        # print("Nodes selected: ",selected_nodes)
        # print("Expected Answer: ", sel["answer"])
        assert set(selected_nodes) == set(selection["answer"])

    print("--Finished:", request.node.name)


@pytest.mark.roadway
@pytest.mark.travis
def test_select_roadway_features_from_projectcard(request):
    print("\n--Starting:", request.node.name)
    net = _read_stpaul_net()
    print("Reading project card ...")
    project_card_name = "3_multiple_roadway_attribute_change.yml"

    project_card_path = os.path.join(STPAUL_DIR, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path)

    print("Selecting roadway features ...")
    sel = project_card.facility
    selected_link_indices = net.select_roadway_features(sel)
    print("Features selected:", len(selected_link_indices))

    print("--Finished:", request.node.name)


@pytest.mark.parametrize(
    "apply_feature_change_project_card",
    [
        "1_simple_roadway_attribute_change.yml",
        "2_multiple_roadway.yml",
        "3_multiple_roadway_attribute_change.yml",
    ],
)
@pytest.mark.roadway
@pytest.mark.travis
def test_apply_roadway_feature_change(request, apply_feature_change_project_card):
    print("\n--Starting:", request.node.name)
    my_net = _read_stpaul_net()
    print("Reading project card", apply_feature_change_project_card, "...")
    project_card_path = os.path.join(
        STPAUL_DIR, "project_cards", apply_feature_change_project_card
    )
    project_card = ProjectCard.read(project_card_path)

    print("Selecting roadway features ...")
    selected_link_indices = my_net.select_roadway_features(project_card.facility)

    attributes_to_update = [p["property"] for p in project_card.properties]
    orig_links = my_net.links_df.loc[selected_link_indices, attributes_to_update]
    print("Original Links:\n", orig_links)

    my_net.apply_roadway_feature_change(
        my_net.select_roadway_features(project_card.facility), project_card.properties
    )

    rev_links = my_net.links_df.loc[selected_link_indices, attributes_to_update]
    print("Revised Links:\n", rev_links)

    print("--Finished:", request.node.name)


@pytest.mark.roadway
@pytest.mark.travis
def test_add_managed_lane(request):
    print("\n--Starting:", request.node.name)
    net = _read_stpaul_net()
    print("Reading project card ...")
    project_card_name = "5_managed_lane.yml"
    project_card_path = os.path.join(STPAUL_DIR, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path)

    print("Selecting roadway features ...")
    selected_link_indices = net.select_roadway_features(project_card.facility)

    attributes_to_update = [p["property"] for p in project_card.properties]
    orig_links = net.links_df.loc[selected_link_indices, attributes_to_update]
    print("Original Links:\n", orig_links)

    net.apply_managed_lane_feature_change(
        net.select_roadway_features(project_card.facility), project_card.properties
    )

    rev_links = net.links_df.loc[selected_link_indices, attributes_to_update]
    print("Revised Links:\n", rev_links)

    net.write(filename="test_ml", path=SCRATCH_DIR)

    print("--Finished:", request.node.name)


@pytest.mark.roadway
@pytest.mark.travis
def test_add_adhoc_field(request):
    """
    Makes sure new fields can be added in the API and be saved and read in again.
    """
    print("\n--Starting:", request.node.name)
    net = _read_stpaul_net()
    net.links_df["my_ad_hoc_field"] = 22.5

    print("Network with field...\n ", net.links_df["my_ad_hoc_field"][0:5])

    assert net.links_df["my_ad_hoc_field"][0] == 22.5


@pytest.mark.roadway
@pytest.mark.travis
def test_add_adhoc_field_from_card(request):
    """
    Makes sure new fields can be added from a project card and that
    they will be the right type.
    """
    print("\n--Starting:", request.node.name)
    net = _read_stpaul_net()
    project_card_name = "new_fields_project_card.yml"

    print("Reading project card", project_card_name, "...")
    project_card_path = os.path.join(STPAUL_DIR, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path)

    print("Selecting roadway features ...")
    selected_link_indices = net.select_roadway_features(project_card.facility)

    attributes_to_update = [p["property"] for p in project_card.properties]

    net.apply_roadway_feature_change(
        net.select_roadway_features(project_card.facility), project_card.properties
    )

    rev_links = net.links_df.loc[selected_link_indices, attributes_to_update]
    rev_types = [(a, net.links_df[a].dtypes) for a in attributes_to_update]
    # rev_types = net.links_df[[attributes_to_update]].dtypes
    print("Revised Links:\n", rev_links, "\nNew Property Types:\n", rev_types)

    assert net.links_df.loc[selected_link_indices[0], "my_ad_hoc_field_float"] == 1.1
    assert net.links_df.loc[selected_link_indices[0], "my_ad_hoc_field_integer"] == 2
    assert (
        net.links_df.loc[selected_link_indices[0], "my_ad_hoc_field_string"] == "three"
    )
    print("--Finished:", request.node.name)


@pytest.mark.roadway
@pytest.mark.travis
def test_bad_properties_statements(request):
    """
    Makes sure new fields can be added from a project card and that
    they will be the right type.
    """

    print("\n--Starting:", request.node.name)
    net = _read_stpaul_net()
    ok_properties_change = [{"property": "lanes", "change": 1}]
    bad_properties_change = [{"property": "my_random_var", "change": 1}]
    bad_properties_existing = [{"property": "my_random_var", "existing": 1}]

    with pytest.raises(ValueError):
        net.validate_properties(bad_properties_change)

    with pytest.raises(ValueError):
        net.validate_properties(ok_properties_change, require_existing_for_change=True)

    with pytest.raises(ValueError):
        net.validate_properties(bad_properties_existing, ignore_existing=False)

    print("--Finished:", request.node.name)


@pytest.mark.travis
@pytest.mark.roadway
def test_add_delete_roadway_project_card(request):
    print("\n--Starting:", request.node.name)

    print("Reading network ...")
    net = _read_stpaul_net()
    project_cards_list = [
        "10_simple_roadway_add_change.yml",
        "11_multiple_roadway_add_and_delete_change.yml",
    ]

    for card_name in project_cards_list:
        print("Applying project card - ", card_name, "...")
        project_card_path = os.path.join(STPAUL_DIR, "project_cards", card_name)
        project_card = ProjectCard.read(project_card_path, validate=False)

        orig_links_count = len(net.links_df)
        orig_nodes_count = len(net.nodes_df)

        missing_nodes = []
        missing_links = []

        project_card_dictionary = project_card.__dict__

        def _get_missing_nodes(project_card_dictionary):
            if project_card_dictionary[
                "category"
            ].lower() == "roadway deletion" and project_card_dictionary.get("nodes"):
                for key, val in project_card_dictionary["nodes"].items():
                    return [v for v in val if v not in net.nodes_df[key].tolist()]

        def _get_missing_links(project_card_dictionary):
            if project_card_dictionary[
                "category"
            ].lower() == "roadway deletion" and project_card_dictionary.get("links"):
                for key, val in project_card_dictionary["links"].items():
                    return [v for v in val if v not in net.links_df[key].tolist()]

        # count nodes that are in original network that should be deleted
        if not project_card_dictionary.get("changes"):
            m_n = _get_missing_nodes(project_card_dictionary)
            if m_n:
                missing_nodes += m_n

            m_l = _get_missing_links(project_card_dictionary)
            if m_l:
                missing_links += m_l
        else:
            for project_dictionary in project_card_dictionary["changes"]:
                m_n = _get_missing_nodes(project_dictionary)
                if m_n:
                    missing_nodes += m_n

                m_l = _get_missing_links(project_dictionary)
                if m_l:
                    missing_links += m_l

        net.apply(project_card.__dict__)

        rev_links_count = len(net.links_df)
        rev_nodes_count = len(net.nodes_df)

        def _count_add_or_delete_features(project_card_dictionary):
            _links_added = 0
            _nodes_added = 0
            _links_deleted = 0
            _nodes_deleted = 0

            if project_card_dictionary["category"].lower() == "add new roadway":
                if project_card_dictionary.get("links") is not None:
                    _links_added = len(project_card_dictionary["links"])
                if project_card_dictionary.get("nodes") is not None:
                    _nodes_added = len(project_card_dictionary["nodes"])

            if project_card_dictionary["category"].lower() == "roadway deletion":
                if project_card_dictionary.get("links") is not None:
                    print("links", project_card_dictionary["links"])
                    _links_deleted = sum(
                        len(project_card_dictionary["links"][key])
                        for key in project_card_dictionary["links"]
                    )
                if project_card_dictionary.get("nodes"):
                    print("nodes", project_card_dictionary["nodes"])
                    _nodes_deleted = sum(
                        len(project_card_dictionary["nodes"][key])
                        for key in project_card_dictionary["nodes"]
                    )
                    print("nodes_deleted:", _nodes_deleted)
                    print("NODES:", project_card_dictionary["nodes"])

            return {
                "links_added": _links_added,
                "nodes_added": _nodes_added,
                "links_deleted": _links_deleted,
                "nodes_deleted": _nodes_deleted,
            }

        links_added = 0
        links_deleted = 0
        nodes_added = 0
        nodes_deleted = 0

        if not project_card_dictionary.get("changes"):
            count_info = _count_add_or_delete_features(project_card_dictionary)
            links_added = count_info["links_added"]
            links_deleted = count_info["links_deleted"]
            nodes_added = count_info["nodes_added"]
            nodes_deleted = count_info["nodes_deleted"]
        else:
            for project_dictionary in project_card_dictionary["changes"]:
                count_info = _count_add_or_delete_features(project_dictionary)
                links_added += count_info["links_added"]
                links_deleted += count_info["links_deleted"]
                nodes_added += count_info["nodes_added"]
                nodes_deleted += count_info["nodes_deleted"]

        num_missing_nodes = len(set(missing_nodes))
        print("MISSING NODES:", num_missing_nodes)

        num_missing_links = len(set(missing_links))
        print("MISSING LINKS:", num_missing_links)

        net_links_network = rev_links_count - orig_links_count
        net_links_project_card = links_added - links_deleted + num_missing_links

        net_nodes_network = rev_nodes_count - orig_nodes_count
        net_nodes_project_card = nodes_added - nodes_deleted + num_missing_nodes
        assert net_links_network == net_links_project_card
        assert net_nodes_network == net_nodes_project_card

    print("--Finished:", request.node.name)


@pytest.mark.roadway
@pytest.mark.travis
def test_export_network_to_csv(request):
    print("\n--Starting:", request.node.name)
    net = _read_stpaul_net()
    net.links_df.to_csv(os.path.join(SCRATCH_DIR, "links_df.csv"), index=False)
    net.nodes_df.to_csv(os.path.join(SCRATCH_DIR, "nodes_df.csv"), index=False)


variable_queries = [
    {"v": "lanes", "category": None, "time_period": ["7:00", "9:00"]},
    {"v": "ML_price", "category": "sov", "time_period": ["7:00", "9:00"]},
    {"v": "ML_price", "category": ["hov3", "hov2"], "time_period": ["7:00", "9:00"]},
]


@pytest.mark.parametrize("variable_query", variable_queries)
@pytest.mark.roadway
def test_query_roadway_property_by_time_group(request, variable_query):
    print("\n--Starting:", request.node.name)
    net = _read_stpaul_net()
    print("Applying project card...")
    project_card_path = os.path.join(STPAUL_DIR, "project_cards", "5_managed_lane.yml")
    project_card = ProjectCard.read(project_card_path, validate=False)
    net.apply_managed_lane_feature_change(
        net.select_roadway_features(project_card.facility), project_card.properties
    )
    print("Querying Attribute...")
    print("QUERY:\n", variable_query)
    v_series = net.get_property_by_time_period_and_group(
        variable_query["v"],
        category=variable_query["category"],
        time_period=variable_query["time_period"],
    )
    selected_link_indices = net.select_roadway_features(project_card.facility)

    print("CALCULATED:\n", v_series.loc[selected_link_indices])
    print("ORIGINAL:\n", net.links_df.loc[selected_link_indices, variable_query["v"]])

    ## todo make test make sure the values are correct.


@pytest.mark.highway
@pytest.mark.travis
def test_write_model_net(request):
    print("\n--Starting:", request.node.name)

    print("Reading network ...")

    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    print("Reading project card ...")
    project_card_name = "5_managed_lane.yml"
    project_card_path = os.path.join(STPAUL_DIR, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path)

    net.apply(project_card.__dict__)
    net.links_df.to_csv(os.path.join(SCRATCH_DIR, "in_ml_links.csv"), index=False)
    net.nodes_df.to_csv(os.path.join(SCRATCH_DIR, "in_ml_nodes.csv"), index=False)
    net.shapes_df.to_csv(os.path.join(SCRATCH_DIR, "in_ml_shape.csv"), index=False)

    ml_net = net.create_managed_lane_network(in_place=False)
    ml_net.links_df.to_csv(os.path.join(SCRATCH_DIR, "out_ml_links.csv"), index=False)
    ml_net.nodes_df.to_csv(os.path.join(SCRATCH_DIR, "out_ml_nodes.csv"), index=False)
    ml_net.shapes_df.to_csv(os.path.join(SCRATCH_DIR, "out_ml_shape.csv"), index=False)

    print("--Finished:", request.node.name)


@pytest.mark.offset
@pytest.mark.travis
def test_lat_lon_offset(request):
    print("\n--Starting:", request.node.name)

    in_lat_lon = [-93.0903549, 44.961085]
    print(in_lat_lon)

    new_lat_lon = offset_lat_lon(in_lat_lon)
    print(new_lat_lon)

    print("--Finished:", request.node.name)


@pytest.mark.get_dist
@pytest.mark.travis
def test_get_distance_bw_lat_lon(request):
    print("\n--Starting:", request.node.name)

    start = [-93.0889873, 44.966861]
    end = [-93.08844310000001, 44.9717832]
    dist = haversine_distance(start, end)
    print(dist)

    print("--Finished:", request.node.name)


@pytest.mark.highway
@pytest.mark.travis
def test_network_connectivity(request):
    print("\n--Starting:", request.node.name)

    print("Reading network ...")

    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    print("Checking network connectivity ...")
    print("Drive Network Connected:", net.is_network_connected(mode="drive"))
    print("--Finished:", request.node.name)

@pytest.mark.highway
@pytest.mark.travis
def test_get_modal_network(request):
    print("\n--Starting:", request.node.name)

    mode = "transit"
    print("Reading network. Mode: {} ...".format(mode))

    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    _links_df, _nodes_df = RoadwayNetwork.get_modal_links_nodes(
        net.links_df, net.nodes_df, mode=mode,
    )
    mode_variable = RoadwayNetwork.MODES_TO_NETWORK_LINK_VARIABLES[mode]
    non_transit_links =_links_df[_links_df[mode_variable] != 1]
    assert non_transit_links.shape[0] == 0

@pytest.mark.highway
@pytest.mark.travis
@pytest.mark.menow
def test_network_connectivity_ignore_single_nodes(request):
    print("\n--Starting:", request.node.name)

    print("Reading network ...")

    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    print("Assessing network connectivity for walk...")
    _, disconnected_nodes = net.assess_connectivity(mode="walk", ignore_end_nodes=True)
    print("{} Disconnected Subnetworks:".format(len(disconnected_nodes)))
    print("-->\n{}".format("\n".join(list(map(str,disconnected_nodes)))))
    print("--Finished:", request.node.name)

@pytest.mark.roadway
@pytest.mark.travis
@pytest.mark.xfail(strict=True)
def test_add_roadway_links(request):
    print("\n--Starting:", request.node.name)
    net = _read_stpaul_net()

    print("Reading project card ...")
    # project_card_name = "10_simple_roadway_add_change.yml"
    project_card_name = "10a_incorrect_roadway_add_change.yml"

    project_card_path = os.path.join(STPAUL_DIR, "project_cards", project_card_name)
    project_card = ProjectCard.read(project_card_path)

    project_card_dictionary = project_card.__dict__

    net.add_new_roadway_feature_change(
        project_card_dictionary.get("links"), project_card_dictionary.get("nodes")
    )

    print("--Finished:", request.node.name)

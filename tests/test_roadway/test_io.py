"""Tests roadway input output."""

import os
import time

import pytest
from geopandas import GeoDataFrame
from shapely.geometry import Polygon

from network_wrangler import (
    WranglerLogger,
    load_roadway_from_dir,
    write_roadway,
)
from network_wrangler.models.roadway.tables import RoadLinksTable
from network_wrangler.roadway import diff_nets
from network_wrangler.roadway.io import (
    convert_roadway_file_serialization,
    id_roadway_file_paths_in_dir,
)
from network_wrangler.roadway.network import RoadwayNetwork


def test_id_roadway_file_paths_in_dir(request, tmpdir):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    # Create mock files in the temporary directory
    links_file = tmpdir / "test_links.json"
    nodes_file = tmpdir / "test_nodes.geojson"
    shapes_file = tmpdir / "test_shapes.geojson"
    links_file.write("")
    nodes_file.write("")
    shapes_file.write("")

    # Test Case 1: All files are present
    links_path, nodes_path, shapes_path = id_roadway_file_paths_in_dir(
        tmpdir, file_format="geojson"
    )
    assert links_path == links_file
    assert nodes_path == nodes_file
    assert shapes_path == shapes_file

    # Test Case 2: Links file is missing
    links_file.remove()
    with pytest.raises(FileNotFoundError):
        id_roadway_file_paths_in_dir(tmpdir, file_format="geojson")

    # Test Case 3: Nodes file is missing
    links_file.write("")
    nodes_file.remove()
    with pytest.raises(FileNotFoundError):
        id_roadway_file_paths_in_dir(tmpdir, file_format="geojson")

    # Test Case 4: Shapes file is missing (optional)
    nodes_file.write("")
    shapes_file.remove()
    links_path, nodes_path, shapes_path = id_roadway_file_paths_in_dir(
        tmpdir, file_format="geojson"
    )
    assert links_path == links_file
    assert nodes_path == nodes_file
    assert shapes_path is None


def test_convert(request, example_dir, tmpdir):
    """Test that the convert function works for both geojson and parquet.

    Also makes sure that the converted network is the same as the original when the original
    is geographically complete (it will have added information when it is not geographically
    complete).
    """
    WranglerLogger.info(f"--Starting: {request.node.name}")
    out_dir = tmpdir

    # convert EX from geojson to parquet
    convert_roadway_file_serialization(
        example_dir / "small",
        "geojson",
        out_dir,
        "parquet",
        "simple",
        True,
    )

    output_files_parq = [
        out_dir / "simple_links.parquet",
        out_dir / "simple_nodes.parquet",
    ]

    missing_parq = [i for i in output_files_parq if not i.exists()]
    if missing_parq:
        WranglerLogger.error(f"Missing {len(missing_parq)} parquet output files: {missing_parq})")
        msg = "Missing converted parquet files."
        raise FileNotFoundError(msg)

    # convert parquet to geojson
    convert_roadway_file_serialization(
        out_dir,
        "parquet",
        out_dir,
        "geojson",
        "simple",
        True,
    )

    output_files_geojson = [
        out_dir / "simple_links.json",
        out_dir / "simple_nodes.geojson",
    ]

    missing_geojson = [i for i in output_files_geojson if not i.exists()]
    if missing_geojson:
        WranglerLogger.error(
            f"Missing {len(missing_geojson)} geojson output files: {missing_geojson})"
        )
        msg = "Missing converted geojson files."
        raise FileNotFoundError(msg)

    WranglerLogger.debug("Reading in og network to test that it is equal.")
    in_net = load_roadway_from_dir(example_dir / "small", file_format="geojson")

    WranglerLogger.debug("Reading in converted network to test that it is equal.")
    out_net_parq = load_roadway_from_dir(out_dir, file_format="parquet")
    out_net_geojson = load_roadway_from_dir(out_dir, file_format="geojson")

    WranglerLogger.info("Evaluating original vs parquet network.")
    assert not diff_nets(in_net, out_net_parq), "The original and parquet networks differ."
    WranglerLogger.info("Evaluating parquet vs geojson network.")
    assert not diff_nets(out_net_parq, out_net_geojson), "The parquet and geojson networks differ."


def test_roadway_model_coerce(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    assert isinstance(small_net, RoadwayNetwork)
    WranglerLogger.debug(f"small_net.nodes_df.cols: \n{small_net.nodes_df.columns}")
    assert "osm_node_id" in small_net.nodes_df.columns
    WranglerLogger.debug(f"small_net.links_df.cols: \n{small_net.links_df.columns}")
    assert "osm_link_id" in small_net.links_df.columns


@pytest.mark.parametrize("io_format", ["geojson", "parquet"])
@pytest.mark.parametrize("ex", ["stpaul", "small"])
def test_roadway_geojson_read_write_read(request, example_dir, test_out_dir, ex, io_format):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    read_dir = example_dir / ex
    net = load_roadway_from_dir(read_dir)
    test_io_dir = test_out_dir / ex
    t_0 = time.time()
    write_roadway(net, file_format=io_format, out_dir=test_io_dir, overwrite=True)
    t_write = time.time() - t_0
    WranglerLogger.info(
        f"{int(t_write // 60): 02d}:{int(t_write % 60): 02d} ... {ex} write to {io_format}"
    )
    t_0 = time.time()
    net = load_roadway_from_dir(test_io_dir, file_format=io_format)
    t_read = time.time() - t_0
    WranglerLogger.info(
        f"{int(t_read // 60): 02d}:{int(t_read % 60): 02d} ... {ex} read from {io_format}"
    )
    assert isinstance(net, RoadwayNetwork)
    # make sure field order is as expected.
    skip_ordered = ["geometry"]
    _shared_ordered_fields = [
        c for c in RoadLinksTable.__fields__ if c in net.links_df.columns and c not in skip_ordered
    ]
    _output_cols = [c for c in net.links_df.columns if c not in skip_ordered][
        0 : len(_shared_ordered_fields)
    ]
    assert _output_cols == _shared_ordered_fields


def test_load_roadway_no_shapes(request, example_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    # Test Case 2: Without Shapes File
    roadway_network = load_roadway_from_dir(example_dir / "small")
    assert isinstance(roadway_network, RoadwayNetwork)
    assert not roadway_network.links_df.empty
    assert not roadway_network.nodes_df.empty
    assert roadway_network._shapes_df is None


def test_load_roadway_within_boundary(request, example_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    one_block = Polygon(
        [
            [-93.09424891687992, 44.950667556032386],
            [-93.09318302493314, 44.949458919295751],
            [-93.09110424119152, 44.950413327659845],
            [-93.09238213374682, 44.951563597873246],
            [-93.09424891687992, 44.950667556032386],
        ]
    )
    boundary_gdf = GeoDataFrame({"geometry": [one_block]}, crs="EPSG:4326")
    roadway_network = load_roadway_from_dir(example_dir / "small", boundary_gdf=boundary_gdf)

    assert isinstance(roadway_network, RoadwayNetwork)

    expected_node_ids = [2, 3, 6, 7]
    assert set(roadway_network.nodes_df.index) == set(expected_node_ids)
    assert set(roadway_network.links_df["A"]).issubset(set(expected_node_ids))
    assert set(roadway_network.links_df["B"]).issubset(set(expected_node_ids))

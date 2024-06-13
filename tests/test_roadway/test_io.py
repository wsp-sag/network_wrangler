"""Tests roadway input output."""

import time

import pytest

from network_wrangler import (
    write_roadway,
    load_roadway_from_dir,
    WranglerLogger,
)
from network_wrangler.roadway import diff_nets
from network_wrangler.roadway.io import convert_roadway_file_serialization
from network_wrangler.roadway.network import RoadwayNetwork


def test_convert(example_dir, tmpdir):
    """Test that the convert function works for both geojson and parquet.

    Also makes sure that the converted network is the same as the original when the original
    is geographically complete (it will have added information when it is not geographically
    complete).
    """
    out_dir = tmpdir

    # convert EX from geojson to parquet
    convert_roadway_file_serialization(
        example_dir / "small",
        "parquet",
        out_dir,
        "geojson",
        "simple",
        True,
    )

    output_files_parq = [
        out_dir / "simple_link.parquet",
        out_dir / "simple_node.parquet",
    ]

    missing_parq = [i for i in output_files_parq if not i.exists()]
    if missing_parq:
        WranglerLogger.error(f"Missing {len(missing_parq)} parquet output files: {missing_parq})")
        raise FileNotFoundError("Missing converted parquet files.")

    # convert parquet to geojson
    convert_roadway_file_serialization(
        out_dir,
        "geojson",
        out_dir,
        "parquet",
        "simple",
        True,
    )

    output_files_geojson = [
        out_dir / "simple_link.json",
        out_dir / "simple_node.geojson",
    ]

    missing_geojson = [i for i in output_files_geojson if not i.exists()]
    if missing_geojson:
        WranglerLogger.error(
            f"Missing {len(missing_geojson)} geojson output files: {missing_geojson})"
        )
        raise FileNotFoundError("Missing converted geojson files.")

    WranglerLogger.debug("Reading in og network to test that it is equal.")
    in_net = load_roadway_from_dir(example_dir / "small", suffix="geojson")

    WranglerLogger.debug("Reading in converted network to test that it is equal.")
    out_net_parq = load_roadway_from_dir(out_dir, suffix="parquet")
    out_net_geojson = load_roadway_from_dir(out_dir, suffix="geojson")

    WranglerLogger.info("Evaluating original vs parquet network.")
    assert not diff_nets(in_net, out_net_parq), "The original and parquet networks differ."
    WranglerLogger.info("Evaluating parquet vs geojson network.")
    assert not diff_nets(out_net_parq, out_net_geojson), "The parquet and geojson networks differ."


def test_roadway_model_coerce(small_net):
    assert isinstance(small_net, RoadwayNetwork)
    WranglerLogger.debug(f"small_net.nodes_df.cols: \n{small_net.nodes_df.columns}")
    assert "osm_node_id" in small_net.nodes_df.columns
    WranglerLogger.debug(f"small_net.links_df.cols: \n{small_net.links_df.columns}")
    assert "osm_link_id" in small_net.links_df.columns


@pytest.mark.parametrize("io_format", ["geojson", "parquet"])
@pytest.mark.parametrize("ex", ["stpaul", "small"])
def test_roadway_geojson_read_write_read(example_dir, test_out_dir, ex, io_format):
    read_dir = example_dir / ex
    net = load_roadway_from_dir(read_dir)
    test_io_dir = test_out_dir / ex
    t_0 = time.time()
    write_roadway(net, file_format=io_format, out_dir=test_io_dir, overwrite=True)
    t_write = time.time() - t_0
    WranglerLogger.info(
        f"{int(t_write // 60): 02d}:{int(t_write % 60): 02d} – {ex} write to {io_format}"  # noqa: E231, E501
    )
    t_0 = time.time()
    net = load_roadway_from_dir(test_io_dir, suffix=io_format)
    t_read = time.time() - t_0
    WranglerLogger.info(
        f"{int(t_read // 60): 02d}:{int(t_read % 60): 02d} – {ex} read from {io_format}"  # noqa: E231, E501
    )
    assert isinstance(net, RoadwayNetwork)

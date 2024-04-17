"""
Run just the tests labeled basic using `pytest tests/test_roadway/test_io.py`
To run with print statments, use `pytest -s tests/test_roadway/test_io.py`
"""

import time

import pytest
from pathlib import Path

from network_wrangler import (
    load_roadway,
    write_roadway,
    load_roadway_from_dir,
    WranglerLogger,
)
from network_wrangler.roadway import diff_nets, convert_roadway
from network_wrangler.roadway.network import RoadwayNetwork


@pytest.mark.xfail
def test_convert(example_dir, test_dir):
    """
    Test that the convert function works for both geojson and parquet.

    Also makes sure that the converted network is the same as the original when the original
    is geographically complete (it will have added information when it is not geographically complete).
    """
    out_dir = test_dir / "out"

    # convert EX from geojson to parquet
    convert_roadway(
        example_dir / "small",
        "parquet",
        out_dir,
        "geojson",
        "simple",
        True,
    )

    # convert parquet to geojson
    convert_roadway(
        out_dir,
        "geojson",
        out_dir,
        "parquet",
        "simple",
        True,
    )

    output_files_parq = [
        out_dir / "simple_link.parquet",
        out_dir / "simple_node.parquet",
        out_dir / "simple_shape.parquet",
    ]

    output_files_geojson = [
        out_dir / "simple_link.json",
        out_dir / "simple_node.geojson",
        out_dir / "simple_shape.geojson",
    ]

    for f in output_files_parq + output_files_geojson:
        if not f.exists():
            raise FileNotFoundError(f"File {f} was not created")

    WranglerLogger.debug("Reading in og network to test that it is equal.")
    in_net = load_roadway_from_dir(example_dir / "small", suffix="geojson")

    WranglerLogger.debug("Reading in converted network to test that it is equal.")
    out_net_parq = load_roadway_from_dir(out_dir, suffix="parquet")
    out_net_geojson = load_roadway_from_dir(out_dir, suffix="geojson")

    WranglerLogger.info("Evaluating original vs parquet network.")
    assert not diff_nets(
        in_net, out_net_parq
    ), "The original and parquet networks differ."
    WranglerLogger.info("Evaluating parquet vs geojson network.")
    assert not diff_nets(
        out_net_parq, out_net_geojson
    ), "The parquet and geojson networks differ."


@pytest.mark.parametrize("write_format", ["parquet", "geojson"])
@pytest.mark.parametrize("ex", ["stpaul", "small"])
def test_roadway_write(stpaul_net, small_net, test_out_dir, write_format, ex):
    if ex == "stpaul":
        net = stpaul_net
    else:
        net = small_net
    write_dir = test_out_dir / ex
    t_0 = time.time()
    write_roadway(net, file_format=write_format, out_dir=write_dir, overwrite=True)
    t_write = time.time() - t_0
    WranglerLogger.info(
        f"{int(t_write // 60):02d}:{int(t_write % 60):02d} – {ex} write to {write_format}"
    )


@pytest.mark.parametrize("read_format", ["geojson", "parquet"])
@pytest.mark.parametrize("ex", ["stpaul", "small"])
def test_roadway_read(example_dir, test_out_dir, read_format, ex):
    read_dir = example_dir / ex
    if read_format in ["parquet"]:
        read_dir = test_out_dir / ex

    t_0 = time.time()
    net = load_roadway_from_dir(read_dir, suffix=read_format)
    t_read = time.time() - t_0
    WranglerLogger.info(
        f"{int(t_read // 60):02d}:{int(t_read % 60):02d} – {ex} read from {read_format}"
    )
    assert isinstance(net, RoadwayNetwork)


def test_quick_roadway_read_write(request, scratch_dir, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    out_prefix = "t_readwrite"
    out_shape_file = Path(scratch_dir) / (out_prefix + "_" + "shape.geojson")
    out_link_file = Path(scratch_dir) / (out_prefix + "_" + "link.json")
    out_node_file = Path(scratch_dir) / (out_prefix + "_" + "node.geojson")
    write_roadway(small_net, prefix=out_prefix, out_dir=scratch_dir)
    _ = load_roadway(
        links_file=out_link_file, nodes_file=out_node_file, shapes_file=out_shape_file
    )
    WranglerLogger.info(f"--Finished: {request.node.name}")

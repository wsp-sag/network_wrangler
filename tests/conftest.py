import os

import pandas as pd
import pytest


pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 50000)

@pytest.fixture(scope="session",autouse=True)
def test_logging(test_out_dir):
    from network_wrangler import setup_logging
    setup_logging(
        info_log_filename=os.path.join(test_out_dir,"tests.info.log"),
        debug_log_filename=os.path.join(test_out_dir,"tests.debug.log"),
    )

@pytest.fixture(scope="session")
def base_dir():
    return os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

@pytest.fixture(scope="session")
def example_dir(base_dir):
    return os.path.join(base_dir, "examples")

@pytest.fixture(scope="session")
def test_dir():
    return os.path.dirname(os.path.realpath(__file__))

@pytest.fixture(scope="session")
def test_out_dir(test_dir):
    _test_out_dir = os.path.join(test_dir,"out")
    if not os.path.exists: os.mkdir(_test_out_dir)
    return _test_out_dir

@pytest.fixture(scope="session")
def stpaul_ex_dir(example_dir):
    return os.path.join(example_dir, "stpaul")


@pytest.fixture(scope="session")
def small_ex_dir(example_dir):
    return os.path.join(example_dir, "single")


@pytest.fixture(scope="session")
def scratch_dir(base_dir):
    return os.path.join(base_dir, "scratch")


@pytest.fixture(scope="module")
def stpaul_net(stpaul_ex_dir):
    from network_wrangler import RoadwayNetwork

    shape_filename = os.path.join(stpaul_ex_dir, "shape.geojson")
    link_filename = os.path.join(stpaul_ex_dir, "link.json")
    node_filename = os.path.join(stpaul_ex_dir, "node.geojson")

    net = RoadwayNetwork.read(
        link_file=link_filename,
        node_file=node_filename,
        shape_file=shape_filename,
    )
    return net


@pytest.fixture(scope="module")
def small_net(small_ex_dir):
    from network_wrangler import RoadwayNetwork

    shape_filename = os.path.join(small_ex_dir, "shape.geojson")
    link_filename = os.path.join(small_ex_dir, "link.json")
    node_filename = os.path.join(small_ex_dir, "node.geojson")

    net = RoadwayNetwork.read(
        link_file=link_filename,
        node_file=node_filename,
        shape_file=shape_filename,
        fast=True,
    )
    return net

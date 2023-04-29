import copy
import os

import pandas as pd
import pytest


pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 50000)


@pytest.fixture(scope="session", autouse=True)
def test_logging(test_out_dir):
    from network_wrangler import setup_logging

    setup_logging(
        info_log_filename=os.path.join(test_out_dir, "tests.info.log"),
        debug_log_filename=os.path.join(test_out_dir, "tests.debug.log"),
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
    _test_out_dir = os.path.join(test_dir, "out")
    if not os.path.exists(_test_out_dir):
        os.mkdir(_test_out_dir)
    return _test_out_dir


@pytest.fixture
def stpaul_base_scenario(stpaul_ex_dir, stpaul_net, stpaul_transit_net):
    base_scenario = {
        "road_net": copy.deepcopy(stpaul_net),
        "transit_net": copy.deepcopy(stpaul_transit_net),
    }
    return base_scenario


@pytest.fixture(scope="session")
def stpaul_card_dir(stpaul_ex_dir):
    return os.path.join(stpaul_ex_dir, "project_cards")


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
        links_file=link_filename,
        nodes_file=node_filename,
        shapes_file=shape_filename,
    )
    return net


@pytest.fixture(scope="module")
def stpaul_transit_net(stpaul_ex_dir):
    from network_wrangler import TransitNetwork

    return TransitNetwork.read(stpaul_ex_dir)


@pytest.fixture(scope="module")
def small_net(small_ex_dir):
    from network_wrangler import RoadwayNetwork

    shape_filename = os.path.join(small_ex_dir, "shape.geojson")
    link_filename = os.path.join(small_ex_dir, "link.json")
    node_filename = os.path.join(small_ex_dir, "node.geojson")

    net = RoadwayNetwork.read(
        links_file=link_filename,
        nodes_file=node_filename,
        shapes_file=shape_filename,
    )
    return net

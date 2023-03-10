import os

import pytest


@pytest.fixture(scope="module")
def base_dir():
    return os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


@pytest.fixture(scope="module")
def example_dir(base_dir):
    return os.path.join(base_dir, "examples")


@pytest.fixture(scope="module")
def stpaul_ex_dir(example_dir):
    return os.path.join(example_dir, "stpaul")


@pytest.fixture(scope="module")
def small_ex_dir(example_dir):
    return os.path.join(example_dir, "single")


@pytest.fixture(scope="module")
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

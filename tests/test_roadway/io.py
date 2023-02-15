import os
import time

import pytest

import pandas as pd

from network_wrangler import RoadwayNetwork
from network_wrangler import WranglerLogger

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

SCRATCH_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "scratch"
)


def _read_small_net():
    net = RoadwayNetwork.read(
        link_file=SMALL_LINK_FILE,
        node_file=SMALL_NODE_FILE,
        shape_file=SMALL_SHAPE_FILE,
        fast=True,
    )
    return net


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


@pytest.mark.roadway
@pytest.mark.travis
def test_export_network_to_csv(request):
    print("\n--Starting:", request.node.name)
    net = _read_stpaul_net()
    net.links_df.to_csv(os.path.join(SCRATCH_DIR, "links_df.csv"), index=False)
    net.nodes_df.to_csv(os.path.join(SCRATCH_DIR, "nodes_df.csv"), index=False)

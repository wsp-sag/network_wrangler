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
Run just the tests labeled basic using `pytest tests/test_roadway/test_io.py`
To run with print statments, use `pytest -s tests/test_roadway/test_io.py`
"""

def test_roadway_read_write(request, small_net, scratch_dir):
    print("\n--Starting:", request.node.name)

    out_prefix = "t_readwrite"
    out_shape_file = os.path.join(scratch_dir, out_prefix + "_" + "shape.geojson")
    out_link_file = os.path.join(scratch_dir, out_prefix + "_" + "link.json")
    out_node_file = os.path.join(scratch_dir, out_prefix + "_" + "node.geojson")

    time0 = time.time()

    net = small_net
    time1 = time.time()
    net.write(filename=out_prefix, path=scratch_dir)
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

def test_quick_roadway_read_write(request, scratch_dir, small_net):
    print("\n--Starting:", request.node.name)

    out_prefix = "t_readwrite"
    out_shape_file = os.path.join(scratch_dir, out_prefix + "_" + "shape.geojson")
    out_link_file = os.path.join(scratch_dir, out_prefix + "_" + "link.json")
    out_node_file = os.path.join(scratch_dir, out_prefix + "_" + "node.geojson")
    net = small_net
    net.write(filename=out_prefix, path=scratch_dir)
    net_2 = RoadwayNetwork.read(
        link_file=out_link_file, node_file=out_node_file, shape_file=out_shape_file
    )
    print("--Finished:", request.node.name)


def test_export_network_to_csv(request, small_net, scratch_dir):
    print("\n--Starting:", request.node.name)
    net = small_net
    net.links_df.to_csv(os.path.join(scratch_dir, "links_df.csv"), index=False)
    net.nodes_df.to_csv(os.path.join(scratch_dir, "nodes_df.csv"), index=False)

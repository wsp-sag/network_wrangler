import os
import pytest

from network_wrangler import RoadwayNetwork

import copy

EX_DIR = r"/Users/elizabeth/Downloads/standard_roadway_pre_base_project_cards"
EX_DIR = r"/Users/elizabeth/Documents/urbanlabs/MetCouncil/working/network_wrangler/examples/stpaul"

LINKS = os.path.join(EX_DIR, "link.json")
NODES = os.path.join(EX_DIR, "node.geojson")
SHAPES = os.path.join(EX_DIR, "shape.geojson")


def read_ex_net(benchmark):
    benchmark(
        RoadwayNetwork.read(
            link_file=LINKS,
            node_file=NODES,
            shape_file=SHAPES,
            fast=True,
        )
    )


def copy_net(net):
    return copy.deepcopy(net)


@pytest.mark.performance
def test_read_net_speed(benchmark):
    print("starting")
    net = RoadwayNetwork.read(
        link_file=LINKS,
        node_file=NODES,
        shape_file=SHAPES,
        fast=True,
    )
    print("read net")
    net2 = benchmark(copy_net(net))
    print("copied net")

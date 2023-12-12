import os
import pytest

from network_wrangler import RoadwayNetwork

import copy


def read_ex_net(benchmark, stpaul_ex_dir):
    LINKS = os.path.join(stpaul_ex_dir, "link.json")
    NODES = os.path.join(stpaul_ex_dir, "node.geojson")
    SHAPES = os.path.join(stpaul_ex_dir, "shape.geojson")
    benchmark(
        RoadwayNetwork.read(
            links_file=LINKS,
            nodes_file=NODES,
            shapes_file=SHAPES,
        )
    )


def copy_net(net):
    return copy.deepcopy(net)


@pytest.mark.failing
def test_read_net_speed(benchmark, stpaul_ex_dir):
    print("starting")
    LINKS = os.path.join(stpaul_ex_dir, "link.json")
    NODES = os.path.join(stpaul_ex_dir, "node.geojson")
    SHAPES = os.path.join(stpaul_ex_dir, "shape.geojson")
    net = RoadwayNetwork.read(
        link_file=LINKS,
        node_file=NODES,
        shape_file=SHAPES,
    )
    print("read net")
    benchmark(copy_net(net))
    print("copied net")

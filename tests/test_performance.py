import os
import pytest

from network_wrangler import load_roadway


def read_ex_net(benchmark, stpaul_ex_dir):
    LINKS = os.path.join(stpaul_ex_dir, "link.json")
    NODES = os.path.join(stpaul_ex_dir, "node.geojson")
    SHAPES = os.path.join(stpaul_ex_dir, "shape.geojson")
    benchmark(
        load_roadway(
            links_file=LINKS,
            nodes_file=NODES,
            shapes_file=SHAPES,
        )
    )

import os
import json
import pytest
from network_wrangler import RoadwayNetwork
from network_wrangler import TransitNetwork
from network_wrangler import ProjectCard


"""
Run just the tests labeled transit using `pytest -v -m transit`
"""

STPAUL_DIR = os.path.join(os.getcwd(), "example", "stpaul")
SCRATCH_DIR = os.path.join(os.getcwd(), "tests")


@pytest.mark.transit_with_graph
@pytest.mark.travis
def test_set_graph(request):
    print("\n--Starting:", request.node.name)

    road_net = RoadwayNetwork.read(
        link_file=os.path.join(STPAUL_DIR, 'link.json'),
        node_file=os.path.join(STPAUL_DIR, 'node.geojson'),
        shape_file=os.path.join(STPAUL_DIR, 'shape.geojson'),
        fast=True
    )
    transit_net = TransitNetwork.read(STPAUL_DIR)
    transit_net.set_graph(road_net)

    print("--Finished:", request.node.name)

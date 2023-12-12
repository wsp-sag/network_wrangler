import os

import pytest

import pandas as pd


from network_wrangler import RoadwayNetwork
from network_wrangler import TransitNetwork
from network_wrangler.logger import WranglerLogger

"""
Run just the tests labeled transit using `pytest -v -m transit`
"""

STPAUL_DIR = os.path.join(os.getcwd(), "examples", "stpaul")
SCRATCH_DIR = os.path.join(os.getcwd(), "scratch")


def test_set_roadnet(request, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    transit_net = TransitNetwork.read(STPAUL_DIR)
    transit_net.road_net = stpaul_net

    WranglerLogger.info(f"--Finished: {request.node.name}")

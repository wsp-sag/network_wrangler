import copy
import os
import time

import pytest

import pandas as pd

from network_wrangler import ProjectCard
from network_wrangler import RoadwayNetwork
from network_wrangler import WranglerLogger


"""
Run just the tests labeled basic using `pytest tests/test_roadway/test_properties.py
To run with print statments, use `pytest -s tests/test_roadway/test_properties.py`
"""


def test_network_connectivity(request, small_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    MODE = 'drive'
    net = copy.deepcopy(small_net)
    connected = net.is_network_connected(mode=MODE)
   
    WranglerLogger.info(f"{MODE} Network Connected:{connected}")
    assert connected
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_network_connectivity_ignore_single_nodes(request, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    MODE = 'walk'
    net = copy.deepcopy(stpaul_net)

    WranglerLogger.debug(f"Assessing network connectivity for {MODE}.")

    _, disconnected_nodes = net.assess_connectivity(mode= MODE, ignore_end_nodes=True)
    _nl = '\n'
    WranglerLogger.debug(f"{len(disconnected_nodes)} Disconnected Subnetworks:")
    WranglerLogger.debug(f"-->\n{_nl.join(list(map(str, disconnected_nodes)))}")
    WranglerLogger.info(f"--Finished: {request.node.name}")

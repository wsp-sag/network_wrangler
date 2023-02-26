import os
import time

import pytest

import pandas as pd

from network_wrangler import ProjectCard
from network_wrangler import RoadwayNetwork
from network_wrangler import WranglerLogger

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 50000)

"""
Run just the tests labeled basic using `pytest tests/test_roadway/test_properties.py
To run with print statments, use `pytest -s tests/test_roadway/test_properties.py`
"""

def test_network_connectivity(request,stpaul_net):
    print("\n--Starting:", request.node.name)

    print("Reading network ...")

    net = stpaul_net
    print("Checking network connectivity ...")
    print("Drive Network Connected:", net.is_network_connected(mode="drive"))
    print("--Finished:", request.node.name)


def test_network_connectivity_ignore_single_nodes(request,stpaul_net):
    print("\n--Starting:", request.node.name)

    print("Reading network ...")

    net = stpaul_net
    
    print("Assessing network connectivity for walk...")
    _, disconnected_nodes = net.assess_connectivity(mode="walk", ignore_end_nodes=True)
    print("{} Disconnected Subnetworks:".format(len(disconnected_nodes)))
    print("-->\n{}".format("\n".join(list(map(str, disconnected_nodes)))))
    print("--Finished:", request.node.name)

import os
from copy import deepcopy
import logging
import pytest

from network_wrangler import RoadwayNetwork, ProjectCard, Scenario, TransitNetwork

###############################
#   DIRECTORIES TO REFERENCE  #
###############################

@pytest.fixture(scope="session")
def base_dir():
    return os.path.dirname(os.path.dirname(__file__))

@pytest.fixture(scope="session")
def example_dir(base_dir):
    return os.path.join(base_dir, "examples")

@pytest.fixture(scope="session")
def stpaul_dir(example_dir):
    return os.path.join(example_dir, "stpaul")

@pytest.fixture(scope="session")
def stpaul_logfiles(example_dir):
    return os.path.join(example_dir, "stpaul", "logfiles")

@pytest.fixture(scope="session")
def stpaul_project_cards(example_dir):
    return os.path.join(example_dir, "stpaul", "project_cards")

@pytest.fixture(scope="session")
def small_dir(example_dir):
    return os.path.join(example_dir, "single")

###############################
#      ROADWAY NETWORKS       #
###############################
def _get_net(dir):
    net = RoadwayNetwork.read(
        link_file=os.path.join(dir, "link.json"),
        node_file=os.path.join(dir, "node.geojson"),
        shape_file=os.path.join(dir, "shape.geojson"),
        fast=True,
    )
    return net

# CACHED

@pytest.fixture(scope="session")
def cached_small_net(small_dir):
    #logging.info("getting cached small net")
    return _get_net(small_dir)

@pytest.fixture(scope="session")
def cached_stpaul_net(stpaul_dir):
    #logging.info("getting cached stpaul net")
    return _get_net(stpaul_dir)

# Copy cached version to get a fresh copy of the network.

@pytest.fixture
def small_net(cached_small_net):
    #logging.info("copying and returning cached small net")
    if isinstance(cached_small_net,RoadwayNetwork):
        #logging.debug("returning RoadwayNetwork small net")
        return deepcopy(cached_small_net)
    #logging.debug("returning function call for cachced_small net")
    net = cached_small_net()
    return deepcopy(net)

@pytest.fixture
def stpaul_net(cached_stpaul_net):
    #logging.info("copying and returning cached stpaul net")
    if isinstance(cached_stpaul_net,RoadwayNetwork):
        #logging.debug("returning RoadwayNetwork stpaul net")
        return deepcopy(cached_stpaul_net)
    #logging.debug("returning function call for cacched_stpaul net")
    net = cached_stpaul_net()
    return deepcopy(net)

###############################
#      TRANSIT NETWORKS       #
###############################

@pytest.fixture(scope="session")
def cached_stpaul_transit(stpaul_dir):
    return TransitNetwork.read(stpaul_dir)

@pytest.fixture
def stpaul_transit(cached_stpaul_transit):
    #logging.info("copying and returning cached stpaul transit net")
    if isinstance(cached_stpaul_transit,TransitNetwork):
        #logging.debug("returning copy of stpaul transit net")
        return deepcopy(cached_stpaul_transit)
    #logging.debug("returning function call for cached stpaul transit net")
    tnet = cached_stpaul_transit()
    return deepcopy(tnet)

###############################
#           SCENARIOS         #
###############################

@pytest.fixture
def stpaul_basic_scenario(stpaul_project_cards):
    base_scenario = {}
    _card_files = [
        "a_test_project_card.yml",
        "b_test_project_card.yml",
        "c_test_project_card.yml",
    ]
    _project_cards = [
        ProjectCard.read(os.path.join(stpaul_project_cards, f)) for f in _card_files
    ]

    scen = Scenario.create_scenario(
        base_scenario=base_scenario, project_cards_list=_project_cards
    )
    return scen

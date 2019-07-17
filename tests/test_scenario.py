import os
import pytest
from network_wrangler import ProjectCard
from network_wrangler import Scenario
from network_wrangler.Logger import WranglerLogger

"""
Run just the tests labeled basic using `pytest -v -m basic`
"""

@pytest.mark.scenario
def test_project_card_read():

    in_dir  = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'example', 'stpaul','project_cards')
    in_file = os.path.join(in_dir,"1_simple_roadway_attribute_change.yml")
    project_card = ProjectCard.read(in_file)
    WranglerLogger.info(project_card.__dict__)
    assert(project_card.Category == "Roadway Attribute Change")

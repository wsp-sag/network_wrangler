import os
import pytest
from network_wrangler import ProjectCard
from network_wrangler import Scenario
from network_wrangler.Logger import WranglerLogger

"""
Run just the tests labeled scenario using `pytest -v -m scenario`
To run with print statments, use `pytest -s -m scenario`
"""

@pytest.mark.scenario
def test_project_card_read():

    in_dir  = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'example', 'stpaul','project_cards')
    in_file = os.path.join(in_dir,"1_simple_roadway_attribute_change.yml")
    project_card = ProjectCard.read(in_file)
    WranglerLogger.info(project_card.__dict__)
    assert(project_card.Category == "Roadway Attribute Change")

@pytest.mark.ashish
def test_scenario_conflicts():
    base_scenario = {}

    project_cards_list = []
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','4_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','5_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','6_test_project_card.yml')))

    scen = Scenario.create_scenario(base_scenario = base_scenario, project_cards_list = project_cards_list)
    scen.check_scenario_conflicts()

@pytest.mark.ashish
def test_scenario_corequisites():
    base_scenario = {}

    project_cards_list = []
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','4_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','5_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','6_test_project_card.yml')))

    scen = Scenario.create_scenario(base_scenario = base_scenario, project_cards_list = project_cards_list)
    scen.check_scenario_corequisites()

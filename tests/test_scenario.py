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
    print("test_project_card_read: Testing project card is read into object dictionary")
    in_dir  = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'example', 'stpaul','project_cards')
    in_file = os.path.join(in_dir,"1_simple_roadway_attribute_change.yml")
    project_card = ProjectCard.read(in_file)
    WranglerLogger.info(project_card.__dict__)
    print("---test_project_card_read()---\n",str(project_card),"\n---end test_project_card_read()---\n")
    assert(project_card.category == "Roadway Attribute Change")

@pytest.mark.ashish
@pytest.mark.scenario
def test_scenario_conflicts():

    project_cards_list = []
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','4_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','5_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','6_test_project_card.yml')))

    scen = Scenario.create_scenario(base_scenario = {}, project_cards_list = project_cards_list)

    print("---test_scenario_conflicts()---\n",str(scen),"\n")
    scen.check_scenario_conflicts()

    if scen.has_conflict_error:
        print('Conflicting project found for scenario!')

    print('Conflict checks done:', scen.conflicts_checked)
    print("---end test_scenario_conflicts()---\n")

@pytest.mark.ashish
@pytest.mark.scenario
def test_scenario_requisites():
    base_scenario = {}

    project_cards_list = []
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','4_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','5_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','6_test_project_card.yml')))

    scen = Scenario.create_scenario(base_scenario = base_scenario, project_cards_list = project_cards_list)

    print("---test_scenario_requisites()---\n",str(scen),"\n")

    scen.check_scenario_requisites()
    if scen.has_requisite_error:
        print('Missing pre- or co-requisite projects found for scenario!')

    print('Requisite checks done:', scen.requisites_checked)
    print("---end test_scenario_requisites()---\n")

@pytest.mark.scenario
def test_project_sort():
    print("---test_project sort()---")
    base_scenario = {}

    project_cards_list = []
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','4_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','5_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','6_test_project_card.yml')))

    scen = Scenario.create_scenario(base_scenario = base_scenario, project_cards_list = project_cards_list)
    print("\n--Prerequisites:")
    import pprint
    pprint.pprint(scen.prerequisites)
    print("\nUnordered Projects:",scen.project_names())
    scen.check_scenario_conflicts()
    scen.check_scenario_requisites()

    scen.order_project_cards()
    print("Ordered Projects:",scen.project_names())
    print("---end test_project sort()---")

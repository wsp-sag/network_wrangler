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
def test_project_card_read(request):
    print("\n--Starting:",request.node.name)
    in_dir  = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'example', 'stpaul','project_cards')
    in_file = os.path.join(in_dir,"1_simple_roadway_attribute_change.yml")
    project_card = ProjectCard.read(in_file)
    WranglerLogger.info(project_card.__dict__)
    print(str(project_card))
    assert(project_card.category == "Roadway Attribute Change")
    print("--Finished:",request.node.name)

@pytest.mark.ashish
@pytest.mark.scenario
def test_scenario_conflicts(request):

    project_cards_list = []
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','4_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','5_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','6_test_project_card.yml')))

    scen = Scenario.create_scenario(base_scenario = {}, project_cards_list = project_cards_list)

    print(str(scen))
    scen.check_scenario_conflicts()
    if scen.has_conflict_error:
        print('Conflicting project found for scenario!')

    print('Conflict checks done:', scen.conflicts_checked)
    print("--Finished:",request.node.name)

@pytest.mark.ashish
@pytest.mark.scenario
def test_scenario_requisites(request):
    print("\n--Starting:",request.node.name)
    base_scenario = {}

    project_cards_list = []
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','4_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','5_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','6_test_project_card.yml')))

    scen = Scenario.create_scenario(base_scenario = base_scenario, project_cards_list = project_cards_list)


    print(str(scen),"\n")

    scen.check_scenario_requisites()
    if scen.has_requisite_error:
        print('Missing pre- or co-requisite projects found for scenario!')

    print('Requisite checks done:', scen.requisites_checked)
    print("--Finished:",request.node.name)

@pytest.mark.scenario
def test_project_sort(request):
    print("\n--Starting:",request.node.name)
    base_scenario = {}

    project_cards_list = []
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','4_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','5_test_project_card.yml')))
    project_cards_list.append(ProjectCard.read(os.path.join(os.getcwd(),'example','stpaul','project_cards','6_test_project_card.yml')))

    scen = Scenario.create_scenario(base_scenario = base_scenario, project_cards_list = project_cards_list)
    print("\n> Prerequisites:")
    import pprint
    pprint.pprint(scen.prerequisites)
    print("\nUnordered Projects:",scen.project_names())
    scen.check_scenario_conflicts()
    scen.check_scenario_requisites()

    scen.order_project_cards()
    print("Ordered Projects:",scen.project_names())
    print("--Finished:",request.node.name)

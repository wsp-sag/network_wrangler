import os
import json
from network_wrangler import RoadwayNetwork
from network_wrangler import ProjectCard
from network_wrangler import Scenario
import yaml

def test_roadway_feature_change():
    dir = os.path.join(os.getcwd(),'example','stpaul')
    shape_file = os.path.join(dir,"shape.geojson")
    link_file = os.path.join(dir,"link.json")
    node_file = os.path.join(dir,"node.geojson")

    print(shape_file)
    print(link_file)
    print(node_file)

    print("Reading network ...")
    net = RoadwayNetwork.read(link_file= link_file, node_file=node_file, shape_file=shape_file)

    print("Reading project card ...")
    project_card = os.path.join(os.getcwd(),'example','stpaul','project_cards','1_simple_roadway_attribute_change.yml')
    project_card_dict = ProjectCard(project_card).dictionary

    road_dict = project_card_dict.get("Road")
    change_dict = project_card_dict.get("Change")
    road_id = road_dict.get("Name").split("=")[1]
    attribute = project_card_dict.get("Attribute").upper()
    existing_value = change_dict.get("Existing")
    build_value = change_dict.get("Build")

    print('link id: %s' % (road_id))
    print('attribute: %s' % (attribute))
    print('existing_value: %s' % (existing_value))
    print('build_value: %s' % (build_value))

    project_link_before = next(item for item in net.links_df['features'] if item["id"] == road_id)
    print(project_link_before)

    print("Applying project card ...")
    result = RoadwayNetwork.apply_roadway_feauture_change(net, project_card_dict)

    project_link_after = next(item for item in net.links_df['features'] if item["id"] == road_id)
    print(project_link_after)

def test_card_conflicts():
    base_scenario = {}

    card_directory = os.path.join(os.getcwd(),'example','stpaul','project_cards')

    project_card_names = ['4_test_project_card', '5_test_project_card', '6_test_project_card']
    scen = Scenario.Scenario.create_scenario(base_scenario = base_scenario, card_directory = card_directory, project_card_names = project_card_names)
    scen.check_scenario_conflicts()


if __name__ == "__main__":
    #test_roadway_feature_change()
    test_card_conflicts()

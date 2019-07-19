import os
import json
from network_wrangler import RoadwayNetwork
from network_wrangler import ProjectCard
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
    net = RoadwayNetwork.read(link_file= link_file, node_file=node_file, shape_file=shape_file, fast=True)

    print("Reading project card ...")
    project_card = os.path.join(os.getcwd(),'example','stpaul','project_cards','3_multiple_roadway_attribute_change.yml')
    project_card_dict = ProjectCard.read(project_card).__dict__

    print("Selecting Roadway Feaures ...")
    net_with_selection = RoadwayNetwork.select_roadway_features(net, project_card_dict)

    print("Applying project card ...")
    result = RoadwayNetwork.apply_roadway_feature_change(net_with_selection, project_card_dict)

    if result:
        print("Writing out revised network ...")
        RoadwayNetwork.write(net_with_selection, filename = 'out', path = 'tests')
    else:
        print("Error in applying project card ...")

def test_select_roadway_features():
    dir = os.path.join(os.getcwd(),'example','stpaul')
    shape_file = os.path.join(dir,"shape.geojson")
    link_file = os.path.join(dir,"link.json")
    node_file = os.path.join(dir,"node.geojson")

    print(shape_file)
    print(link_file)
    print(node_file)

    print("Reading network ...")
    net = RoadwayNetwork.read(link_file= link_file, node_file=node_file, shape_file=shape_file, fast=True)
    print(net.links_df.head())

    print("Reading project card ...")
    project_card = os.path.join(os.getcwd(),'example','stpaul','project_cards','3_multiple_roadway_attribute_change.yml')
    project_card_dict = ProjectCard.read(project_card).__dict__
    print(project_card_dict)

    print("Selecting Roadway Feaures ...")
    net_with_selection = RoadwayNetwork.select_roadway_features(net, project_card_dict)
    print(net_with_selection.links_df[net_with_selection.links_df['sel_links'] == 1])

def test_read_write():
    dir = os.path.join(os.getcwd(),'example','single')
    shape_file = os.path.join(dir,"shape.geojson")
    link_file = os.path.join(dir,"link.json")
    node_file = os.path.join(dir,"node.geojson")

    print(shape_file)
    print(link_file)
    print(node_file)

    print("Reading network ...")
    net = RoadwayNetwork.read(link_file= link_file, node_file=node_file, shape_file=shape_file)

    print(net.links_df.head())

    print("Writing network ...")
    RoadwayNetwork.write(net, filename = 'out', path = 'tests')

    dir = os.path.join(os.getcwd(),'tests')
    shape_file = os.path.join(dir,"out_shape.geojson")
    link_file = os.path.join(dir,"out_link.json")
    node_file = os.path.join(dir,"out_node.geojson")

    print(shape_file)
    print(link_file)
    print(node_file)

    print("Reading New network ...")
    net = RoadwayNetwork.read(link_file= link_file, node_file=node_file, shape_file=shape_file)
    print(net.links_df.head())

if __name__ == "__main__":
    #test_read_write()
    #test_select_roadway_features()
    test_roadway_feature_change()

import os
import json
from geopandas import GeoDataFrame
import pytest
from network_wrangler import RoadwayNetwork
from network_wrangler import ProjectCard
import time
import numpy as np
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 50000)

"""
Run just the tests labeled basic using `pytest -m roadway`
To run with print statments, use `pytest -s -m roadway`
"""

STPAUL_DIR = os.path.join(os.getcwd(),'example','stpaul')
STPAUL_SHAPE_FILE = os.path.join(STPAUL_DIR,"shape.geojson")
STPAUL_LINK_FILE = os.path.join(STPAUL_DIR,"link.json")
STPAUL_NODE_FILE = os.path.join(STPAUL_DIR,"node.geojson")

SMALL_DIR = os.path.join(os.getcwd(),'example','single')
SMALL_SHAPE_FILE = os.path.join(SMALL_DIR,"shape.geojson")
SMALL_LINK_FILE = os.path.join(SMALL_DIR,"link.json")
SMALL_NODE_FILE = os.path.join(SMALL_DIR,"node.geojson")

SCRATCH_DIR = os.path.join(os.getcwd(),"scratch")

@pytest.mark.roadway
def test_roadway_read_write(request):
    print("\n--Starting:",request.node.name)

    out_prefix = "t_readwrite"
    out_shape_file = os.path.join(SCRATCH_DIR,out_prefix+"_"+"shape.geojson")
    out_link_file  = os.path.join(SCRATCH_DIR,out_prefix+"_"+"link.json")
    out_node_file  = os.path.join(SCRATCH_DIR,out_prefix+"_"+"node.geojson")

    time0 = time.time()

    net = RoadwayNetwork.read(link_file= STPAUL_LINK_FILE, node_file=STPAUL_NODE_FILE, shape_file=STPAUL_SHAPE_FILE, fast=True)
    time1 = time.time()
    print("Writing to: {}".format(SCRATCH_DIR))
    net.write(filename=out_prefix,path=SCRATCH_DIR)
    time2 = time.time()
    net_2 = RoadwayNetwork.read(link_file= out_link_file, node_file=out_node_file, shape_file=out_shape_file)
    time3 = time.time()

    read_time1 = time1-time0
    read_time2 = time3-time2
    write_time = time2-time1

    print("TIME, read (w/out valdiation, with): {},{}".format(read_time1, read_time2))
    print("TIME, write:{}".format(write_time))
    '''
    # right now don't have a good way of ignoring differences in rounding
    with open(shape_file, 'r') as s1:
        og_shape = json.loads(s1.read())
        #og_shape.replace('\r', '').replace('\n', '').replace(' ','')
    with open(os.path.join('scratch','t_readwrite_shape.geojson'), 'r')  as s2:
        new_shape = json.loads(s2.read())
        #new_shape.replace('\r', '').replace('\n', '').replace(' ','')
    assert(og_shape==new_shape)
    '''

@pytest.mark.roadway
@pytest.mark.travis
def test_quick_roadway_read_write(request):
    print("\n--Starting:",request.node.name)

    out_prefix = "t_readwrite"
    out_shape_file = os.path.join(SCRATCH_DIR,out_prefix+"_"+"shape.geojson")
    out_link_file  = os.path.join(SCRATCH_DIR,out_prefix+"_"+"link.json")
    out_node_file  = os.path.join(SCRATCH_DIR,out_prefix+"_"+"node.geojson")
    net = RoadwayNetwork.read(link_file= SMALL_LINK_FILE, node_file=SMALL_NODE_FILE, shape_file=SMALL_SHAPE_FILE, fast=True)
    net.write(filename=out_prefix,path=SCRATCH_DIR)
    net_2 = RoadwayNetwork.read(link_file= out_link_file, node_file=out_node_file, shape_file=out_shape_file)
    print("--Finished:",request.node.name)

@pytest.mark.menow
@pytest.mark.basic
@pytest.mark.roadway
def test_select_roadway_features(request):
    print("\n--Starting:",request.node.name)
    net = RoadwayNetwork.read(link_file= STPAUL_LINK_FILE, node_file=STPAUL_NODE_FILE, shape_file=STPAUL_SHAPE_FILE, fast=True)

    test_selections = { \
    "1. simple": {
     'link':[{'name': ['6th','Sixth','sixth']}],
     'A':{'osmNodeId': '187899923'},
     'B':{'osmNodeId': '187865924'},
     'answer': ['187899923', '187858777', '187923585', '187865924'],
    },
    "2. farther": {
     'link':[{'name': ['6th','Sixth','sixth']}],
     'A':{'osmNodeId': '187899923'}, # start searching for segments at A
     'B':{'osmNodeId': '187942339'}
    },
    "3. multi-criteria": {
     'link':[
        {'name': ['6th','Sixth','sixth']},
        {'LANES': [1,2]},
        ],
     'A':{'osmNodeId': '187899923'}, # start searching for segments at A
     'B':{'osmNodeId': '187942339'}
    }
    }

    for i,sel in test_selections.items():
        print("--->",i,"\n",sel)
        path_found = False
        selected_links = net.select_roadway_features(sel)
        if not type(selected_links) == GeoDataFrame:
            print("Couldn't find path from {} to {}".format(sel['A'],sel['B']))
        else:
            print("Features selected:",len(selected_links))
            selected_nodes = [str(sel['A']['osmNodeId'])]+selected_links['v'].tolist()
            #print("Nodes selected: ",selected_nodes)

            if 'answer' in sel.keys():
                print("Expected Answer: ",sel['answer'])
                assert(set(selected_nodes) == set(sel['answer']))

    print("--Finished:",request.node.name)

@pytest.mark.roadway
@pytest.mark.travis
@pytest.mark.menow
def test_select_roadway_features_from_projectcard(request):
    print("\n--Starting:",request.node.name)

    print("Reading network ...")
    net = RoadwayNetwork.read(link_file= STPAUL_LINK_FILE, node_file=STPAUL_NODE_FILE, shape_file=STPAUL_SHAPE_FILE, fast=True)

    print("Reading project card ...")
    #project_card_name = '1_simple_roadway_attribute_change.yml'
    #project_card_name = '2_multiple_roadway.yml'
    project_card_name = '3_multiple_roadway_attribute_change.yml'

    project_card_path = os.path.join(os.getcwd(),'example','stpaul','project_cards',project_card_name)
    project_card = ProjectCard.read(project_card_path)

    print("Selecting roadway features ...")
    sel = project_card.facility
    print("Selection:\n",sel)
    selected_links = net.select_roadway_features(sel)

    if not type(selected_links) == GeoDataFrame:
        print("Couldn't find path from {} to {}".format(sel['A'],sel['B']))
    else:
        print("Features selected:",len(selected_links))

    print("--Finished:",request.node.name)

def roadway_feature_change(net, project_card):
    print("Selecting roadway features ...")
    sel = project_card.facility
    print("Selection:\n",sel)
    selected_links = net.select_roadway_features(sel)

    if not type(selected_links) == GeoDataFrame:
        print("Error in applying project card: Couldn't find path from {} to {}".format(sel['A'],sel['B']))
    else:
        selected_indices = selected_links.index.tolist()
        net.links_df['selected_links'] = np.where(net.links_df.index.isin(selected_indices), 1, 0)

        print("Applying project card ...")
        prop = project_card.properties
        print("Properties:\n",prop)
        revised_net = net.apply_roadway_feature_change(prop)

        columns_updated = [p['property'] for p in project_card.properties]

        orig_links = net.links_df.loc[selected_indices, columns_updated]
        print("Original Links:\n",orig_links)

        new_links = revised_net.links_df.loc[selected_indices, columns_updated]
        print("Updated Links:\n",new_links)

@pytest.mark.roadway
@pytest.mark.travis
@pytest.mark.menow
def test_roadway_feature_change(request):
    print("\n--Starting:",request.node.name)

    print("Reading network ...")
    net = RoadwayNetwork.read(link_file= STPAUL_LINK_FILE, node_file=STPAUL_NODE_FILE, shape_file=STPAUL_SHAPE_FILE, fast=True)

    project_card_set = [
        (net, '1_simple_roadway_attribute_change.yml'),
        (net, '2_multiple_roadway.yml'),
        (net, '3_multiple_roadway_attribute_change.yml'),
        (net, '4_simple_managed_lane.yml'),
        (net, '5_managed_lane.yml'),
    ]

    for my_net, project_card_name in project_card_set:
        project_card_path = os.path.join(os.getcwd(),'example','stpaul','project_cards',project_card_name)
        print("Reading project card from:\n {}".format(project_card_path))
        project_card = ProjectCard.read(project_card_path)

        roadway_feature_change(my_net, project_card)

    print("--Finished:",request.node.name)

@pytest.mark.managed
@pytest.mark.roadway
@pytest.mark.travis
def test_add_managed_lane(request):
    print("\n--Starting:",request.node.name)

    print("Reading network ...")
    net = RoadwayNetwork.read(link_file= STPAUL_LINK_FILE, node_file=STPAUL_NODE_FILE, shape_file=STPAUL_SHAPE_FILE, fast=True)

    print("Reading project card ...")
    #project_card_name = '4_simple_managed_lane.yml'
    project_card_name = '5_managed_lane.yml'
    project_card_path = os.path.join(os.getcwd(),'example','stpaul','project_cards',project_card_name)
    project_card = ProjectCard.read(project_card_path)

    print("Selecting roadway features ...")
    sel = project_card.facility
    print("\nSelection:",sel)
    selected_links = net.select_roadway_features(sel)

    if not type(selected_links) == GeoDataFrame:
        print("Couldn't find mainline facility from {} to {}".format(sel['A'],sel['B']))
    else:
        selected_indices = selected_links.index.tolist()

        prop = project_card.properties
        columns_updated = [p['property'] for p in prop]

        net.links_df['selected_links'] = np.where(net.links_df.index.isin(selected_indices), 1, 0)

        print("Applying project card ...")
        print("\nProperties:",prop)
        revised_net = net.add_roadway_attributes(prop)

        if 'selected_links' in revised_net.links_df.columns:
            revised_net.links_df.drop(['selected_links'], axis = 1, inplace = True)

        in_links = net.links_df.loc[selected_indices, columns_updated]
        print("\nOriginal Links:\n", in_links)

        out_links = revised_net.links_df.loc[selected_indices, columns_updated]
        print("\nRevised Links:\n", out_links)

        revised_net.links_df.loc[selected_indices, :].to_csv(os.path.join(SCRATCH_DIR, "ml_out_links.csv"), index=False)

        revised_net.write(filename="test_ml", path=SCRATCH_DIR)

    print("--Finished:",request.node.name)

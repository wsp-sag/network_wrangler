import os
import json
import pytest
from network_wrangler import RoadwayNetwork
from network_wrangler import ProjectCard
import time

"""
Run just the tests labeled basic using `pytest -m roadway`
To run with print statments, use `pytest -s -m roadway`
"""

STPAUL_DIR = os.path.join(os.getcwd(),'example','stpaul')
STPAUL_SHAPE_FILE = os.path.join(STPAUL_DIR,"shape.geojson")
STPAUL_LINK_FILE = os.path.join(STPAUL_DIR,"link.json")
STPAUL_NODE_FILE = os.path.join(STPAUL_DIR,"node.geojson")

SMALL_DIR = os.path.join(os.getcwd(),'example','stpaul')
SMALL_SHAPE_FILE = os.path.join(SMALL_DIR,"shape.geojson")
SMALL_LINK_FILE = os.path.join(SMALL_DIR,"link.json")
SMALL_NODE_FILE = os.path.join(SMALL_DIR,"node.geojson")


@pytest.mark.basic
@pytest.mark.roadway
def test_roadway_read_write(request):
    print("\n--Starting:",request.node.name)

    out_path   = "scratch"
    out_prefix = "t_readwrite"
    out_dir    = os.path.join(os.getcwd(),out_path)
    out_shape_file = os.path.join(out_dir,out_prefix+"_"+"shape.geojson")
    out_link_file  = os.path.join(out_dir,out_prefix+"_"+"link.json")
    out_node_file  = os.path.join(out_dir,out_prefix+"_"+"node.geojson")
    time0 = time.time()
    net = RoadwayNetwork.read(link_file= STPAUL_LINK_FILE, node_file=STPAUL_NODE_FILE, shape_file=STPAUL_SHAPE_FILE, fast=True)
    time1 = time.time()
    net.write(filename=out_prefix,path=out_path)
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

    out_path   = "scratch"
    out_prefix = "t_readwrite"
    out_dir    = os.path.join(os.getcwd(),out_path)
    out_shape_file = os.path.join(out_dir,out_prefix+"_"+"shape.geojson")
    out_link_file  = os.path.join(out_dir,out_prefix+"_"+"link.json")
    out_node_file  = os.path.join(out_dir,out_prefix+"_"+"node.geojson")
    net = RoadwayNetwork.read(link_file= SMALL_LINK_FILE, node_file=SMALL_NODE_FILE, shape_file=SMALL_SHAPE_FILE, fast=True)
    net.write(filename=out_prefix,path=out_path)
    net_2 = RoadwayNetwork.read(link_file= out_link_file, node_file=out_node_file, shape_file=out_shape_file)
    print("--Finished:",request.node.name)

@pytest.mark.menow
def test_select_roadway_features(request):
    print("\n--Starting:",request.node.name)
    net = RoadwayNetwork.read(link_file= STPAUL_LINK_FILE, node_file=STPAUL_NODE_FILE, shape_file=STPAUL_SHAPE_FILE, fast=True)

    test_selections = { \
    "1. simple": {
     'link':[
        {'name': ['6th','Sixth','sixth']}
        ],
     'A':{'osmNodeId': '187899923'},
     'B':{'osmNodeId': '187865924'},
     'answer': ['187899923', '187858777', '187923585', '187865924'],
    },
    "2. farther": {
     'link':[
        {'name': ['6th','Sixth','sixth']}
        ],
     'A':{'osmNodeId': '187899923'}, # start searching for segments at A
     'B':{'osmNodeId': '187942339'}
    },

    }

    for i,sel in test_selections.items():
        print("--->",i,"\n",sel)
        path_found = False
        sp_found = net.select_roadway_features(sel)
        if not sp_found:
            print("Couldn't find path from {} to {}".format(sel['A'],sel['B']))
        else:
            sel_key = net.build_selection_key(sel)
            sel_links = net.selections[sel_key]['links']
            print("Features selected:",len(sel_links))
            sel_nodes = [str(sel['A']['osmNodeId'])]+sel_links['v'].tolist()
            print("Nodes selected: ",sel_nodes)

            if 'answer' in sel.keys():
                print("Expected Answer: ",sel['answer'])
                assert(set(sel_nodes) == set(sel['answer']))

    print("--Finished:",request.node.name)

@pytest.mark.ashish
@pytest.mark.roadway
@pytest.mark.travis
def test_select_roadway_features_from_projectcard(request):
    print("\n--Starting:",request.node.name)

    print("Reading network ...")
    net = RoadwayNetwork.read(link_file= STPAUL_LINK_FILE, node_file=STPAUL_NODE_FILE, shape_file=STPAUL_SHAPE_FILE, fast=True)

    print("Reading project card ...")
    project_card_path = os.path.join(os.getcwd(),'example','stpaul','project_cards','3_multiple_roadway_attribute_change.yml')
    project_card = ProjectCard.read(project_card_path)

    print("Selecting roadway features ...")
    print("Selection:\n",project_card.facility)
    net.select_roadway_features(project_card.facility)
    print('Number of features selected', len(net.links_df[net.links_df['sel_links'] == 1]))
    print("--Finished:",request.node.name)

@pytest.mark.ashish
@pytest.mark.roadway
@pytest.mark.travis
def test_roadway_feature_change(request):
    print("\n--Starting:",request.node.name)

    print("Reading network ...")
    net = RoadwayNetwork.read(link_file= STPAUL_LINK_FILE, node_file=STPAUL_NODE_FILE, shape_file=STPAUL_SHAPE_FILE, fast=True)

    print("Reading project card ...")
    project_card_path = os.path.join(os.getcwd(),'example','stpaul','project_cards','3_multiple_roadway_attribute_change.yml')
    project_card = ProjectCard.read(project_card_path)

    print("Selecting roadway feaures ...")
    net.select_roadway_features(project_card.facility)

    print("Applying project card ...")
    error, revised_net = RoadwayNetwork.apply_roadway_feature_change(net, project_card.properties)

    if not error:
        print("Writing out revised network ...")
        RoadwayNetwork.write(revised_net, filename = 'out', path = 'tests')
    else:
        print("Error in applying project card ...")

    print("--Finished:",request.node.name)

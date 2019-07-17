import os
import json
import pytest
from network_wrangler import RoadwayNetwork
import time


"""
Run just the tests labeled basic using `pytest -m roadway`
To run with print statments, use `pytest -s -m roadway`
"""

@pytest.mark.menow
def test_roadway_change():
    print("HI")
    pass

@pytest.mark.basic
@pytest.mark.roadway
def test_roadway_read_write():
    in_dir        = os.path.join(os.getcwd(),'example','stpaul')
    in_shape_file = os.path.join(in_dir,"shape.geojson")
    in_link_file  = os.path.join(in_dir,"link.json")
    in_node_file  = os.path.join(in_dir,"node.geojson")

    out_path   = "scratch"
    out_prefix = "t_readwrite"
    out_dir    = os.path.join(os.getcwd(),out_path)
    out_shape_file = os.path.join(out_dir,out_prefix+"_"+"shape.geojson")
    out_link_file  = os.path.join(out_dir,out_prefix+"_"+"link.json")
    out_node_file  = os.path.join(out_dir,out_prefix+"_"+"node.geojson")
    time0 = time.time()
    net = RoadwayNetwork.read(link_file= in_link_file, node_file=in_node_file, shape_file=in_shape_file, fast=True)
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

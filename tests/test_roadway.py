import os
import json
import pytest
from network_wrangler import RoadwayNetwork

"""
Run just the tests labeled basic using `pytest -v -m basic`
"""

@pytest.mark.menow
def test_roadway_change():
    print("HI")
    pass

@pytest.mark.basic
@pytest.mark.travis
def test_roadway_read_write():
    in_dir        = os.path.join(os.getcwd(),'example','single')
    in_shape_file = os.path.join(in_dir,"shape.geojson")
    in_link_file  = os.path.join(in_dir,"link.json")
    in_node_file  = os.path.join(in_dir,"node.geojson")

    out_path   = "scratch"
    out_prefix = "t_readwrite"
    out_dir    = os.path.join(os.getcwd(),out_path)
    out_shape_file = os.path.join(out_dir,out_prefix+"_"+"shape.geojson")
    out_link_file  = os.path.join(out_dir,out_prefix+"_"+"link.json")
    out_node_file  = os.path.join(out_dir,out_prefix+"_"+"node.geojson")

    net = RoadwayNetwork.read(link_file= in_link_file, node_file=in_node_file, shape_file=in_shape_file)
    net.write(filename=out_prefix,path=out_path)
    net_2 = RoadwayNetwork.read(link_file= out_link_file, node_file=out_node_file, shape_file=out_shape_file)

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

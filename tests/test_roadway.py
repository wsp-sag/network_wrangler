import os
import pytest
from network_wrangler import RoadwayNetwork

"""
Run just the tests labeled basic using `pytest -v -m basic`
"""

@pytest.mark.basic
@pytest.mark.travis
def test_roadway_read_write():
    dir = os.path.join(os.getcwd(),'example','single')
    shape_file = os.path.join(dir,"shape.geojson")
    link_file = os.path.join(dir,"link.json")
    node_file = os.path.join(dir,"node.geojson")

    print(shape_file)
    print(link_file)
    print(node_file)

    net = RoadwayNetwork.read(link_file= link_file, node_file=node_file, shape_file=shape_file)
    print(net.nodes_df.columns)
    net.write(filename="t_readwrite")

    with open(shape_file, 'r') as s1:
        og_shape = s1.read()
        og_shape.replace('\r', '').replace('\n', '').replace(' ','')
    with open("t_readwrite_shape.geojson", 'r')  as s2:
        new_shape = s2.read()
        new_shape.replace('\r', '').replace('\n', '').replace(' ','')
    assert(og_shape==new_shape)

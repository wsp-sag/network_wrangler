import os
import json
import pytest
from network_wrangler import TransitNetwork

base_path    = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
examples_dir = os.path.join(base_path,'example')

"""
Run just the tests labeled transit using `pytest -v -m transit`
"""

@pytest.mark.basic
@pytest.mark.travis
@pytest.mark.transit
def test_transit_read_write():

    transit_net_dir = os.path.join(examples_dir, "stpaul")

    transit_net = TransitNetwork.read(path = transit_net_dir)

    transit_net.write(filename="tr_readwrite",path='tests')

if __name__ == '__main__':
    test_transit_read_write()

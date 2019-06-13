import os
import json
import pytest
from network_wrangler import TransitNetwork, setupLogging

base_path    = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
examples_dir = os.path.join(base_path,'example')
tests_temp_dir = os.path.join(base_path,'scratch')

if not os.path.exists(tests_temp_dir):
    os.mkdir(tests_temp_dir)

"""
Run just the tests labeled transit using `pytest -v -m transit`
"""

@pytest.mark.basic
@pytest.mark.travis
@pytest.mark.transit
def test_transit_read_write():
    transit_net_dir = os.path.join(examples_dir, "stpaul")

    transit_net = TransitNetwork(feed_path = transit_net_dir)
    print('Transit Net Directory:', transit_net_dir)
    transit_net.write(outpath=tests_temp_dir)
    print('Transit Write Directory:', tests_temp_dir)

if __name__ == '__main__':
    test_transit_read_write()

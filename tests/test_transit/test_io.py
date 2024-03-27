import pytest

from network_wrangler import TransitNetwork, load_transit
from network_wrangler import WranglerLogger

"""
Run just the tests using `pytest tests/test_transit/test_io.py`
"""


def test_transit_read_write(request, stpaul_transit_net, scratch_dir):
    """Checks that reading a network, writing it to a file and then reading it again
    results in a valid TransitNetwork.
    """
    stpaul_transit_net.write(path=scratch_dir)
    WranglerLogger.debug(f"Transit Write Directory:{scratch_dir}")
    WranglerLogger.debug(
        f"stpaul_transit_net.feed.frequencies\n{stpaul_transit_net.feed.frequencies.dtypes}"
    )
    stpaul_transit_net_read_write = load_transit(scratch_dir)
    assert isinstance(stpaul_transit_net_read_write, TransitNetwork)

    WranglerLogger.info(f"--Finished: {request.node.name}")

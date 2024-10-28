"""To run these tests, use `pytest -s tests/test_roadway/test_properties.py`."""

from network_wrangler import WranglerLogger
from network_wrangler.roadway.graph import assess_connectivity


def test_network_connectivity(request, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = stpaul_net
    _mode = "drive"
    _connected = net.is_connected(mode=_mode)
    WranglerLogger.debug(f"{_mode} Network Connected: {_connected}")
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_network_connectivity_ignore_single_nodes(request, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    net = stpaul_net
    _mode = "walk"
    _, disconnected_nodes = assess_connectivity(net, mode=_mode, ignore_end_nodes=True)

    WranglerLogger.debug(f"{len(disconnected_nodes)} Disconnected Subnetworks")
    assert len(disconnected_nodes) == 5

    WranglerLogger.info(f"--Finished: {request.node.name}")

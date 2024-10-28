"""Tests for public api of feed.py.

Run just these tests using `pytest tests/test_transit/test_feed.py`
"""

from pathlib import Path

import pytest
from pandera.errors import SchemaErrors

from network_wrangler import WranglerLogger, load_transit, write_transit
from network_wrangler.models._base.db import ForeignKeyValueError, RequiredTableError
from network_wrangler.transit.network import TransitNetwork
from network_wrangler.utils.models import TableValidationError

"""
Run just the tests using `pytest tests/test_transit/test_io.py`
"""


def test_transit_read_write_small(request, small_transit_net, test_out_dir):
    """Check read-write-read consistency for small transit network.

    Checks that reading a network, writing it to a file and then reading it again
    results in a valid TransitNetwork.
    """
    write_transit(small_transit_net, out_dir=test_out_dir)
    WranglerLogger.debug(f"Transit Write Directory: {test_out_dir}")
    small_transit_net_read_write = load_transit(test_out_dir)
    assert isinstance(small_transit_net_read_write, TransitNetwork)

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_transit_read_write(request, stpaul_transit_net, test_out_dir):
    """Check read-write-read consistency for larger transit network.

    Checks that reading a network, writing it to a file and then reading it again
    results in a valid TransitNetwork.
    """
    write_transit(stpaul_transit_net, out_dir=test_out_dir)
    WranglerLogger.debug(f"Transit Write Directory: {test_out_dir}")
    stpaul_transit_net_read_write = load_transit(test_out_dir)
    assert isinstance(stpaul_transit_net_read_write, TransitNetwork)

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_bad_dir(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    with pytest.raises(FileExistsError):
        load_transit("I don't exist")


def test_missing_files(request, test_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    missing_files_dir = test_dir / "data" / "transit_input_fail" / "missing_files"
    with pytest.raises(RequiredTableError):
        load_transit(missing_files_dir)


def test_bad_fk(request, test_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    bad_fk_dir = test_dir / "data" / "transit_input_fail" / "bad_fks"
    with pytest.raises(ForeignKeyValueError):
        load_transit(bad_fk_dir)


def test_bad_prop_vals(request, test_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    bad_prop_vals_dir = test_dir / "data" / "transit_input_fail" / "bad_prop_values"
    with pytest.raises(TableValidationError):
        load_transit(bad_prop_vals_dir)


def test_missing_props(request, test_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    missing_props_dir = test_dir / "data" / "transit_input_fail" / "missing_props"
    with pytest.raises(TableValidationError):
        load_transit(missing_props_dir)


def test_write_feed_geo(request, small_transit_net, small_net, test_out_dir):
    from network_wrangler.transit.io import write_feed_geo

    WranglerLogger.info(f"--Starting: {request.node.name}")
    write_feed_geo(
        small_transit_net.feed,
        ref_nodes_df=small_net.nodes_df,
        out_dir=test_out_dir,
        out_prefix="write_feed_geo_small",
    )
    assert Path(test_out_dir / "write_feed_geo_small_trn_stops.geojson").exists()
    assert Path(test_out_dir / "write_feed_geo_small_trn_shapes.geojson").exists()


def test_write_feed_geo_w_shapes(request, stpaul_transit_net, stpaul_net, test_out_dir):
    from network_wrangler.transit.io import write_feed_geo

    WranglerLogger.info(f"--Starting: {request.node.name}")
    write_feed_geo(
        stpaul_transit_net.feed,
        ref_nodes_df=stpaul_net.nodes_df,
        out_dir=test_out_dir,
        out_prefix="write_feed_geo_stpaul",
    )
    assert Path(test_out_dir / "write_feed_geo_stpaul_trn_stops.geojson").exists()
    assert Path(test_out_dir / "write_feed_geo_stpaul_trn_shapes.geojson").exists()

"""Tests for transit property change.

Run just these tests using `pytest tests/test_transit/test_transit_prop_changes.py`
"""

import copy
import pandas as pd
import pytest
from pandera.errors import SchemaError
from network_wrangler import WranglerLogger


def test_invalid_field_value_set(request, small_transit_net):
    """Checks that changing data to an invalid field value will fail."""
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(small_transit_net)
    feed = net.feed
    # For Enum/Categorical will fail when mutated.
    with pytest.raises(TypeError):
        feed.stops.loc[0, "wheelchair_boarding"] = 9999

    # Should fail to be coerced
    _new_stop_times = copy.deepcopy(feed.stop_times)
    _new_stop_times.loc[3, "shape_dist_traveled"] = "abc"
    with pytest.raises(SchemaError):
        feed.stop_times = _new_stop_times

    # Should fail to be coerced
    _new_stop_times = copy.deepcopy(feed.stop_times)
    _new_stop_times.loc[4, "arrival_time"] = "123"
    with pytest.raises(SchemaError):
        feed.stop_times = _new_stop_times

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_force_invalid_field_value(request, small_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(small_transit_net)
    feed = net.feed
    feed.stop_times.loc[3, "shape_dist_traveled"] = "abc"

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_valid_field_value_set(request, small_transit_net):
    """Checks that setting a valid field value will pass."""
    WranglerLogger.info(f"--Starting: {request.node.name}")
    net = copy.deepcopy(small_transit_net)
    feed = net.feed

    # Should be coerced
    _new_stop_times = copy.deepcopy(feed.stop_times)
    _new_stop_times.loc[0, "stop_sequence"] = "1"
    feed.stop_times = _new_stop_times
    assert feed.stop_times.loc[0, "stop_sequence"] == 1

    _new_stop_times = copy.deepcopy(feed.stop_times)
    _new_stop_times.loc[1, "arrival_time"] = "12:00:00"
    feed.stop_times = _new_stop_times
    assert feed.stop_times.loc[1, "arrival_time"] == pd.Timestamp("12:00:00")

    WranglerLogger.info(f"--Finished: {request.node.name}")


@pytest.mark.xfail
def test_coerce_over24hr_times(request, small_transit_net):
    """Checks that setting a valid field value will pass."""
    WranglerLogger.info(f"--Starting: {request.node.name}")
    feed = small_transit_net.feed
    # Should be ok b/c GTFS can last "several days"
    _new_stop_times = copy.deepcopy(feed.stop_times)
    _new_stop_times.loc[2, "arrival_time"] = "42:00:00"
    feed.stop_times = _new_stop_times
    assert feed.stop_times.loc[2, "arrival_time"] == pd.Timestamp("42:00:00")

    WranglerLogger.debug(f"feed.stop_times: \n{feed.stop_times}")
    WranglerLogger.info(f"--Finished: {request.node.name}")

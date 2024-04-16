import pytest

from network_wrangler import WranglerLogger


def test_invalid_field_value_set(request, stpaul_transit_net):
    """Checks that reading in data with an invalid field value will fail."""
    tables_dict = stpaul_transit_net.feed.tables_dict

    # For Enum/Categorical
    with pytest.raises(TypeError):
        tables_dict["stops"].loc[0, "wheelchair_boarding"] = 9999

    # Should fail to be coerced
    tables_dict["stop_times"].loc[3, "stop_distance"] = "abc"

    # Should fail to be coerced
    tables_dict["stop_times"].loc[4, "arrival_time"] = "123"

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_valid_field_value_set(request, stpaul_transit_net):
    """Checks that setting a valid field value will pass"""
    tables_dict = stpaul_transit_net.feed.tables_dict

    # Should be coerced
    tables_dict["stop_times"].loc[0, "stop_sequence"] = "1"
    tables_dict["stop_times"].loc[2, "arrival_time"] = "12:00:00"
    # Should be ok b/c GTFS can last "several days"
    tables_dict["stop_times"].loc[2, "arrival_time"] = "42:00:00"

    WranglerLogger.info(f"--Finished: {request.node.name}")

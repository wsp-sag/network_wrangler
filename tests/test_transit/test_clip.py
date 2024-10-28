"""Tests for public api of transit clipping.

Run just these tests using `pytest tests/test_transit/tet_clip.py`
"""

import geopandas as gpd

from network_wrangler import WranglerLogger
from network_wrangler.transit.clip import clip_transit
from network_wrangler.transit.io import write_feed_geo


def test_clip_transit_node_ids(request, small_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    node_ids = [1, 2]
    clipped_network = clip_transit(small_transit_net, node_ids=node_ids)

    # Assert that the clipped network is not empty
    assert len(clipped_network.feed.stops) > 0
    assert len(clipped_network.feed.routes) > 0
    WranglerLogger.debug(f"\nClipped Stops: \n{clipped_network.feed.stops}")
    assert 2 in clipped_network.feed.stops.stop_id.values
    assert 3 not in clipped_network.feed.stops.stop_id.values
    WranglerLogger.debug(f"\nClipped Shapes: \n{clipped_network.feed.shapes}")
    assert 2 in clipped_network.feed.shapes.shape_model_node_id.values
    assert 3 not in clipped_network.feed.shapes.shape_model_node_id.values
    WranglerLogger.debug(f"\nClipped Stop Times: \n{clipped_network.feed.stop_times}")
    assert 2 in clipped_network.feed.stop_times.stop_id.values
    assert 3 not in clipped_network.feed.stop_times.stop_id.values

    # since 4 is after 3, should have cut this too
    assert 4 not in clipped_network.feed.shapes.shape_model_node_id.values


def test_clip_transit_min_stops(request, small_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    node_ids = [1, 3, 4]
    clipped_network = clip_transit(small_transit_net, node_ids=node_ids, min_stops=3)

    # Assert that the clipped network is not empty
    assert len(clipped_network.feed.stops) > 0
    assert len(clipped_network.feed.routes) > 0

    # assert that it kept blue-2 but not blue-1 which only has 2 stops
    WranglerLogger.debug(f"\nClipped Stops: \n{clipped_network.feed.stops}")
    assert 3 in clipped_network.feed.stops.stop_id.values
    WranglerLogger.debug(f"\nClipped Stop Times: \n{clipped_network.feed.stop_times}")
    assert "blue-1" not in clipped_network.feed.stop_times.trip_id.values
    assert "blue-2" in clipped_network.feed.stop_times.trip_id.values
    WranglerLogger.debug(f"\nClipped Trips: \n{clipped_network.feed.trips}")
    assert "blue-1" not in clipped_network.feed.trips.trip_id.values
    assert "blue-2" in clipped_network.feed.trips.trip_id.values


def test_clip_transit_boundary_geocode(request, stpaul_transit_net, test_dir, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    boundary_geocode = "Downtown, St Paul, MN"
    clipped_network = clip_transit(
        stpaul_transit_net, boundary_geocode=boundary_geocode, roadway_net=stpaul_net
    )

    assert len(clipped_network.feed.stops) > 0
    assert len(clipped_network.feed.routes) > 0

    # TODO Add assertions for specific stops and trips
    # TRIP_IN = "JUN19-MVS-BUS-Weekday-01"
    # TRIP_OUT = 1
    # STOP_NODE_IN = 100781
    # STOP_NODE_OUT = 1
    # assert TRIP_IN in clipped_network.feed.trips.trip_id.values
    # assert TRIP_OUT not in clipped_network.feed.trips.values
    # assert STOP_NODE_IN in clipped_network.feed.stops.model_node_id.values
    # assert STOP_OUT not in clipped_network.feed.stops.values
    write_feed_geo(
        clipped_network.feed,
        ref_nodes_df=stpaul_net.nodes_df,
        out_dir=test_dir / "out",
        out_prefix="DwntnSP",
    )


def test_clip_transit_to_roadway(request, stpaul_transit_net, test_dir, stpaul_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    from network_wrangler.roadway.clip import clip_roadway

    boundary_file = test_dir / "data" / "unionstation.geojson"
    boundary_gdf = gpd.read_file(boundary_file)

    WranglerLogger.debug("----CLIP TO ROADWAY TO BOUNDARY GDF---- ")
    clipped_rd = clip_roadway(stpaul_net, boundary_gdf=boundary_gdf)

    WranglerLogger.debug("----CLIP TRANSIT TO ROADWAY---- ")
    clipped_net_to_rd = clip_transit(stpaul_transit_net, roadway_net=clipped_rd)
    # this method will also check tha the resulting transit network is consistent with
    # the roadway networks

    # make sure not empty
    assert len(clipped_net_to_rd.feed.stops) > 0
    assert len(clipped_net_to_rd.feed.routes) > 0
    assert len(clipped_net_to_rd.feed.frequencies) > 0
    assert len(clipped_net_to_rd.feed.trips) > 0
    assert len(clipped_net_to_rd.feed.stop_times) > 0
    assert len(clipped_net_to_rd.feed.shapes) > 0

    write_feed_geo(
        clipped_net_to_rd.feed,
        ref_nodes_df=stpaul_net.nodes_df,
        out_dir=test_dir / "out",
        out_prefix="UnionSt",
    )

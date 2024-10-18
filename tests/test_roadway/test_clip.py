"""Tests roadway network clipping functions."""

import geopandas as gpd

from network_wrangler.roadway.clip import clip_roadway
from network_wrangler.roadway.io import write_roadway


def test_clip_roadway_geojson(stpaul_net, test_dir):
    boundary_file = test_dir / "data" / "ecolab.geojson"
    clipped_network = clip_roadway(stpaul_net, boundary_file=boundary_file)

    # Assert that the clipped network is not empty
    assert len(clipped_network.nodes_df) > 0
    assert len(clipped_network.links_df) > 0
    write_roadway(clipped_network, out_dir=test_dir / "out", prefix="ecolab", true_shape=True)


def test_clip_roadway_geocode(stpaul_net, test_dir):
    clipped_network = clip_roadway(stpaul_net, boundary_geocode="Downtown, St Paul, MN")

    # Assert that the clipped network is not empty
    assert len(clipped_network.nodes_df) > 0
    assert len(clipped_network.links_df) > 0
    write_roadway(clipped_network, out_dir=test_dir / "out", prefix="downtown", true_shape=True)


def test_clip_roadway_gdf(stpaul_net, test_dir):
    boundary_file = test_dir / "data" / "unionstation.geojson"
    boundary_gdf = gpd.read_file(boundary_file)
    clipped_network = clip_roadway(stpaul_net, boundary_gdf=boundary_gdf)

    # Assert that the clipped network is not empty
    assert len(clipped_network.nodes_df) > 0
    assert len(clipped_network.links_df) > 0
    write_roadway(
        clipped_network, out_dir=test_dir / "out", prefix="union_station", true_shape=True
    )

"""Tests for roadway centroid nodes and centroid connectors.

Uses the ``small`` example network as a base and builds two synthetic zones so the
end-to-end ``add_centroid_nodes`` -> ``add_centroid_connectors`` flow can be exercised
as a regression guard.
"""

import geopandas as gpd
import pytest
from shapely.geometry import Point

from network_wrangler import load_roadway_from_dir
from network_wrangler.roadway.links.connectors import (
    FitForCentroidConnection,
    add_centroid_connectors,
    calculate_bearing_from_centroid,
)
from network_wrangler.roadway.nodes.centroids import (
    add_centroid_nodes,
    prepare_zones_table,
    zones_table_to_gdf,
)

LAT_LON_CRS = 4326
LOCAL_CRS = "EPSG:26915"  # UTM 15N, appropriate for the St Paul small example


@pytest.fixture
def centroid_net(small_ex_dir):
    """Fresh copy of the small network prepped for centroid connectors.

    A fresh load (rather than the shared ``small_net`` fixture) avoids mutating a
    network used by other tests.
    """
    net = load_roadway_from_dir(small_ex_dir)
    # every link is a great candidate to connect a centroid to
    net.links_df["drive_centroid_fit"] = int(FitForCentroidConnection.BEST)
    return net


@pytest.fixture
def zones_gdf(centroid_net):
    """Two zones splitting the small network's nodes east/west of a longitude line."""
    nodes = centroid_net.nodes_df
    groups = [
        (1001, nodes[nodes["X"] < -93.092]),
        (1002, nodes[nodes["X"] >= -93.092]),
    ]
    zones = []
    for zone_id, sub in groups:
        hull = sub.geometry.union_all().convex_hull.buffer(0.001)
        zones.append({"TAZ": zone_id, "geometry": hull, "geometry_centroid": hull.centroid})
    return gpd.GeoDataFrame(zones, geometry="geometry", crs=LAT_LON_CRS)


def test_calculate_bearing_from_centroid_cardinal_points():
    """calculate_bearing_from_centroid returns clockwise-from-north bearings for the 4 cardinals."""
    # points due N, E, S, W of the origin centroid
    gdf = gpd.GeoDataFrame(
        {"geometry": [Point(0, 1), Point(1, 0), Point(0, -1), Point(-1, 0)]},
        geometry="geometry",
    )
    gdf["geometry_centroid"] = [Point(0, 0)] * 4

    result = calculate_bearing_from_centroid(gdf, "geometry_centroid", "angle")

    assert result["angle"].round().tolist() == [0.0, 90.0, 180.0, 270.0]


def test_add_centroid_nodes(centroid_net, zones_gdf):
    """Centroid nodes are added with model_node_id = zone id and X/Y at the centroid."""
    n_nodes_before = len(centroid_net.nodes_df)

    zones_table = prepare_zones_table(zones_gdf, zone_id_col="TAZ")
    add_centroid_nodes(centroid_net, zones_table)

    assert len(centroid_net.nodes_df) == n_nodes_before + len(zones_gdf)
    for _, zone in zones_gdf.iterrows():
        node = centroid_net.nodes_df.loc[
            centroid_net.nodes_df["model_node_id"] == zone["TAZ"]
        ].iloc[0]
        assert node["X"] == pytest.approx(zone["geometry_centroid"].x)
        assert node["Y"] == pytest.approx(zone["geometry_centroid"].y)


def test_add_centroid_connectors(centroid_net, zones_gdf):
    """Connectors are created bidirectionally for each zone up to the requested count."""
    num_connectors = 2
    zones_table = prepare_zones_table(zones_gdf, zone_id_col="TAZ")
    add_centroid_nodes(centroid_net, zones_table)
    n_links_before = len(centroid_net.links_df)

    summary = add_centroid_connectors(
        centroid_net,
        zones_table.copy(),
        mode="drive",
        local_crs=LOCAL_CRS,
        zone_buffer_distance=50,
        num_centroid_connectors=num_connectors,
        max_mode_graph_degrees=6,
        default_link_attribute_dict={"lanes": 1},
    )

    per_zone = dict(zip(summary["zone_id"], summary["num_connectors"], strict=False))
    assert per_zone == {1001: num_connectors, 1002: num_connectors}

    n_added = len(centroid_net.links_df) - n_links_before
    # bidirectional: one link each way per connector
    assert n_added == 2 * sum(per_zone.values())

    new_links = centroid_net.links_df.tail(n_added)
    for taz, count in per_zone.items():
        outbound = new_links[new_links["A"] == taz]
        inbound = new_links[new_links["B"] == taz]
        assert len(outbound) == count
        assert len(inbound) == count
        # the same nodes are connected in both directions
        assert set(outbound["B"]) == set(inbound["A"])


def test_add_centroid_connectors_two_zone_types(centroid_net, zones_gdf):
    """Adding a second zone type (e.g. MAZs after TAZs) never connects centroid to centroid."""
    taz_table = prepare_zones_table(zones_gdf, zone_id_col="TAZ")
    add_centroid_nodes(centroid_net, taz_table)
    add_centroid_connectors(
        centroid_net,
        taz_table.copy(),
        mode="drive",
        local_crs=LOCAL_CRS,
        zone_buffer_distance=50,
        num_centroid_connectors=2,
        max_mode_graph_degrees=6,
        default_link_attribute_dict={"lanes": 1},
    )
    taz_ids = set(zones_gdf["TAZ"])

    # non-overlapping id space, same geography, so MAZ candidates could otherwise
    # spatially include the just-created TAZ centroids
    maz_gdf = zones_gdf.copy()
    maz_gdf["MAZ"] = maz_gdf["TAZ"] + 1000
    maz_table = prepare_zones_table(maz_gdf, zone_id_col="MAZ")
    add_centroid_nodes(centroid_net, maz_table)
    n_links_before = len(centroid_net.links_df)

    summary = add_centroid_connectors(
        centroid_net,
        maz_table.copy(),
        mode="drive",
        local_crs=LOCAL_CRS,
        zone_buffer_distance=50,
        num_centroid_connectors=2,
        max_mode_graph_degrees=6,
        default_link_attribute_dict={"lanes": 1},
    )
    assert (summary["num_connectors"] > 0).all()

    new_links = centroid_net.links_df.tail(len(centroid_net.links_df) - n_links_before)
    assert not (new_links["A"].isin(taz_ids) | new_links["B"].isin(taz_ids)).any()

    # the second (MAZ) pass must not wipe out the first (TAZ) pass's counts
    taz_counts = centroid_net.nodes_df.loc[
        centroid_net.nodes_df["model_node_id"].isin(taz_ids), "zone_num_connectors"
    ]
    assert (taz_counts == 2).all()
    maz_ids = set(maz_gdf["MAZ"])
    maz_counts = centroid_net.nodes_df.loc[
        centroid_net.nodes_df["model_node_id"].isin(maz_ids), "zone_num_connectors"
    ]
    assert (maz_counts == 2).all()


def test_zones_table_roundtrip_with_metadata(zones_gdf):
    """Zones table conversion preserves metadata and can rename the ID column on request."""
    zones_table = prepare_zones_table(
        zones_gdf,
        zone_id_col="TAZ",
        metadata={"zone_name": "TAZ"},
    )

    assert zones_table.attrs["zone_name"] == "TAZ"

    unrenamed = zones_table_to_gdf(zones_table)
    assert "zone_id" in unrenamed.columns

    roundtrip = zones_table_to_gdf(zones_table, zone_id_col="TAZ")
    assert "TAZ" in roundtrip.columns
    assert "zone_id" not in roundtrip.columns
    assert roundtrip.attrs["zone_name"] == "TAZ"

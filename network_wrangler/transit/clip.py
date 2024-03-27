"""
Functions to clip a TransitNetwork object to a boundary.

Clipped transit is an independent transit network that is a subset of the original transit network.

Example usage:

```python
from network_wrangler.transit load_transit, write_transit
from network_wrangler.trasit.clip import clip_transit

stpaul_transit = load_transit(example_dir / "stpaul")
boundary_file = test_dir / "data" / "ecolab.geojson"
clipped_network = clip_transit(stpaul_transit, boundary_file=boundary_file)
write_transit(clipped_network, out_dir, prefix="ecolab", format="geojson", true_shape=True)
```

"""

from typing import Union
from pathlib import Path

import geopandas as gpd

from ..utils import get_bounding_polygon


def clip_transit_to_dfs(
    network: "TransitNetwork",
    boundary_gdf: gpd.GeoDataFrame = None,
    boundary_geocode: Union[str, dict] = None,
    boundary_file: Union[str, Path] = None,
) -> dict:
    """clip a TransitNetwork object to a boundary.

    Retains only the links within or crossing the boundary and all the nodes that those links
    connect to.

    Args:
        network (TransitNetwork): _description_
        boundary_gdf (gpd.GeoDataFrame, optional): _description_. Defaults to None.
        boundary_geocode (Union[str,dict], optional): _description_. Defaults to None.
        boundary_file (Union[str,Path], optional): _description_. Defaults to None.

    Returns:
        Dict mapping table names to GeoDataFrames of a transit feed
    """
    boundary_gdf = get_bounding_polygon(
        boundary_gdf=boundary_gdf,
        boundary_geocode=boundary_geocode,
        boundary_file=boundary_file,
    )
    clipped_feed_dfs = {}
    # make sure boundary_gdf.crs == network.crs
    if not boundary_gdf.crs == network.crs:
        boundary_gdf = boundary_gdf.to_crs(network.crs)
    _feed = network.feed
    # get the shape nodes that intersect the boundary

    clipped_feed_dfs["stops"] = _feed.feed.stops[
        _feed.stops.geometry.intersects(boundary_gdf)
    ]

    clipped_feed_dfs["shapes"] = _feed.shapes[
        _feed.shapes.geometry.intersects(boundary_gdf)
    ]

    sel_shapes = clipped_feed_dfs["shapes"].shape_id.unique().tolist()
    clipped_feed_dfs["trips"] = _feed.trips[_feed.trips.shape_id.isin(sel_shapes)]

    sel_routes = clipped_feed_dfs["trips"].route_id.unique().tolist()
    clipped_feed_dfs["routes"] = _feed.routes[_feed.routes.route_id.isin(sel_routes)]

    sel_stops = clipped_feed_dfs["stops"].stop_id.unique().tolist()
    clipped_feed_dfs["stop_times"] = _feed.stop_times[
        _feed.stop_times.stop_id.isin(sel_stops)
    ]

    for table in _feed.tables:
        if table not in clipped_feed_dfs:
            clipped_feed_dfs[table] = _feed.__dict__[table]

    return clipped_feed_dfs


def clip_transit(
    network: "TransitNetwork",
    boundary_gdf: gpd.GeoDataFrame = None,
    boundary_geocode: Union[str, dict] = None,
    boundary_file: Union[str, Path] = None,
) -> "TransitNetwork":
    """clip a TransitNetwork object to a boundary.

    Retains only the links within or crossing the boundary and all the nodes that those links
    connect to.

    Args:
        network (TransitNetwork): _description_
        boundary_gdf (gpd.GeoDataFrame, optional): _description_. Defaults to None.
        boundary_geocode (Union[str,dict], optional): _description_. Defaults to None.
        boundary_file (Union[str,Path], optional): _description_. Defaults to None.
    """
    from .io import load_transit

    clipped_feed_dfs = clip_transit_to_dfs(
        network=network,
        boundary_gdf=boundary_gdf,
        boundary_geocode=boundary_geocode,
        boundary_file=boundary_file,
    )

    # create a new TransitNetwork object with the clipped feed dataframes
    clipped_net = load_transit(clipped_feed_dfs)

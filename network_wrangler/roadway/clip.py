"""
Functions to clip a RoadwayNetwork object to a boundary.

Clipped roadway is an independent roadway network that is a subset of the original roadway network.

Unlike a Subnet, it is geographic selection defined by a bounday rather than
a logical selection defined by a graph.

Example usage:

```python

from network_wrangler.roadway load_roadway_from_dir, write_roadway
from network_wrangler.roadway.clip import clip_roadway

stpaul_net = load_roadway_from_dir(example_dir / "stpaul")
boundary_file = test_dir / "data" / "ecolab.geojson"
clipped_network = clip_roadway(stpaul_net, boundary_file=boundary_file)
write_roadway(clipped_network, out_dir, prefix="ecolab", format="geojson", true_shape=True)
```

"""
from typing import Union
from pathlib import Path

import geopandas as gpd

from ..utils import get_bounding_polygon


def clip_roadway_to_dfs(
    network: "RoadwayNetwork",
    boundary_gdf: gpd.GeoDataFrame = None,
    boundary_geocode: Union[str, dict] = None,
    boundary_file: Union[str, Path] = None,
) -> tuple:
    """Clips a RoadwayNetwork object to a boundary and returns the resulting GeoDataFrames.

    Retains only the links within or crossing the boundary and all the nodes that those links
    connect to.

    Args:
        network (RoadwayNetwork): RoadwayNetwork object to be clipped.
        boundary_gdf (gpd.GeoDataFrame, optional): GeoDataframe of one or more polygons which
            define the boundary to clip to. Defaults to None.
        boundary_geocode (Union[str,dict], optional): Place name to clip data to as ascertained
            from open street maps's Nomatim API (e.g. "Hennipen County, MN, USA").
            Defaults to None.
        boundary_file (Union[str,Path], optional): Geographic data file that can be read by
            GeoPandas (e.g. geojson, parquet, shp) that defines a geographic polygon area to clip
            to. Defaults to None.

    Returns: tuple of GeoDataFrames trimmed_links_df, trimmed_nodes_df, trimmed_shapes_df

    """
    boundary_gdf = get_bounding_polygon(
        boundary_gdf=boundary_gdf,
        boundary_geocode=boundary_geocode,
        boundary_file=boundary_file,
    )

    # make sure boundary_gdf.crs == network.crs
    if boundary_gdf.crs != network.crs:
        boundary_gdf = boundary_gdf.to_crs(network.crs)
    # get the boundary as a single polygon
    boundary = boundary_gdf.geometry.unary_union
    # get the links that intersect the boundary
    trimmed_links_df = network.links_df[network.links_df.geometry.intersects(boundary)]

    # get the nodes that the links connect to
    trimmed_node_ids = network.node_ids_in_links(trimmed_links_df)
    trimmed_nodes_df = network.nodes_df[network.nodes_df.index.isin(trimmed_node_ids)]

    # get shapes the links use
    trimmed_shapes_df = network.shapes_df[
        network.shapes_df.index.isin(trimmed_links_df["shape_id"])
    ]

    return trimmed_links_df, trimmed_nodes_df, trimmed_shapes_df


def clip_roadway(
    network: "RoadwayNetwork",
    boundary_gdf: gpd.GeoDataFrame = None,
    boundary_geocode: Union[str, dict] = None,
    boundary_file: Union[str, Path] = None,
) -> "RoadwayNetwork":
    """
    Clip a RoadwayNetwork object to a boundary.

    Retains only the links within or crossing the boundary and all the nodes that those links
    connect to.  At least one of boundary_gdf, boundary_geocode, or boundary_file must be provided.

    Args:
        network (RoadwayNetwork): RoadwayNetwork object to be clipped.
        boundary_gdf (gpd.GeoDataFrame, optional): GeoDataframe of one or more polygons which
            define the boundary to clip to. Defaults to None.
        boundary_geocode (Union[str,dict], optional): Place name to clip data to as ascertained
            from open street maps's Nomatim API (e.g. "Hennipen County, MN, USA").
            Defaults to None.
        boundary_file (Union[str,Path], optional): Geographic data file that can be read by
            GeoPandas (e.g. geojson, parquet, shp) that defines a geographic polygon area to clip
            to. Defaults to None.

    Returns: RoadwayNetwork clipped to the defined boundary.
    """

    trimmed_links_df, trimmed_nodes_df, trimmed_shapes_df = clip_roadway_to_dfs(
        network=network,
        boundary_gdf=boundary_gdf,
        boundary_geocode=boundary_geocode,
        boundary_file=boundary_file,
    )
    from .network import RoadwayNetwork

    trimmed_net = RoadwayNetwork(
        links_df=trimmed_links_df,
        nodes_df=trimmed_nodes_df,
        shapes_df=trimmed_shapes_df,
    )
    return trimmed_net

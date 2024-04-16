import geopandas as gpd
import pandas as pd

from shapely import LineString

from ..logger import WranglerLogger

from .feed import unique_shape_links, unique_stop_time_links
from .feed import WranglerShapesTable, WranglerStopTimesTable, WranglerStopsTable, TripsTable


def to_points_gdf(
    table: pd.DataFrame,
    ref_nodes_df: gpd.GeoDataFrame = None,
    ref_road_net: "RoadwayNetwork" = None,
    **kwargs,
) -> gpd.GeoDataFrame:
    """
    Convert a table to a GeoDataFrame.

    If the table is already a GeoDataFrame, return it as is. Otherwise, attempt to convert the table
    to a GeoDataFrame using the following methods:
    1. If the table has a 'geometry' column, return a GeoDataFrame using that column.
    2. If the table has 'lat' and 'lon' columns, return a GeoDataFrame using those columns.
    3. If the table has a '*model_node_id' column, return a GeoDataFrame using that column and the
         nodes_df provided.
    If none of the above, raise a ValueError.

    Args:
        table: DataFrame to convert to GeoDataFrame.
        ref_nodes_df: GeoDataFrame of nodes to use to convert model_node_id to geometry.
        ref_road_net: RoadwayNetwork object to use to convert model_node_id to geometry.

    Returns:
        GeoDataFrame: GeoDataFrame representation of the table.
    """
    if table is gpd.GeoDataFrame:
        return table

    WranglerLogger.debug("Converting GTFS table to GeoDataFrame")
    if "geometry" in table.columns:
        return gpd.GeoDataFrame(table, geometry="geometry")

    lat_cols = list(filter(lambda col: "lat" in col, table.columns))
    lon_cols = list(filter(lambda col: "lon" in col, table.columns))
    model_node_id_cols = list(filter(lambda col: "model_node_id" in col, table.columns))

    if not (lat_cols and lon_cols) or not model_node_id_cols:
        raise ValueError(
            "Could not find lat/long, geometry columns or *model_node_id column in \
                         table necessary to convert to GeoDataFrame"
        )

    if lat_cols and lon_cols:
        # using first found lat and lon columns
        return gpd.GeoDataFrame(
            table,
            geometry=gpd.points_from_xy(table[lon_cols[0]], table[lat_cols[0]]),
            crs="EPSG:4326",
        )

    if model_node_id_cols:
        node_id_col = model_node_id_cols[0]

        if ref_nodes_df is None:
            if ref_road_net is None:
                raise ValueError(
                    "Must provide either nodes_df or road_net to convert \
                                 model_node_id to geometry"
                )
            ref_nodes_df = ref_road_net.nodes_df

        WranglerLogger.debug(
            "Converting GTFS table to GeoDataFrame using model_node_id"
        )

        _table = table.merge(
            ref_nodes_df[["model_node_id,geometry"]],
            left_on=node_id_col,
            right_on="model_node_id",
        )
        return gpd.GeoDataFrame(_table, geometry="geometry")

    raise ValueError(
        "Could not find lat/long, geometry columns or *model_node_id column in table \
                     necessary to convert to GeoDataFrame"
    )


def shapes_to_trip_shapes_gdf(
    shapes: "WranglerShapesTable",
    # trips: TripsTable,
    ref_nodes_df: gpd.GeoDataFrame = None,
    crs: int = 4326
) -> gpd.GeoDataFrame:
    """
    Geodataframe with one polyline shape per shape_id.

    TODO: add information about the route and trips.

    Args:
        shapes: WranglerShapesTable
        trips: TripsTable
        ref_nodes_df: If specified, will use geometry from these nodes.  Otherwise, will use
            geometry in shapes file. Defaults to None.
        crs: int, optional, default 4326
    """

    if ref_nodes_df is not None:
        shapes = update_shapes_geometry(shapes, ref_nodes_df)

    shape_geom = (
        shapes[["shape_id", "shape_pt_lat", "shape_pt_lon"]]
        .groupby("shape_id")
        .agg(list)
        .apply(lambda x: LineString(zip(x[1], x[0])), axis=1)
    )

    route_shapes_gdf = gpd.GeoDataFrame(
        data=shape_geom.index, geometry=shape_geom.values, crs=crs
    )

    return route_shapes_gdf


def update_point_geometry(
    df: pd.DataFrame,
    ref_point_df: pd.DataFrame,
    lon_field: str = "X",
    lat_field: str = "Y",
    id_field: str = "model_node_id",
    ref_lon_field: str = "X",
    ref_lat_field: str = "Y",
    ref_id_field: str = "model_node_id",
) -> pd.DataFrame:
    from ..utils.data import update_df_by_col_value
    import copy

    df = copy.deepcopy(df)

    ref_df = ref_point_df.rename(
        columns={
            ref_lon_field: lon_field,
            ref_lat_field: lat_field,
            ref_id_field: id_field,
        }
    )

    updated_df = update_df_by_col_value(
        df,
        ref_df[[id_field, lon_field, lat_field]],
        id_field,
        properties=[lat_field, lon_field],
        source_must_update_all=False
    )
    return updated_df


def update_stops_geometry(stops: "WranglerStopsTable", ref_nodes_df):
    return update_point_geometry(
        stops, ref_nodes_df, lon_field="stop_lon", lat_field="stop_lat"
    )


def update_shapes_geometry(shapes: "WranglerShapesTable", ref_nodes_df):
    return update_point_geometry(
        shapes,
        ref_nodes_df,
        id_field="shape_model_node_id",
        lon_field="shape_pt_lon",
        lat_field="shape_pt_lat",
    )


def shapes_to_shape_links_gdf(
    shapes: WranglerShapesTable,
    ref_nodes_df: gpd.GeoDataFrame = None,
    from_field: str = "A",
    to_field: str = "B",
    crs: int = 4326,
) -> gpd.GeoDataFrame:
    """Translates shapes to shape links geodataframe using geometry from ref_nodes_df if provided.

    TODO: Add join to links and then shapes to get true geometry.

    Args:
        shapes: Feed shapes table
        ref_nodes_df: If specified, will use geometry from these nodes.  Otherwise, will use
            geometry in shapes file. Defaults to None.
        from_field: Field used for the link's from node `model_node_id`. Defaults to "A".
        to_field: Field used for the link's to node `model_node_id`. Defaults to "B".
        crs (int, optional): Coordinate reference system. SHouldn't be changed unless you know
            what you are doing. Defaults to 4326 which is WGS84 lat/long.

    Returns:
        gpd.GeoDataFrame: _description_
    """
    from ..utils.geo import linestring_from_lats_lons

    if ref_nodes_df is not None:
        shapes = update_shapes_geometry(shapes, ref_nodes_df)
    tr_links = unique_shape_links(shapes, from_field=from_field, to_field=to_field)
    # WranglerLogger.debug(f"tr_links :\n{tr_links }")

    geometry = linestring_from_lats_lons(
        tr_links,
        [f"shape_pt_lat_{from_field}", f"shape_pt_lat_{to_field}"],
        [f"shape_pt_lon_{from_field}", f"shape_pt_lon_{to_field}"],
    )
    # WranglerLogger.debug(f"geometry\n{geometry}")
    shapes_gdf = gpd.GeoDataFrame(tr_links, geometry=geometry, crs=crs)
    return shapes_gdf


def stop_times_to_stop_time_points_gdf(
    stop_times: "WranglerStopTimesTable",
    stops: "WranglerStopsTable",
    ref_nodes_df: pd.DataFrame = None,
) -> gpd.GeoDataFrame:
    """Stoptimes geodataframe as points using geometry from stops.txt or optionally another df.

    Args:
        stop_times (WranglerStopTimesTable): Feed stop times table.
        stops (WranglerStopsTable): Feed stops table.
        ref_nodes_df (pd.DataFrame, optional): If specified, will use geometry from these nodes.  
            Otherwise, will use geometry in shapes file. Defaults to None.
    """
    if ref_nodes_df is not None:
        stops = update_stops_geometry(stops, ref_nodes_df)

    stop_times_geo = stop_times.merge(
        stops[["stop_id", "stop_lat", "stop_lon"]],
        right_on="stop_id",
        left_on="stop_id",
        how="left",
    )
    return gpd.GeoDataFrame(
        stop_times_geo,
        geometry=gpd.points_from_xy(
            stop_times_geo["stop_lon"], stop_times_geo["stop_lat"]
        ),
        crs=4326,
    )


def stop_times_to_stop_time_links_gdf(
    stop_times: "WranglerStopTimesTable",
    stops: "WranglerStopsTable",
    ref_nodes_df: pd.DataFrame = None,
    from_field: str = "A",
    to_field: str = "B"
) -> gpd.GeoDataFrame:
    """Stop times geodataframe as links using geometry from stops.txt or optionally another df.

    Args:
        stop_times (WranglerStopTimesTable): Feed stop times table.
        stops (WranglerStopsTable): Feed stops table.
        ref_nodes_df (pd.DataFrame, optional): If specified, will use geometry from these nodes.
            Otherwise, will use geometry in shapes file. Defaults to None.
        from_field: Field used for the link's from node `model_node_id`. Defaults to "A".
        to_field: Field used for the link's to node `model_node_id`. Defaults to "B".
    """
    from ..utils.geo import linestring_from_lats_lons

    if ref_nodes_df is not None:
        stops = update_stops_geometry(stops, ref_nodes_df)

    lat_fields = []
    lon_fields = []
    tr_links = unique_stop_time_links(
        stop_times, from_field=from_field, to_field=to_field
    )
    for f in (from_field, to_field):
        tr_links = tr_links.merge(
            stops[["stop_id", "stop_lat", "stop_lon"]],
            right_on="stop_id",
            left_on=f,
            how="left",
        )
        lon_f = f"{f}_X"
        lat_f = f"{f}_Y"
        tr_links = tr_links.rename(columns={"stop_lon": "X", "stop_lat": "Y"})
        lon_fields.append(lon_f)
        lat_fields.append(lat_f)

    geometry = linestring_from_lats_lons(tr_links, lat_fields, lon_fields)
    return gpd.GeoDataFrame(tr_links, geometry=geometry)

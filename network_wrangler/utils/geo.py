import math

from typing import List, Tuple, Union
from pathlib import Path

import pandas as pd
import geopandas as gpd
from pyproj import Proj, Transformer
from shapely.geometry import LineString, Point
from shapely.ops import transform
from geographiclib.geodesic import Geodesic

from ..logger import WranglerLogger
from ..models._base.geo import LatLongCoordinates
from ..models.roadway.types import LocationReference, LocationReferences


# key:value (from espg, to espg): pyproj transform object
transformers = {}


def get_bearing(lat1, lon1, lat2, lon2):
    """
    calculate the bearing (forward azimuth) b/w the two points

    returns: bearing in radians
    """
    # bearing in degrees
    brng = Geodesic.WGS84.Inverse(lat1, lon1, lat2, lon2)["azi1"]

    # convert bearing to radians
    brng = math.radians(brng)

    return brng


def offset_point_with_distance_and_bearing(
    lon: float, lat: float, distance: float, bearing: float
) -> List[float]:
    """
    Get the new lon-lat (in degrees) given current point (lon-lat), distance and bearing

    args:
        lon: longitude of original point
        lat: latitude of original point
        distance: distance in meters to offset point by
        bearing: direction to offset point to in radians

    returns: list of new offset lon-lat
    """
    # Earth's radius in meters
    radius = 6378137

    # convert the lat long from degree to radians
    lat_radians = math.radians(lat)
    lon_radians = math.radians(lon)

    # calculate the new lat long in radians
    out_lat_radians = math.asin(
        math.sin(lat_radians) * math.cos(distance / radius)
        + math.cos(lat_radians) * math.sin(distance / radius) * math.cos(bearing)
    )

    out_lon_radians = lon_radians + math.atan2(
        math.sin(bearing) * math.sin(distance / radius) * math.cos(lat_radians),
        math.cos(distance / radius) - math.sin(lat_radians) * math.sin(lat_radians),
    )
    # convert the new lat long back to degree
    out_lat = math.degrees(out_lat_radians)
    out_lon = math.degrees(out_lon_radians)

    return [out_lon, out_lat]


def haversine_distance(origin: list, destination: list) -> float:
    """
    Returns haversine distance in miles between the coordinates of two points in lat/lon.

    Args:
    origin: lat/lon for point A
    destination: lat/lon for point B

    Returns: string
    """

    lon1, lat1 = origin
    lon2, lat2 = destination
    radius = 6378137  # meter

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(
        math.radians(lat1)
    ) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) * math.sin(dlon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c  # meters
    d = d * 0.000621371  # miles

    return d


def length_of_linestring_miles(
    gdf: Union[gpd.GeoSeries, gpd.GeoDataFrame]
) -> pd.Series:
    """
    Returns a Series with the linestring length in miles.

    Args:
        gdf: GeoDataFrame with linestring geometry.  If given a GeoSeries will attempt to convert
            to a GeoDataFrame.
    """
    if isinstance(gdf, gpd.GeoSeries):
        gdf = gpd.GeoDataFrame(geometry=gdf)

    p_crs = gdf.estimate_utm_crs()
    gdf = gdf.to_crs(p_crs)
    METERS_IN_MILES = 1609.34
    return gdf.geometry.length / METERS_IN_MILES


def linestring_from_nodes(
    links_df: pd.DataFrame,
    nodes_df: gpd.GeoDataFrame,
    from_node: str = "A",
    to_node: str = "B",
    node_pk: str = "model_node_id",
) -> gpd.GeoSeries:
    """
    Creates a LineString geometry GeoSeries from a DataFrame of links and a DataFrame of nodes.

    Args:
        links_df: DataFrame with columns for from_node and to_node.
        nodes_df: GeoDataFrame with geometry column.
        from_node: column name in links_df for the from node. Defaults to "A".
        to_node: column name in links_df for the to node. Defaults to "B".
        node_pk: primary key column name in nodes_df. Defaults to "model_node_id".
    """
    assert "geometry" in nodes_df.columns, "nodes_df must have a 'geometry' column"

    # need to continuously reset the index to make sure the index is the same as the link index
    _link_idx = links_df.params.idx_col

    links_df = (
        links_df[[from_node, to_node]]
        .reset_index()
        .merge(
            nodes_df[[node_pk, "geometry"]],
            left_on=from_node,
            right_on=node_pk,
            how="left",
        )
        .set_index(_link_idx)
    )

    links_df = links_df.rename(columns={"geometry": "geometry_A"})

    links_df = (
        links_df.reset_index()
        .merge(
            nodes_df[[node_pk, "geometry"]],
            left_on=to_node,
            right_on=node_pk,
            how="left",
        )
        .set_index(_link_idx)
    )

    links_df = links_df.rename(columns={"geometry": "geometry_B"})

    # makes sure all nodes exist
    _missing_geo_links_df = links_df[
        links_df["geometry_A"].isnull() | links_df["geometry_B"].isnull()
    ]
    if not _missing_geo_links_df.empty:
        missing_nodes = _missing_geo_links_df[[from_node, to_node]].values
        raise ValueError(f"Missing from/to nodes in nodes_df: {missing_nodes}")

    # create geometry from points
    links_df["geometry"] = links_df.apply(
        lambda row: LineString([row["geometry_A"], row["geometry_B"]]), axis=1
    )

    # convert to GeoDataFrame
    links_gdf = gpd.GeoDataFrame(links_df, geometry="geometry")

    return links_gdf["geometry"]


def linestring_from_lats_lons(df, lat_fields, lon_fields) -> gpd.GeoSeries:
    """
    Create a LineString geometry from a DataFrame with lon/lat fields.

    Args:
        df: DataFrame with columns for lon/lat fields.
        lat_fields: list of column names for the lat fields.
        lon_fields: list of column names for the lon fields.
    """
    if len(lon_fields) != len(lat_fields):
        raise ValueError("lon_fields and lat_fields must have the same length")

    line_geometries = gpd.GeoSeries(
        [
            LineString(
                [(row[lon], row[lat]) for lon, lat in zip(lon_fields, lat_fields)]
            )
            for _, row in df.iterrows()
        ]
    )

    return gpd.GeoSeries(line_geometries)


def point_from_xy(x, y, xy_crs: int = 4326, point_crs: int = 4326):
    """
    Creates a point geometry from x and y coordinates.

    Args:
        x: x coordinate, in xy_crs
        y: y coordinate, in xy_crs
        xy_crs: coordinate reference system in ESPG code for x/y inputs. Defaults to 4326 (WGS84)
        point_crs: coordinate reference system in ESPG code for point output.
            Defaults to 4326 (WGS84)

    Returns: Shapely Point in point_crs
    """

    point = Point(x, y)

    if xy_crs == point_crs:
        return point

    if (xy_crs, point_crs) not in transformers:
        # store transformers in dictionary because they are an "expensive" operation
        transformers[(xy_crs, point_crs)] = Transformer.from_proj(
            Proj(init="epsg:" + str(xy_crs)),
            Proj(init="epsg:" + str(point_crs)),
            always_xy=True,  # required b/c Proj v6+ uses lon/lat instead of x/y
        )

    return transform(transformers[(xy_crs, point_crs)].transform, point)


def update_points_in_linestring(
    linestring: LineString, updated_coords: List[float], position: int
):
    """Replaces a point in a linestring with a new point.

    Args:
        linestring (LineString): original_linestring
        updated_coords (List[float]): updated poimt coordinates
        position (int): position in the linestring to update
    """
    coords = [c for c in linestring.coords]
    coords[position] = updated_coords
    return LineString(coords)


def update_nodes_in_linestring_geometry(
    original_df: gpd.GeoDataFrame,
    updated_nodes_df: gpd.GeoDataFrame,
    position: int,
) -> gpd.GeoSeries:
    """
    Updates the nodes in a linestring geometry and returns updated geometry.

    Args:
        original_df: GeoDataFrame with the node primary key and linestring geometry
        updated_nodes_df: GeoDataFrame with updated node geometries.
        position: position in the linestring to update with the node.
    """
    nodes_pk = updated_nodes_df.params.primary_key
    orig_pk = original_df.params.primary_key

    updated_df = original_df.merge(
        updated_nodes_df[["geometry"]],
        left_on=nodes_pk,
        right_index=True,
        suffixes=("", "_node"),
    )

    updated_df["geometry"] = updated_df.apply(
        lambda row: update_points_in_linestring(
            row["geometry"], row["geometry_node"].coords[0], position
        ),
        axis=1,
    )

    updated_df = updated_df.set_index(orig_pk)

    # WranglerLogger.debug(f"updated_df - AFTER: \n {updated_df}")
    return updated_df["geometry"]


def get_point_geometry_from_linestring(polyline_geometry, pos: int = 0):
    # WranglerLogger.debug(
    #    f"get_point_geometry_from_linestring.polyline_geometry.coords[0]: \
    #    {polyline_geometry.coords[0]}."
    # )

    # Note: when upgrading to shapely 2.0, will need to use following command
    # _point_coords = get_coordinates(polyline_geometry).tolist()[pos]
    return point_from_xy(*polyline_geometry.coords[pos])


def location_ref_from_point(
    geometry: Point,
    sequence: int = 1,
    bearing: float = None,
    distance_to_next_ref: float = None,
) -> LocationReference:
    """Generates a shared street point location reference.

    Args:
        geometry (Point): Point shapely geometry
        sequence (int, optional): Sequence if part of polyline. Defaults to None.
        bearing (float, optional): Direction of line if part of polyline. Defaults to None.
        distance_to_next_ref (float, optional): Distnce to next point if part of polyline.
            Defaults to None.

    Returns:
        LocationReference: As defined by sharedStreets.io schema
    """
    lr = {
        "point": LatLongCoordinates(geometry.coords[0]),
    }

    for arg in ["sequence", "bearing", "distance_to_next_ref"]:
        if locals()[arg] is not None:
            lr[arg] = locals()[arg]

    return LocationReference(**lr)


def location_refs_from_linestring(geometry: LineString) -> LocationReferences:
    """Generates a shared street location reference from linestring.

    Args:
        geometry (LineString): Shapely LineString instance

    Returns:
        LocationReferences: As defined by sharedStreets.io schema
    """
    return LocationReferences(
        [
            location_ref_from_point(
                point,
                sequence=i + 1,
                distance_to_next_ref=point.distance(geometry.coords[i + 1]),
                bearing=get_bearing(*point.coords[0], *geometry.coords[i + 1]),
            )
            for i, point in enumerate(geometry.coords[:-1])
        ]
    )


def get_bounding_polygon(
    boundary_geocode: Union[str, dict] = None,
    boundary_file: Union[str, Path] = None,
    boundary_gdf: gpd.GeoDataFrame = None,
    crs: int = 4326,  # WGS84
) -> gpd.GeoSeries:
    """Get the bounding polygon for a given boundary first prioritizing the

    This function retrieves the bounding polygon for a given boundary. The boundary can be provided
    as a GeoDataFrame, a geocode string or dictionary, or a boundary file. The resulting polygon
    geometry is returned as a GeoSeries.

    Args:
        boundary_geocode (Union[str, dict], optional): A geocode string or dictionary
            representing the boundary. Defaults to None.
        boundary_file (Union[str, Path], optional): A path to the boundary file. Only used if
            boundary_geocode is None. Defaults to None.
        boundary_gdf (gpd.GeoDataFrame, optional): A GeoDataFrame representing the boundary.
            Only used if boundary_geocode and boundary_file are None. Defaults to None.
        crs (int, optional): The coordinate reference system (CRS) code. Defaults to 4326 (WGS84).

    Returns:
        gpd.GeoSeries: The polygon geometry representing the bounding polygon.
    """

    import osmnx as ox

    if sum(x is not None for x in [boundary_gdf, boundary_geocode, boundary_file]) != 1:
        raise ValueError(
            "Exacly one of boundary_gdf, boundary_geocode, or boundary_shp must \
                         be provided"
        )

    OK_BOUNDARY_SUFF = [".shp", ".geojson", ".parquet"]

    if boundary_geocode is not None:
        boundary_gdf = ox.geocode_to_gdf(boundary_geocode)
    if boundary_file is not None:
        boundary_file = Path(boundary_file)
        if boundary_file.suffix not in OK_BOUNDARY_SUFF:
            raise ValueError(
                f"Boundary file must have one of the following suffixes: {OK_BOUNDARY_SUFF}"
            )
        if not boundary_file.exists():
            raise FileNotFoundError(f"Boundary file {boundary_file} does not exist")
        if boundary_file.suffix == ".parquet":
            boundary_gdf = gpd.read_parquet(boundary_file)
        else:
            boundary_gdf = gpd.read_file(boundary_file)
            if boundary_file.suffix == ".geojson":  # geojson standard is WGS84
                boundary_gdf.crs = crs

    if boundary_gdf.crs is not None:
        boundary_gdf = boundary_gdf.to_crs(crs)
    # make sure boundary_gdf is a polygon
    if len(boundary_gdf.geom_type[boundary_gdf.geom_type != "Polygon"]) > 0:
        raise ValueError("boundary_gdf must all be Polygons")
    # get the boundary as a single polygon
    boundary_gs = gpd.GeoSeries([boundary_gdf.geometry.unary_union], crs=crs)

    return boundary_gs

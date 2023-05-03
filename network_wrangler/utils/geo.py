import hashlib
import math

import pandas as pd
import geopandas as gpd

from pyproj import Proj, Transformer
from shapely.geometry import LineString, Point
from shapely.ops import transform
from geographiclib.geodesic import Geodesic

from typing import Collection, List, Tuple

from ..logger import WranglerLogger


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
) -> Tuple[float]:
    """
    Get the new lon-lat (in degrees) given current point (lon-lat), distance and bearing

    args:
        lon: longitude of original point
        lat: latitude of original point
        distance: distance in meters to offset point by
        bearing: direction to offset point to in radians

    returns: tuple of new offset lon-lat
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


def offset_location_reference(location_reference, offset_meters: float = 10):
    """
    Creates a new location reference line
    using the first and last nodes of given polyline location reference,
    offseting it by 90 degree to the bearing of given location reference
    and distance equals to offset_meters

    args:
        location_reference: existing location reference
        offset_meters: (Optional) meters to offset the existing

    returns: new location_reference with offset
    """
    lon_1 = location_reference[0]["point"][0]
    lat_1 = location_reference[0]["point"][1]
    lon_2 = location_reference[-1]["point"][0]
    lat_2 = location_reference[-1]["point"][1]

    bearing = get_bearing(lat_1, lon_1, lat_2, lon_2)
    # adding 90 degrees (1.57 radians) to the current bearing
    bearing = bearing + 1.57

    out_location_reference = [
        {
            "sequence": idx + 1,
            "point": offset_point_with_distance_and_bearing(
                lr["point"][0], lr["point"][1], offset_meters, bearing
            ),
        }
        for idx, lr in enumerate(location_reference)
    ]

    return out_location_reference


def meters_to_projected_distance(
    distance_meters: float, gdf: gpd.GeoDataFrame, meters_crs=26915
):
    """Find the specified distance in meters in the geodataframe's coordinate reference system.

    Uses the centroid of the geodataframe as a reference point. Might not be as accurate for
    geodataframes which span large areas.

    Currently a bit of a convoluted algorithm:
    1. use the centeroid of the gdf as the reference point
    2. transform it to a crs that uses meters
    3. create an offset point hat is `distance_meters` away
    4. translate offset point back to gdf_crs
    5. calculate distance between two points

    Args:
        distance_meters (float): desired distance in meters
        gdf (gpd.GeoDataFrame): geodataframe which has a CRS specified
        meters_crs: Espg code for  projected coordinate system which uses meters. Defaults to
            26915 which is NAD83 Zone 15N which is useful for North America.
    """

    gdf_crs = gdf.crs
    ref_point = gdf.geometry.centroid.iloc[0]
    t_gdf_to_meters = Transformer.from_crs(gdf_crs, meters_crs)
    ref_point_meters = Point(t_gdf_to_meters.transform(ref_point.x, ref_point.y))

    offset_point_meters = Point(
        ref_point_meters.x, ref_point_meters.y + distance_meters
    )

    t_meters_to_gdf = Transformer.from_crs(gdf_crs, meters_crs)
    offset_point = Point(t_meters_to_gdf(offset_point_meters.x, offset_point_meters.y))

    distance = ref_point.distance(offset_point)

    return distance


def haversine_distance(origin: list, destination: list) -> float:
    """
    Returns haversine distance between the coordinates of two points in lat/lon.

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


def location_reference_from_nodes(node_list: Collection[pd.Series]):
    """
    Creates a location references using a list of nodes coordinates

    Args:
        node_list: List of nodes represented as series or dicts with "X" and "Y" values.

    """

    out_location_references = [
        {"sequence": idx + 1, "point": [n["X"], n["Y"]]}
        for idx, n in enumerate(node_list)
    ]

    # WranglerLogger.debug(f"out_location_references: {out_location_references}")

    return out_location_references


def line_string_from_location_references(location_references: list):
    """
    Creates a geometry as a LineString using a list of location references.
    """
    # WranglerLogger.debug(
    #    f"line_string_from_location_references.location_references: {[lr['point'] for lr in\
    #  location_references]}."
    #    )
    return LineString([lr["point"] for lr in location_references])


# key:value (from espg, to espg): pyproj transform object
transformers = {}


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
    """Replaces

    Args:
        linestring (LineString): _description_
        point_coords (List[float]): _description_
        position (int): _description_
    """
    coords = [c for c in linestring.coords]
    coords[position] = updated_coords
    return LineString(coords)


def get_point_geometry_from_linestring(polyline_geometry, pos: int = 0):
    # WranglerLogger.debug(
    #    f"get_point_geometry_from_linestring.polyline_geometry.coords[0]: \
    #    {polyline_geometry.coords[0]}."
    # )

    # Note: when upgrading to shapely 2.0, will need to use following command
    # _point_coords = get_coordinates(polyline_geometry).tolist()[pos]
    return point_from_xy(*polyline_geometry.coords[pos])


def create_unique_shape_id(line_string: LineString):
    """
    Creates a unique hash id using the coordinates of the geometry using first and last locations.

    Args:
    line_string: Line Geometry as a LineString

    Returns: string
    """

    x1, y1 = list(line_string.coords)[0]  # first coordinate (A node)
    x2, y2 = list(line_string.coords)[-1]  # last coordinate (B node)

    message = "Geometry {} {} {} {}".format(x1, y1, x2, y2)
    unhashed = message.encode("utf-8")
    hash = hashlib.md5(unhashed).hexdigest()

    return hash

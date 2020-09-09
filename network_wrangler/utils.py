import os
import copy
import hashlib
import math
from typing import Union

from shapely.geometry import LineString

import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
from geographiclib.geodesic import Geodesic

from .logger import WranglerLogger


def point_df_to_geojson(df: pd.DataFrame, properties: list):
    """
    Author: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/
    """
    from .roadwaynetwork import RoadwayNetwork

    geojson = {"type": "FeatureCollection", "features": []}
    for _, row in df.iterrows():
        feature = {
            "type": "Feature",
            "properties": {},
            "geometry": {"type": "Point", "coordinates": []},
        }
        feature["geometry"]["coordinates"] = [row["geometry"].x, row["geometry"].y]
        feature["properties"][RoadwayNetwork.NODE_FOREIGN_KEY] = row.name
        for prop in properties:
            feature["properties"][prop] = row[prop]
        geojson["features"].append(feature)
    return geojson


def link_df_to_json(df: pd.DataFrame, properties: list):
    """ Export pandas dataframe as a json object.

    Modified from: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/

    Args:
        df: Dataframe to export
        properties: list of properties to export
    """

    # can't remember why we need this?
    if "distance" in properties:
        df["distance"].fillna(0)

    json = []
    for _, row in df.iterrows():
        feature = {}
        for prop in properties:
            feature[prop] = row[prop]
        json.append(feature)

    return json


def topological_sort(adjacency_list, visited_list):
    """
    Topological sorting for Acyclic Directed Graph
    """

    output_stack = []

    def _topology_sort_util(vertex):
        if not visited_list[vertex]:
            visited_list[vertex] = True
            for neighbor in adjacency_list[vertex]:
                _topology_sort_util(neighbor)
            output_stack.insert(0, vertex)

    for vertex in visited_list:
        _topology_sort_util(vertex)

    return output_stack


def make_slug(text, delimiter: str = "_"):
    """
    makes a slug from text
    """
    import re

    text = re.sub("[,.;@#?!&$']+", "", text.lower())
    return re.sub("[\ ]+", delimiter, text)


def parse_time_spans(times):
    """
    parse time spans into tuples of seconds from midnight
    can also be used as an apply function for a pandas series
    Parameters
    -----------
    times: tuple(string) or tuple(int) or list(string) or list(int)

    returns
    --------
    tuple(integer)
      time span as seconds from midnight
    """
    try:
        start_time, end_time = times
    except:
        msg = "ERROR: times should be a tuple or list of two, got: {}".format(times)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    # If times are strings, convert to int in seconds, else return as ints
    if isinstance(start_time, str) and isinstance(end_time, str):
        start_time = start_time.strip()
        end_time = end_time.strip()

        # If time is given without seconds, add 00
        if len(start_time) <= 5:
            start_time += ":00"
        if len(end_time) <= 5:
            end_time += ":00"

        # Convert times to seconds from midnight (Partride's time storage)
        h0, m0, s0 = start_time.split(":")
        start_time_sec = int(h0) * 3600 + int(m0) * 60 + int(s0)

        h1, m1, s1 = end_time.split(":")
        end_time_sec = int(h1) * 3600 + int(m1) * 60 + int(s1)

        return (start_time_sec, end_time_sec)

    elif isinstance(start_time, int) and isinstance(end_time, int):
        return times

    else:
        WranglerLogger.error("ERROR: times should be ints or strings")
        raise ValueError()

    return (start_time_sec, end_time_sec)


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


def offset_point_with_distance_and_bearing(lat, lon, distance, bearing):
    """
    Get the new lat long (in degrees) given current point (lat/lon), distance and bearing

    returns: new lat/long
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

    return (out_lat, out_lon)


def offset_location_reference(location_reference, offset_meters=10):
    """
    Creates a new location reference
    using the node a and node b of given location reference,
    offseting it by 90 degree to the bearing of given location reference
    and distance equals to offset_meters

    returns: new location_reference with offset
    """
    lon_1 = location_reference[0]["point"][0]
    lat_1 = location_reference[0]["point"][1]
    lon_2 = location_reference[1]["point"][0]
    lat_2 = location_reference[1]["point"][1]

    bearing = get_bearing(lat_1, lon_1, lat_2, lon_2)
    # adding 90 degrees (1.57 radians) to the current bearing
    bearing = bearing + 1.57

    new_lat_1, new_lon_1 = offset_point_with_distance_and_bearing(
        lat_1, lon_1, offset_meters, bearing
    )
    new_lat_2, new_lon_2 = offset_point_with_distance_and_bearing(
        lat_2, lon_2, offset_meters, bearing
    )

    out_location_reference = [
        {"sequence": 1, "point": [new_lon_1, new_lat_1]},
        {"sequence": 2, "point": [new_lon_2, new_lat_2]},
    ]

    return out_location_reference


def haversine_distance(origin: list, destination: list):
    """
    Calculates haversine distance between two points

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


def create_unique_shape_id(line_string: LineString):
    """
    Creates a unique hash id using the coordinates of the geomtery

    Args:
    line_string: Line Geometry as a LineString

    Returns: string
    """

    x1, y1 = list(line_string.coords)[0]  # first co-ordinate (A node)
    x2, y2 = list(line_string.coords)[-1]  # last co-ordinate (B node)

    message = "Geometry {} {} {} {}".format(x1, y1, x2, y2)
    unhashed = message.encode("utf-8")
    hash = hashlib.md5(unhashed).hexdigest()

    return hash


def create_location_reference_from_nodes(node_a, node_b):
    """
    Creates a location reference using the node a and node b coordinates

    Args:
    node_a: Node A as Series
    node_b: Node B as Series

    """
    out_location_reference = [
        {"sequence": 1, "point": [node_a["X"], node_a["Y"]]},
        {"sequence": 2, "point": [node_b["X"], node_b["Y"]]},
    ]

    return out_location_reference


def create_line_string(location_reference: list):
    """
    Creates a geometry as a LineString using location reference
    """

    return LineString([location_reference[0]["point"], location_reference[1]["point"]])

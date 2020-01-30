import pandas as pd
from .Logger import WranglerLogger
import math
import copy


def point_df_to_geojson(df: pd.DataFrame, properties: list):
    """
    Author: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/
    """
    from .RoadwayNetwork import RoadwayNetwork

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
    """
    Modified from: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/
    """
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

    def topology_sort_util(vertex):
        if not visited_list[vertex]:
            visited_list[vertex] = True
            for neighbor in adjacency_list[vertex]:
                topology_sort_util(neighbor)
            output_stack.insert(0, vertex)

    for vertex in visited_list:
        topology_sort_util(vertex)

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


def offset_lat_lon(lon_lat_point: list, offset_meters=100):
    in_lon = lon_lat_point[0]
    in_lat = lon_lat_point[1]

    # Earth's radius
    radius = 6378137

    # offset in radians
    offset_lat_radians = offset_meters / radius
    offset_lon_radians = offset_meters / (radius * math.cos(math.pi * in_lat / 180))

    # offset lat lon
    latO = in_lat + offset_lat_radians * 180 / math.pi
    lonO = in_lon + offset_lon_radians * 180 / math.pi

    return [lonO, latO]


def haversine_distance(origin: list, destination: list):
    lon1, lat1 = origin
    lon2, lat2 = destination
    radius = 6378137  # meter

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(
        math.radians(lat1)
    ) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) * math.sin(dlon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c

    return d

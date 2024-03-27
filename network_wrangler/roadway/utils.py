import hashlib

from typing import List, Union
from pathlib import Path
import pandas as pd
import geopandas as gpd

from shapely import LineString

from ..logger import WranglerLogger
from ..utils import diff_dfs


def compare_networks(
    nets: List[Union["RoadwayNetwork", "ModelRoadwayNetwork"]],
    names: List[str] = None,
) -> pd.DataFrame:
    if names is None:
        names = ["net" + str(i) for i in range(1, len(nets) + 1)]
    df = pd.DataFrame({name: net.summary for name, net in zip(names, nets)})
    return df


def compare_links(
    links: List[pd.DataFrame],
    names: List[str] = None,
) -> pd.DataFrame:
    if names is None:
        names = ["links" + str(i) for i in range(1, len(links) + 1)]
    df = pd.DataFrame({name: link.of_type.summary for name, link in zip(names, links)})
    return df


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


def diff_nets(net1, net2) -> bool:
    # Need to ignore b/c there are tiny diffrences in how this complex time is serialized and
    # in order to evaluate if they are equivelant you need to do an elemement by element comparison
    # which takes forever.
    IGNORE_COLS = ["locationReferences"]
    WranglerLogger.debug("Comparing networks.")
    WranglerLogger.info("----Comparing links----")
    diff_links = diff_dfs(net1.links_df, net2.links_df, ignore=IGNORE_COLS)
    WranglerLogger.info("----Comparing nodes----")
    diff_nodes = diff_dfs(net1.nodes_df, net2.nodes_df, ignore=IGNORE_COLS)
    WranglerLogger.info("----Comparing shapes----")
    diff_shapes = diff_dfs(net1.shapes_df, net2.shapes_df)
    diff = any([diff_links, diff_nodes, diff_shapes])
    if diff:
        WranglerLogger.error("!!! Differences in networks.")
    else:
        WranglerLogger.info("Networks same for properties in common")
    return diff

"""Utility functions for RoadwayNetwork and ModelRoadwayNetwork classes."""

from __future__ import annotations
import hashlib

from typing import List, Union, TYPE_CHECKING, Optional
import pandas as pd

from ..logger import WranglerLogger
from ..utils.data import diff_dfs

if TYPE_CHECKING:
    from shapely import LineString
    from .network import RoadwayNetwork
    from .model_roadway import ModelRoadwayNetwork


def compare_networks(
    nets: List[Union["RoadwayNetwork", "ModelRoadwayNetwork"]],
    names: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Compare the summary of networks in a list of networks.

    Args:
        nets: list of networks
        names: list of names for the networks
    """
    if names is None:
        names = ["net" + str(i) for i in range(1, len(nets) + 1)]
    df = pd.DataFrame({name: net.summary for name, net in zip(names, nets)})
    return df


def compare_links(
    links: List[pd.DataFrame],
    names: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Compare the summary of links in a list of dataframes.

    Args:
        links: list of dataframes
        names: list of names for the dataframes
    """
    if names is None:
        names = ["links" + str(i) for i in range(1, len(links) + 1)]
    df = pd.DataFrame({name: link.of_type.summary for name, link in zip(names, links)})
    return df


def create_unique_shape_id(line_string: LineString):
    """A unique hash id using the coordinates of the geometry using first and last locations.

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


def diff_nets(net1: RoadwayNetwork, net2: RoadwayNetwork) -> bool:
    """Diff two RoadwayNetworks and return True if they are different.

    Ignore locationReferences as they are not used in the network.

    Args:
        net1 (RoadwayNetwork): First network to compare
        net2 (RoadwayNetwork): Second network to compare
    """
    # Need to ignore b/c there are tiny differences in how this complex time is serialized and
    # in order to evaluate if they are equivelant you need to do an elemement by element comparison
    # which takes forever.
    IGNORE_COLS = ["locationReferences"]
    WranglerLogger.debug("Comparing networks.")
    WranglerLogger.info("----Comparing links----")
    diff_links = diff_dfs(net1.links_df, net2.links_df, ignore=IGNORE_COLS)
    WranglerLogger.info("----Comparing nodes----")
    diff_nodes = diff_dfs(net1.nodes_df, net2.nodes_df, ignore=IGNORE_COLS)
    WranglerLogger.info("----Comparing shapes----")
    if net1.shapes_df is None and net1.shapes_df.empty:
        diff_shapes = False
    else:
        diff_shapes = diff_dfs(net1.shapes_df, net2.shapes_df, ignore=IGNORE_COLS)
    diff = any([diff_links, diff_nodes, diff_shapes])
    if diff:
        WranglerLogger.error("!!! Differences in networks.")
    else:
        WranglerLogger.info("Networks same for properties in common")
    return diff


def set_df_index_to_pk(df: pd.DataFrame) -> pd.DataFrame:
    """Sets the index of the dataframe to be a copy of the primary key.

    Args:
        df (pd.DataFrame): data frame to set the index of
    """
    if df.index.name != df.params.idx_col:
        df[df.params.idx_col] = df[df.params.primary_key]
        df = df.set_index(df.params.idx_col)
    return df

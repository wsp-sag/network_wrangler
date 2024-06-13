"""Utilities for validating a RoadLinksTable beyond its data model."""

import pandas as pd

from ...logger import WranglerLogger
from ...utils.data import fk_in_pk


class NodesInLinksMissingError(Exception):
    """Raised when there is an issue with validating links and nodes."""

    pass


def validate_links_have_nodes(links_df: pd.DataFrame, nodes_df: pd.DataFrame) -> bool:
    """Checks if links have nodes and returns a boolean.

    raises: ValueError if nodes_df is missing and A or B node
    """
    nodes_in_links = list(set(links_df["A"]).union(set(links_df["B"])))

    fk_valid, fk_missing = fk_in_pk(nodes_df.index, nodes_in_links)
    if not fk_valid:
        WranglerLogger.error(f"Nodes missing from links: {fk_missing}")
        raise NodesInLinksMissingError(f"Links are missing these nodes: {fk_missing}")
    return True

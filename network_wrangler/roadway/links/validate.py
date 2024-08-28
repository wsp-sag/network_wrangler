"""Utilities for validating a RoadLinksTable beyond its data model."""

from pathlib import Path

import pandas as pd

from ...logger import WranglerLogger
from ...utils.data import fk_in_pk


class NodesInLinksMissingError(Exception):
    """Raised when there is an issue with validating links and nodes."""

    pass


def validate_links_have_nodes(links_df: pd.DataFrame, nodes_df: pd.DataFrame) -> bool:
    """Checks if links have nodes and returns a boolean.

    raises: NodesInLinksMissingError if nodes_df is missing and A or B node
    """
    nodes_in_links = list(set(links_df["A"]).union(set(links_df["B"])))

    fk_valid, fk_missing = fk_in_pk(nodes_df.index, nodes_in_links)
    if not fk_valid:
        WranglerLogger.error(f"Nodes missing from links: {fk_missing}")
        raise NodesInLinksMissingError(f"Links are missing these nodes: {fk_missing}")
    return True


def validate_links_file(
    links_filename: Path,
    nodes_df: pd.DataFrame = None,
    strict: bool = False,
    errors_filename: Path = "link_errors.csv",
) -> bool:
    """Validates a links file to RoadLinksTable and optionally checks if nodes are in the links.

    Args:
        links_filename (Path): The links file.
        nodes_df (pd.DataFrame): The nodes dataframe. Defaults to None.
        strict (bool): If True, will validate to links_df without trying to parse it first.
        errors_filename (Path): The output file for the validation errors. Defaults
            to "link_errors.csv".

    Returns:
        bool: True if the links file is valid.
    """
    links_df = pd.read_csv(links_filename)
    return validate_links_df(
        links_df, nodes_df=nodes_df, strict=strict, errors_filename=errors_filename
    )


def validate_links_df(
    links_df: pd.DataFrame,
    nodes_df: pd.DataFrame = None,
    strict: bool = False,
    errors_filename: Path = "link_errors.csv",
) -> bool:
    """Validates a links df to RoadLinksTable and optionally checks if nodes are in the links.

    Args:
        links_df (pd.DataFrame): The links dataframe.
        nodes_df (pd.DataFrame): The nodes dataframe. Defaults to None.
        strict (bool): If True, will validate to links_df without trying to parse it first.
        errors_filename (Path): The output file for the validation errors. Defaults
            to "link_errors.csv".

    Returns:
        bool: True if the links dataframe is valid.
    """
    from ...utils.models import validate_df_to_model, TableValidationError
    from ...models.roadway.tables import RoadLinksTable

    is_valid = True

    if not strict:
        from .create import data_to_links_df

        try:
            links_df = data_to_links_df(links_df)
        except Exception as e:
            WranglerLogger.error(f"!!! [Links invalid] - Failed to parse links_df\n{e}")
            is_valid = False

    try:
        validate_df_to_model(links_df, RoadLinksTable, output_file=errors_filename)
    except TableValidationError as e:
        WranglerLogger.error(f"!!! [Links invalid] - Failed Schema validation\n{e}")
        is_valid = False

    try:
        validate_links_have_nodes(links_df, nodes_df)
    except NodesInLinksMissingError as e:
        WranglerLogger.error(f"!!! [Links invalid] - Nodes missing in links\n{e}")
        is_valid = False
    return is_valid

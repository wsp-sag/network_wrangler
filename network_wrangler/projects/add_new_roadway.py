import copy
from typing import Collection

import geopandas as gpd
import pandas as pd

from ..logger import WranglerLogger
from ..utils import (
    point_from_xy,
    line_string_from_location_references,
    create_unique_shape_id,
)


def apply_new_roadway(
    roadway_net: "RoadwayNetwork",
    add_links: Collection[dict] = [],
    add_nodes: Collection[dict] = [],
) -> None:
    """
    Add the new roadway features defined in the project card.

    New shapes are also added for the new roadway links.

    New nodes are added first so that links can refer to any added nodes.

    args:
        roadway_net: input RoadwayNetwork to apply change to
        add_links: list of dictionaries
        add_nodes: list of dictionaries

    returns: updated network with new links and nodes and associated geometries

    .. todo:: validate links and nodes dictionary
    """
    WranglerLogger.debug(
        f"Adding New Roadway Features:\n-Links:\n{add_links}\n-Nodes:\n{add_nodes}"
    )
    if add_nodes:
        _new_nodes_df = _create_nodes(roadway_net, add_nodes)
        roadway_net.nodes_df = pd.concat([roadway_net.nodes_df, _new_nodes_df])

    if add_links:
        _new_links_df = _create_links(roadway_net, add_links)
        roadway_net.links_df = pd.concat([roadway_net.links_df, _new_links_df])

        _new_shapes_df = roadway_net._create_new_shapes_from_links(_new_links_df)
        roadway_net.shapes_df = pd.concat([roadway_net.shapes_df, _new_shapes_df])
    return roadway_net


def _create_links(roadway_net: "RoadwayNetwork", new_links: Collection[dict] = []):
    new_links_df = pd.DataFrame(new_links)

    _idx_c = roadway_net.links_df.params.primary_key + "_idx"
    new_links_df[_idx_c] = new_links_df[roadway_net.links_df.params.primary_key]
    new_links_df.set_index(_idx_c, inplace=True)
    new_links_df = roadway_net.coerce_types(new_links_df)

    new_links_df = roadway_net._create_new_link_geometry(new_links_df)

    WranglerLogger.debug(
        f"New Links:\n{new_links_df[[roadway_net.links_df.params.primary_key,'name']]}"
    )
    assert new_links_valid(roadway_net, new_links_df)
    return new_links_df


def _create_nodes(roadway_net: "RoadwayNetwork", new_nodes: Collection[dict] = []):
    new_nodes_df = pd.DataFrame(new_nodes)

    _idx_c = roadway_net.nodes_df.params_primary_key + "_idx"
    new_nodes_df[_idx_c] = new_nodes_df[roadway_net.nodes_df.params_primary_key]
    new_nodes_df.set_index(_idx_c, inplace=True)
    new_nodes_df = roadway_net.coerce_types(new_nodes_df)

    new_nodes_df["geometry"] = new_nodes_df.apply(
        lambda x: point_from_xy(
            x["X"],
            x["Y"],
            xy_crs=roadway_net.CRS,
            point_crs=roadway_net.CRS,
        ),
        axis=1,
    )

    new_nodes_df = gpd.GeoDataFrame(new_nodes_df)
    WranglerLogger.debug(f"New Nodes:\n{new_nodes_df}")

    assert new_nodes_valid(roadway_net, new_nodes_df)
    return new_nodes_df


def new_nodes_valid(roadway_net: "RoadwayNetwork", new_nodes_df: pd.DataFrame) -> bool:
    # Check to see if same node is already in the network
    _existing_nodes = new_nodes_df[roadway_net.nodes_df.params.primary_key].apply(
        roadway_net.has_node
    )
    if _existing_nodes.any():
        msg = f"Node already exists between nodes:\n {new_nodes_df[_existing_nodes,roadway_net.nodes_df.params.primary_key]}."
        raise ValueError(msg)

    # Check to see if there are missing required columns
    _missing_cols = [
        c
        for c in roadway_net.MIN_NODE_REQUIRED_PROPS_DEFAULT
        if c not in new_nodes_df.columns
    ]
    if _missing_cols:
        msg = f"Missing required link properties:{_missing_cols}"
        raise ValueError(msg)

    # Check to see if there are missing required values
    _missing_values = new_nodes_df[roadway_net.MIN_NODE_REQUIRED_PROPS_DEFAULT].isna()
    if _missing_values.any().any():
        msg = f"Missing values for required node properties:\n{new_nodes_df.loc[_missing_values]}"
        WranglerLogger.Warning(msg)

    return True


def new_links_valid(roadway_net: "RoadwayNetwork", new_links_df: pd.DataFrame) -> bool:
    """Assesses if a set of links are valid for adding to self.links_df.

    Will produce a ValueError if new_links_df:
    1. A-B combinations are not unique within new_links_df
    2. UNIQUE_LINK_KEY is not unique within new_links_df
    3. A-B combinations overlap with an existing A-B link in self.links_df
    4. UNIQUE_LINK_KEY overlaps with an existing UNIQUE_LINK_ID in self.links_df
    5. A and B nodes are not in self.nodes_df
    6. Doesn't contain columns for MIN_LINK_REQUIRED_PROPS_DEFAULT

    Will produce a warning if there are NA values for any MIN_LINK_REQUIRED_PROPS_DEFAULT

    Args:
        new_links_df: dataframe of links being considered for addition to self.links_df

    Returns:
        bool: Returns a True if passes various validation tests.
    """

    # A-B combinations are unique within new_links_df
    _new_fk_id = pd.Series(
        zip(*[new_links_df[c] for c in roadway_net.links_df.params.fks_to_nodes])
    )
    if not _new_fk_id.is_unique:
        msg = f"Duplicate ABs in new links."
        raise ValueError(msg)

    # UNIQUE_LINK_ID is unique within new_links_df
    if not new_links_df[roadway_net.links_df.params.primary_key].is_unique:
        msg = f"Duplicate link IDs in new links."
        raise ValueError(msg)

    # Doesn't overlap with an existing A-B link in self.links_df
    _existing_links_ab = _new_fk_id.apply(roadway_net.has_link)
    if _existing_links_ab.any():
        msg = f"Link already exists between nodes:\n {_new_fk_id[_existing_links_ab]}."
        raise ValueError(msg)

    # Doesn't overlap with an existing UNIQUE_LINK_ID in self.links_df
    _ids = pd.concat(
        [
            roadway_net.links_df[roadway_net.links_df.params.primary_key],
            new_links_df[roadway_net.links_df.params.primary_key],
        ]
    )
    if not _ids.is_unique:
        msg = f"Link ID already exists:\n{_ids.loc[_ids.duplicated()]}."
        raise ValueError(msg)

    _fk_props = [
        roadway_net.links_df.params.primary_key.from_node,
        roadway_net.links_df.params.primary_key.to_node,
    ]

    # A and B nodes are in self.nodes_df
    for fk_prop in _fk_props:
        _has_node = new_links_df[fk_prop].apply(self.has_node)
        if not _has_node.all():
            if len(roadway_net.nodes_df) < 25:
                WranglerLogger.debug(f"self.nodes_df:\n{roadway_net.nodes_df}")
            msg = f"New link specifies non existant node {fk_prop} = {new_links_df.loc[_has_node,fk_prop]}."
            raise ValueError(msg)

    # Check to see if there are missing required columns
    _missing_cols = [
        c
        for c in roadway_net.MIN_LINK_REQUIRED_PROPS_DEFAULT
        if c not in new_links_df.columns
    ]
    if _missing_cols:
        msg = f"Missing required link properties:{_missing_cols}"
        raise ValueError(msg)

    # Check to see if there are missing required values
    _missing_values = new_links_df[roadway_net.MIN_LINK_REQUIRED_PROPS_DEFAULT].isna()
    if _missing_values.any().any():
        msg = f"Missing values for required link properties:\n{new_links_df.loc[_missing_values]}"
        WranglerLogger.Warning(msg)

    return True


def _create_new_link_geometry(
    roadway_net, new_links_df: pd.DataFrame
) -> gpd.GeoDataFrame:
    new_links_df = _add_link_geometry_from_nodes(roadway_net, new_links_df)
    new_links_df[roadway_net.UNIQUE_SHAPE_KEY] = new_links_df["geometry"].apply(
        create_unique_shape_id
    )
    return new_links_df


def _add_link_geometry_from_nodes(
    roadway_net, links_df: pd.DataFrame
) -> gpd.GeoDataFrame:
    links_df["locationReferences"] = roadway_net._create_link_locationreferences(
        links_df
    )
    links_df["geometry"] = links_df["locationReferences"].apply(
        line_string_from_location_references,
    )
    links_df = gpd.GeoDataFrame(links_df)
    return links_df


def _create_new_shapes_from_links(
    roadway_net, links_df: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    new_shapes_df = copy.deepcopy(
        links_df[[roadway_net.shapes_df.params.primary_key, "geometry"]]
    )
    return new_shapes_df

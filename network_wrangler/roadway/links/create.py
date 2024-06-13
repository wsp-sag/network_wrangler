"""Functions for creating RoadLinksTables."""

import time

from typing import List, Union

import geopandas as gpd
import pandas as pd

from pydantic import validate_call
from pandera.typing import DataFrame

from ..utils import set_df_index_to_pk

from ...logger import WranglerLogger
from ...models._base.validate import validate_df_to_model
from ...models.roadway.tables import RoadLinksTable, RoadNodesTable
from ...params import LinksParams, LAT_LON_CRS
from ..utils import create_unique_shape_id
from ...utils.data import coerce_gdf, attach_parameters_to_df
from ...utils.geo import (
    _harmonize_crs,
    length_of_linestring_miles,
    linestring_from_nodes,
)


class LinkCreationError(Exception):
    """Raised when there is an issue with creating links."""

    pass


@validate_call(config=dict(arbitrary_types_allowed=True))
def shape_id_from_link_geometry(
    links_df: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """Create a unique shape_id from the geometry of the link."""
    shape_ids = links_df["geometry"].apply(create_unique_shape_id)
    return shape_ids


def _fill_missing_link_geometries_from_nodes(
    links_df: pd.DataFrame, nodes_df: DataFrame[RoadNodesTable] = None
) -> gpd.GeoDataFrame:
    """Create location references and link geometries from nodes.

    If already has either, then will just fill NA.
    """
    if "geometry" not in links_df:
        links_df["geometry"] = None
    if links_df.geometry.isna().values.any():
        geo_start_t = time.time()
        if nodes_df is None:
            raise LinkCreationError("Must give nodes_df argument if don't have Geometry")
        # TODO FIX
        updated_links_df = linestring_from_nodes(links_df.loc[links_df.geometry.isna()], nodes_df)
        links_df.update(updated_links_df)
        # WranglerLogger.debug(f"links with updated geometries:\n{links_df}")
        WranglerLogger.debug(
            f"Created link geo from nodes in {round(time.time() - geo_start_t, 2)}."
        )
    return links_df


def _fill_missing_distance_from_geometry(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # WranglerLogger.debug(f"_fill_missing_distance_from_geometry.df input:\n{df}.")
    if "distance" not in df:
        df["distance"] = None
    if df["distance"].isna().values.any():
        df.loc[df["distance"].isna(), "distance"] = length_of_linestring_miles(
            df.loc[df["distance"].isna()]
        )
    return df


@validate_call(config=dict(arbitrary_types_allowed=True))
def data_to_links_df(
    links_df: Union[pd.DataFrame, List[dict]],
    in_crs: int = LAT_LON_CRS,
    links_params: Union[None, LinksParams] = None,
    nodes_df: Union[None, DataFrame[RoadNodesTable]] = None,
) -> DataFrame[RoadLinksTable]:
    """Create a links dataframe from list of link properties + link geometries or associated nodes.

    Sets index to be a copy of the primary key.
    Validates output dataframe using LinksSchema.

    Args:
        links_df (pd.DataFrame): df or list of dictionaries of link properties
        in_crs: coordinate reference system id for incoming links if geometry already exists.
            Defaults to 4326. Will convert everything to 4326 if it doesn't match.
        links_params: a LinkParams instance. Defaults to a default LinkParams instance..
        nodes_df: Associated notes geodataframe to use if geometries or location references not
            present. Defaults to None.

    Returns:
        pd.DataFrame: _description_
    """
    WranglerLogger.debug(f"Creating {len(links_df)} links.")
    if not isinstance(links_df, pd.DataFrame):
        links_df = pd.DataFrame(links_df)
    if len(links_df) < 21:
        WranglerLogger.debug(f"data_to_links_df.links_df input: \n{links_df}.")
    links_df = _fill_missing_link_geometries_from_nodes(links_df, nodes_df)
    # Now that have geometry, make sure is GDF
    links_df = coerce_gdf(links_df, in_crs=in_crs, geometry=links_df.geometry)

    links_df = _fill_missing_distance_from_geometry(links_df)

    links_df = _harmonize_crs(links_df, LAT_LON_CRS)
    nodes_df = _harmonize_crs(nodes_df, LAT_LON_CRS)

    links_params = LinksParams() if links_params is None else links_params
    links_df = attach_parameters_to_df(links_df, links_params)
    links_df = set_df_index_to_pk(links_df)

    # set dataframe-level variables
    links_df.gdf_name = "network_links"

    # Validate and coerce to schema
    links_df = validate_df_to_model(links_df, RoadLinksTable)
    assert "params" in links_df.__dict__

    if len(links_df) < 10:
        WranglerLogger.debug(
            f"New Links: \n{links_df[links_df.params.display_cols + ['geometry']]}"
        )
    else:
        WranglerLogger.debug(f"{len(links_df)} new links.")

    return links_df

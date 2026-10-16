"""Functions for creating nodes from data sources."""

import copy
import time

import geopandas as gpd
import pandas as pd
from pandera.typing import DataFrame
from pydantic import validate_call

from ...configs import DefaultConfig, WranglerConfig
from ...errors import NodeAddError
from ...logger import WranglerLogger
from ...models.roadway.tables import RoadLinksTable, RoadNodesAttrs, RoadNodesTable
from ...params import LAT_LON_CRS, SMALL_RECS, WEB_MERCATOR_CRS
from ...utils.geo import get_point_geometry_from_linestring, point_from_xy
from ...utils.models import validate_df_to_model
from ..utils import set_df_index_to_pk


def _create_node_geometries_from_xy(
    nodes_df: pd.DataFrame | list[dict],
    in_crs: int = LAT_LON_CRS,
    net_crs: int = LAT_LON_CRS,
) -> gpd.GeoDataFrame:
    """Fixes geometries in nodes_df if necessary using X and Y columns.

    Args:
        nodes_df: nodes dataframe to fix geometries in.
        in_crs: coordinate system that X and Y are in. Defaults to LAT_LON_CRS.
        net_crs: coordinate system to convert geometries to. Defaults to
            LAT_LON_CRS.

    Returns:
        gpd.GeoDataFrame: nodes dataframe with fixed geometries.
    """
    if not isinstance(nodes_df, pd.DataFrame):
        nodes_df = pd.DataFrame(nodes_df)
    if "X" not in nodes_df.columns or "Y" not in nodes_df.columns:
        msg = "Must have X and Y properties to create geometries from."
        raise NodeAddError(msg)

    geo_start_time = time.time()
    if "geometry" in nodes_df:
        mask = nodes_df["geometry"].isna()
        if mask.any():
            nodes_df.loc[mask, "geometry"] = nodes_df.loc[mask].apply(
                lambda x: point_from_xy(x["X"], x["Y"], xy_crs=in_crs, point_crs=net_crs),
                axis=1,
            )
        WranglerLogger.debug(
            f"Filled missing geometry from X and Y in {round(time.time() - geo_start_time, 2)}."
        )
        return nodes_df

    node_geometries = nodes_df.apply(
        lambda x: point_from_xy(x["X"], x["Y"], xy_crs=in_crs, point_crs=net_crs),
        axis=1,
    )
    WranglerLogger.debug(
        f"Created node geometries from X and Y in {round(time.time() - geo_start_time, 2)}."
    )
    nodes_gdf = gpd.GeoDataFrame(nodes_df, geometry=node_geometries, crs=in_crs)
    return nodes_gdf


@validate_call(config={"arbitrary_types_allowed": True})
def data_to_nodes_df(
    nodes_df: pd.DataFrame | gpd.GeoDataFrame | list[dict],
    config: WranglerConfig = DefaultConfig,  # noqa: ARG001
    in_crs: int = LAT_LON_CRS,
) -> DataFrame[RoadNodesTable]:
    """Turn nodes data into official nodes dataframe.

    Adds missing geometry.
    Makes sure X and Y are consistent with geometry GeoSeries.
    Converts to LAT_LON_CRS.
    Copies and sets idx to primary_key.
    Validates output to NodesSchema.

    Args:
        nodes_df : Nodes dataframe or list of dictionaries that can be converted to a dataframe.
        config: WranglerConfig instance. Defaults to DefaultConfig. NOTE: Not currently used.
        in_crs: Coordinate references system id incoming data xy is in, if it isn't already
            in a GeoDataFrame. Defaults to LAT_LON_CRS.

    Returns:
        gpd.GeoDataFrame: _description_
    """
    WranglerLogger.debug("Turning node data into official nodes_df")

    if isinstance(nodes_df, gpd.GeoDataFrame) and nodes_df.crs != LAT_LON_CRS:
        if nodes_df.crs is None:
            nodes_df.crs = in_crs
        nodes_df = nodes_df.to_crs(LAT_LON_CRS)

    if not isinstance(nodes_df, gpd.GeoDataFrame) or nodes_df.geometry.isnull().values.any():
        nodes_df = _create_node_geometries_from_xy(nodes_df, in_crs=in_crs, net_crs=LAT_LON_CRS)

    # Make sure values are consistent
    nodes_df["X"] = nodes_df["geometry"].apply(lambda g: g.x)
    nodes_df["Y"] = nodes_df["geometry"].apply(lambda g: g.y)

    if len(nodes_df) < SMALL_RECS:
        WranglerLogger.debug(f"nodes_df: \n{nodes_df[['model_node_id', 'geometry', 'X', 'Y']]}")

    # Validate and coerce to schema
    nodes_df = validate_df_to_model(nodes_df, RoadNodesTable)
    nodes_df.attrs.update(RoadNodesAttrs)
    nodes_df.gdf_name = nodes_df.attrs["name"]
    # lmz: why would we do this?
    nodes_df = set_df_index_to_pk(nodes_df)

    return nodes_df


def _create_nodes_from_link(
    links_df: DataFrame[RoadLinksTable],
    link_pos: int,
    node_key_field: str,
    merge_duplicates: bool = False,
) -> DataFrame[RoadNodesTable]:
    """Creates a basic list of node entries from links, their geometry, and a position.

    Useful for creating model network with simple geometry.
    TODO: Does not currently fill in additional values used in nodes.

    Args:
        links_df: subset of self.links_df or similar which needs nodes
            created
        link_pos (int): Position within geometry collection to use for geometry
        node_key_field (str): field name to use for generating index and node key
        merge_duplicates: if True, average the coordinates of any node entries that
            share a ``model_node_id`` instead of raising. Duplicates usually indicate
            an upstream data issue, so this is opt-in. Defaults to False.

    Returns:
        DataFrame[RoadNodesTable]
    """
    nodes_df = copy.deepcopy(links_df[[node_key_field, "geometry"]].drop_duplicates())

    nodes_df = nodes_df.rename(columns={node_key_field: "model_node_id"})

    nodes_df["geometry"] = nodes_df["geometry"].apply(
        get_point_geometry_from_linestring, pos=link_pos
    )
    nodes_df["X"] = nodes_df.geometry.x
    nodes_df["Y"] = nodes_df.geometry.y

    if merge_duplicates:
        nodes_df = _merge_duplicate_nodes_by_average(nodes_df)
    elif nodes_df["model_node_id"].duplicated().any():
        dupe_ids = nodes_df.loc[nodes_df["model_node_id"].duplicated(), "model_node_id"].unique()
        msg = (
            f"Found {len(dupe_ids)} model_node_id(s) with multiple, possibly conflicting, "
            f"geometries: {sorted(dupe_ids)[:10]}. Pass merge_duplicates=True to average "
            "their coordinates if this is expected."
        )
        raise NodeAddError(msg)

    nodes_df = gpd.GeoDataFrame(nodes_df, geometry="geometry", crs=LAT_LON_CRS)
    nodes_df["model_node_id_idx"] = nodes_df["model_node_id"]
    nodes_df.set_index("model_node_id_idx", inplace=True)
    nodes_df = validate_df_to_model(nodes_df, RoadNodesTable)
    # WranglerLogger.debug(f"ct3: nodes_df:\n{nodes_df}")
    return nodes_df


def _merge_duplicate_nodes_by_average(
    nodes_df: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """Merge duplicate model_node_id rows by averaging coordinates.

    Raises if duplicate geometries for the same model_node_id are spatially far apart,
    since that almost always indicates an input-network issue rather than an
    intentional near-duplicate.
    """
    # Keep this as a local threshold for now; if users need to tune it, we can
    # promote it to WranglerConfig (e.g., MODEL_ROADWAY) in a follow-up change.
    DUPLICATE_NODE_ERROR_SPREAD_METERS = 100.0
    duplicate_nodes = nodes_df[nodes_df.duplicated(subset=["model_node_id"], keep=False)]
    if len(duplicate_nodes) == 0:
        return gpd.GeoDataFrame(nodes_df, geometry="geometry", crs=LAT_LON_CRS)

    WranglerLogger.warning(
        f"Found {len(duplicate_nodes)} duplicate node entries for "
        f"{duplicate_nodes['model_node_id'].nunique()} unique node IDs. Averaging their "
        "coordinates because merge_duplicates=True."
    )
    WranglerLogger.debug(f"Duplicate nodes:\n{duplicate_nodes[['model_node_id', 'X', 'Y']]}")

    # Use WEB_MERCATOR_CRS only to measure approximate planar spread in meters
    # for this error threshold check. It is globally available and avoids requiring a
    # region-specific local CRS in this low-stakes diagnostic path.
    dupes_projected = gpd.GeoDataFrame(
        duplicate_nodes, geometry="geometry", crs=LAT_LON_CRS
    ).to_crs(WEB_MERCATOR_CRS)
    dupes_projected["x_m"] = dupes_projected.geometry.x
    dupes_projected["y_m"] = dupes_projected.geometry.y

    spread_by_id = (
        dupes_projected.groupby("model_node_id")["geometry"]
        .apply(lambda geom: geom.apply(lambda g: geom.distance(g).max()).max())
        .astype(float)
    )
    wide_spread = spread_by_id[spread_by_id > DUPLICATE_NODE_ERROR_SPREAD_METERS]
    if not wide_spread.empty:
        msg = (
            "Refusing to merge duplicate model_node_id geometries with large spatial spread "
            f"(>{DUPLICATE_NODE_ERROR_SPREAD_METERS:.1f} m) for {len(wide_spread)} nodes, "
            "which almost certainly indicates an upstream data error. "
            f"Largest spreads (m): {wide_spread.sort_values(ascending=False).head(10).to_dict()}"
        )
        raise NodeAddError(msg)

    merged = nodes_df.groupby("model_node_id", as_index=False).agg({"X": "mean", "Y": "mean"})
    merged["geometry"] = merged.apply(lambda row: point_from_xy(row["X"], row["Y"]), axis=1)
    return gpd.GeoDataFrame(merged, geometry="geometry", crs=LAT_LON_CRS)


def generate_node_ids(nodes_df: DataFrame[RoadNodesTable], range: tuple[int], n: int) -> list[int]:
    """Generate unique node ids for nodes_df.

    Args:
        nodes_df: nodes dataframe to generate unique ids for.
        range: range of ids to generate from.
        n: number of ids to generate.

    Returns:
        list[int]: list of unique node ids.
    """
    if n <= 0:
        return []
    existing_ids = set(nodes_df["model_node_id"].unique())
    new_ids = set(range) - existing_ids
    if len(new_ids) < n:
        msg = f"Only {len(new_ids)} new ids available, need {n}."
        raise NodeAddError(msg)

    return list(new_ids)[:n]

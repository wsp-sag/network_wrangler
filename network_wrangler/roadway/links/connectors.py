"""Functions to create centroid connector links between zone centroids and the roadway network.

Example usage:

```python
zones_table = prepare_zones_table(zones_gdf, zone_id_col="TAZ1454")
add_centroid_nodes(road_net, zones_table)
add_centroid_connectors(
    road_net,
    zones_table,
    mode="drive",
    local_crs="EPSG:26915",
    zone_buffer_distance=20,  # units of local_crs — search radius beyond zone boundary
    num_centroid_connectors=4,  # max connectors per zone
    max_mode_graph_degrees=4,  # exclude high-degree nodes (e.g. motorway ramps)
    default_link_attribute_dict={"lanes": 7},  # required: RoadLinksTable has no lanes default
)
```

See docs/how_to.md for a fuller end-to-end network creation workflow.
"""

from enum import IntEnum
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.geometry
from pandera.typing import DataFrame

from ...errors import LinkAddError
from ...logger import WranglerLogger
from ...models.roadway.tables import ZonesTable
from ...params import LAT_LON_CRS, MODES_TO_NETWORK_LINK_VARIABLES
from ...utils.geo import point_bearings_degrees
from ..network import RoadwayNetwork


class FitForCentroidConnection(IntEnum):
    """Indicates the fitness of this link to be connected to a centroid connector.

    Since the connector will connect to a node, the highest (worst) of this value
    for the links will apply to the node. So if one link is DO_NOT_USE, it won't be used.

    Used by add_centroid_connectors(), which also sets this value to DO_NOT_USE on the
    centroid connector links it creates, so they are excluded as candidates by future calls.
    """

    BEST = 1
    GOOD = 2
    OKAY = 3
    DO_NOT_USE = 100


def calculate_bearing_from_centroid(
    gdf: gpd.GeoDataFrame,
    centroid_col: str = "geometry_centroid",
    bearing_col: str = "bearing",
) -> gpd.GeoDataFrame:
    """Add the bearing from each zone centroid to its point, clockwise from north.

    Adds a new column to the GeoDataFrame containing the bearing in degrees (0-360)
    measured clockwise from north of the line from each centroid to its corresponding
    point geometry.

    Args:
        gdf: GeoDataFrame with point geometries in the geometry column.
        centroid_col: Name of the column containing the centroid point geometry
            (default: 'geometry_centroid').
        bearing_col: Name of the new column to store bearing values
            (default: 'bearing').

    Returns:
        GeoDataFrame with the new bearing column added.

    Raises:
        ValueError: if ``gdf`` is in a geographic (lat/lon) CRS. Bearings require
            projected coordinates for meaningful planar results.

    Note:
        - Bearing is measured clockwise from north (0=north, 90=east, 180=south, 270=west).
        - Returns values in the range [0, 360).
    """
    if gdf.crs is not None and gdf.crs.is_geographic:
        msg = (
            "calculate_bearing_from_centroid requires a projected CRS for planar bearings, "
            f"but got geographic CRS: {gdf.crs}"
        )
        raise ValueError(msg)
    gdf[bearing_col] = point_bearings_degrees(gdf[centroid_col], gdf.geometry)
    return gdf


def _score_and_filter_candidate_nodes(
    road_net: RoadwayNetwork,
    mode: str,
    local_crs: str,
    zones_table: DataFrame[ZonesTable],
    zone_buffer_distance: int,
    max_mode_graph_degrees: int,
) -> gpd.GeoDataFrame:
    """Score nodes by mode fitness and out-degree, then spatially filter to zone candidates.

    Projects nodes and zones to ``local_crs``, spatial-joins nodes to zones within
    ``zone_buffer_distance``, computes distance and bearing from each zone centroid to each
    candidate node, and drops nodes that exceed ``max_mode_graph_degrees``.

    Args:
        road_net: RoadwayNetwork to evaluate.
        mode: Mode string (key in MODES_TO_NETWORK_LINK_VARIABLES).
        local_crs: Projected CRS for distance and bearing calculations.
        zones_table: Validated zones table (in LAT_LON_CRS).
        zone_buffer_distance: Search radius beyond zone boundary (in local_crs units).
        max_mode_graph_degrees: Maximum out-degree; nodes above this are excluded.

    Returns:
        GeoDataFrame of candidate nodes projected to ``local_crs`` with added columns:
        ``zone_id``, ``distance_from_centroid``, ``centroid_angle``,
        ``{mode}_centroid_fit``, ``{mode}_graph_degrees``.
    """
    degrees_col = f"{mode}_graph_degrees"
    fit_col = f"{mode}_centroid_fit"

    # Score each node: out-degree and worst fit among its modal out-links. Centroid connector
    # links created by a prior call (for this or another zone type) have fit_col explicitly
    # set to DO_NOT_USE, so they're excluded here and can't leak in as candidates. Treat an
    # unset fit_col the same way, since there's no basis to consider the link a good fit.
    modal_links_df = road_net.links_df.mode_query(mode)
    modal_links_df = modal_links_df.fillna({fit_col: FitForCentroidConnection.DO_NOT_USE})
    scores_df = modal_links_df.groupby("A")[fit_col].agg(**{degrees_col: "size", fit_col: "max"})
    scores_df = scores_df[scores_df[fit_col] != FitForCentroidConnection.DO_NOT_USE]

    candidate_nodes_df = gpd.GeoDataFrame(
        pd.merge(
            left=road_net.nodes_df,
            right=scores_df,
            left_on="model_node_id",
            right_index=True,
            how="inner",
            validate="one_to_one",
        ),
        geometry="geometry",
        crs=LAT_LON_CRS,
    )
    WranglerLogger.debug(
        f"Evaluated {len(candidate_nodes_df):,} usable {mode} nodes for centroid connectors:\n"
        f"{candidate_nodes_df}"
    )

    # Project to local CRS for planar distance / bearing.
    candidate_nodes_df.to_crs(local_crs, inplace=True)
    zones_table.to_crs(local_crs, inplace=True)

    # Spatial join: assign each node to its enclosing zone (with buffer for edge cases).
    candidate_nodes_df = gpd.sjoin(
        left_df=candidate_nodes_df,
        right_df=zones_table[["zone_id", "geometry", "geometry_centroid"]],
        how="left",
        predicate="dwithin",
        distance=zone_buffer_distance,
    )
    WranglerLogger.debug(
        f"After spatial join, non-unique model_node_ids:\n"
        f"{candidate_nodes_df.loc[candidate_nodes_df['model_node_id'].duplicated(keep=False)]}"
    )

    # Reproject centroid column and compute distance + bearing from centroid to node.
    gs = gpd.GeoSeries(candidate_nodes_df["geometry_centroid"], crs=LAT_LON_CRS)
    candidate_nodes_df["geometry_centroid"] = gs.to_crs(local_crs).values
    candidate_nodes_df["distance_from_centroid"] = candidate_nodes_df.geometry.distance(
        gpd.GeoSeries(candidate_nodes_df["geometry_centroid"], crs=local_crs)
    )
    candidate_nodes_df = calculate_bearing_from_centroid(
        candidate_nodes_df, "geometry_centroid", "centroid_angle"
    )
    WranglerLogger.debug(f"After distance/bearing calculation:\n{candidate_nodes_df}")

    # Keep only nodes inside a zone and below the out-degree cap.
    candidate_nodes_df = candidate_nodes_df.loc[candidate_nodes_df["zone_id"].notna()]
    candidate_nodes_df = candidate_nodes_df.loc[
        candidate_nodes_df[degrees_col] <= max_mode_graph_degrees
    ]
    candidate_nodes_df.sort_values(by=["zone_id", fit_col, "distance_from_centroid"], inplace=True)
    candidate_nodes_df.reset_index(drop=True, inplace=True)

    WranglerLogger.debug(
        f"Filtered to {len(candidate_nodes_df):,} candidate nodes:\n{candidate_nodes_df}"
    )
    return candidate_nodes_df


def _select_nodes_by_sector(
    candidate_nodes_df: gpd.GeoDataFrame,
    num_centroid_connectors: int,
    fit_col: str,
    zones_table: DataFrame[ZonesTable],
) -> gpd.GeoDataFrame:
    """Select up to ``num_centroid_connectors`` nodes per zone using sector-based distribution.

    Divides the 360° bearing circle around each centroid into ``num_centroid_connectors``
    equal sectors and picks the best-fit, closest node within each sector.

    Args:
        candidate_nodes_df: Scored and spatially filtered candidate nodes
            (output of :func:`_score_and_filter_candidate_nodes`).
        num_centroid_connectors: Maximum connectors (and sectors) per zone.
        fit_col: Column name containing the fitness score (e.g. ``drive_centroid_fit``).
        zones_table: Validated zones table; used only to warn about zones with no candidates.

    Returns:
        GeoDataFrame of selected nodes with an additional ``connector_num`` column
        (1-based index within each zone).
    """
    sector_width = 360.0 / num_centroid_connectors
    candidate_nodes_df["sector"] = (
        (candidate_nodes_df["centroid_angle"] % 360) // sector_width
    ).astype(int)
    candidate_nodes_df["sector_rank"] = candidate_nodes_df.groupby(
        ["zone_id", "sector"]
    ).cumcount()
    candidate_nodes_df.sort_values(
        ["zone_id", "sector_rank", fit_col, "distance_from_centroid"], inplace=True
    )
    selected_nodes_df = candidate_nodes_df.groupby("zone_id").head(num_centroid_connectors).copy()
    selected_nodes_df["connector_num"] = selected_nodes_df.groupby("zone_id").cumcount() + 1
    selected_nodes_df.reset_index(drop=True, inplace=True)

    zones_with_connectors = set(selected_nodes_df["zone_id"])
    for _zone_id in zones_table["zone_id"]:
        if _zone_id not in zones_with_connectors:
            WranglerLogger.warning(f"No centroid connectors for zone_id {_zone_id}")

    WranglerLogger.info(
        f"Selected {len(selected_nodes_df):,} centroid connectors for {len(zones_table):,} zones"
    )
    WranglerLogger.debug(f"selected_nodes_df:\n{selected_nodes_df}")
    return selected_nodes_df


def _build_connector_links_df(
    selected_nodes_df: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Build a DataFrame of bidirectional centroid connector link geometries.

    Creates one zone→node link and one node→zone link for each selected node,
    with a LineString geometry between the zone centroid and the roadway node.

    Args:
        selected_nodes_df: Selected connector nodes
            (output of :func:`_select_nodes_by_sector`).

    Returns:
        DataFrame with columns: ``A``, ``B``, ``name``, ``length``, ``geometry``.
    """
    links_taz_to_node_df = selected_nodes_df.copy()
    links_taz_to_node_df.rename(
        columns={"zone_id": "A", "model_node_id": "B", "distance_from_centroid": "length"},
        inplace=True,
    )
    links_taz_to_node_df["name"] = "zone to node"
    links_taz_to_node_df["geometry"] = [
        shapely.geometry.LineString([c, g])
        for c, g in zip(
            links_taz_to_node_df["geometry_centroid"],
            links_taz_to_node_df["geometry"],
            strict=True,
        )
    ]

    links_node_to_taz_df = selected_nodes_df.copy()
    links_node_to_taz_df.rename(
        columns={"model_node_id": "A", "zone_id": "B", "distance_from_centroid": "length"},
        inplace=True,
    )
    links_node_to_taz_df["name"] = "node to zone"
    links_node_to_taz_df["geometry"] = [
        shapely.geometry.LineString([g, c])
        for g, c in zip(
            links_node_to_taz_df["geometry"],
            links_node_to_taz_df["geometry_centroid"],
            strict=True,
        )
    ]

    centroid_links_df = pd.concat([links_taz_to_node_df, links_node_to_taz_df])
    centroid_links_df.reset_index(drop=False, inplace=True)
    centroid_links_df = centroid_links_df[["A", "B", "name", "length", "geometry"]]
    WranglerLogger.debug(f"centroid_links_df:\n{centroid_links_df}")
    return centroid_links_df


def add_centroid_connectors(
    road_net: RoadwayNetwork,
    zones_table: DataFrame[ZonesTable],
    mode: str,
    local_crs: str,
    zone_buffer_distance: int,
    num_centroid_connectors: int,
    max_mode_graph_degrees: int,
    default_link_attribute_dict: dict[str, Any],
) -> gpd.GeoDataFrame:
    """Creates centroid connector links between zone centroids and roadway network nodes.

    This function identifies suitable roadway nodes for each zone and creates connector links
    from the zone centroid to those nodes. The selection process prioritizes nodes based on
    their fitness for centroid connections and ensures good spatial distribution.

    Selection Algorithm:
        1. **Node Evaluation**: For each node in the modal graph, calculates:
            - Outgoing degree (number of outbound links)
            - Fitness for centroid connection (worst fitness of connected links)

        2. **Spatial Filtering**: Identifies nodes within each zone boundary and filters out:
            - Nodes with `{mode}_centroid_fit` = DO_NOT_USE
            - Nodes outside zone boundaries
            - Nodes with outgoing degree > max_mode_graph_degrees

        3. **Connector Selection** (per zone):
            - Divide the bearing circle into ``num_centroid_connectors`` equal sectors.
            - Pick the best-fit, closest node per (zone, sector).

        4. **Link Creation**: Creates bidirectional links between zone centroid and selected nodes

    Centroid Connector Link Attributes:
        - **model_link_id**: Auto-incremented from max existing link ID
        - **A, B**: Origin and destination node IDs (bidirectional, so both directions created)
        - **name**: Set to "node to zone" or "zone to node"
        - **length**: Euclidean distance between centroid and node (in local_crs units)
        - **geometry**: LineString from origin to destination
        - **highway**: Set to "centroid_connector" (if highway column exists in network)
        - **Mode access variables**:
            * All mode variables set to False by default
            * Variables for specified mode set to True (from MODES_TO_NETWORK_LINK_VARIABLES)
            * Example: For mode='drive', sets drive_access=True, bike_access=False, etc.
        - **{mode}_centroid_fit columns**: Set to DO_NOT_USE for every existing
            ``{mode}_centroid_fit`` column, so these new connectors are never
            picked as candidates by a future add_centroid_connectors() call.
        - **Custom attributes**: Any attributes from default_link_attribute_dict parameter

    Args:
        road_net: the RoadwayNetwork to update by adding centroid connectors.
            Assumes centroids exist as nodes already. Also assumes links have
            an attribute, `{mode}_centroid_fit`, set to one of the FitForCentroidConnect values.
        zones_table: prepared zones table from ``prepare_zones_table`` with
            polygon geometry in ``geometry`` and centroid point geometry in
            ``geometry_centroid`` (in ``LAT_LON_CRS``).
        mode: one of the keys in [`MODES_TO_NETWORK_LINK_VARIABLES`][network_wrangler.params.MODES_TO_NETWORK_LINK_VARIABLES]
        local_crs: Projected CRS to use for distance calculations.
        zone_buffer_distance: buffer distance from zone shape to consider node for centroid connector.
            This should be in the units of the local_crs.
        num_centroid_connectors: maximum number of centroid connectors per zone
        max_mode_graph_degrees: maximum outgoing degree for a node to be eligible
        default_link_attribute_dict: link attributes to set for the new centroid connector links.
            Must include ``lanes``, since ``RoadLinksTable`` requires a non-null value and
            centroid connectors have no natural default lane count.

    Returns:
        A copy of zones_table with an additional column, `num_connectors`. The road_net is
            updated in place with new centroid connector links, and the nodes table has an
            additional column: `zone_num_connectors`.
    """
    if not default_link_attribute_dict or "lanes" not in default_link_attribute_dict:
        msg = (
            "default_link_attribute_dict must include a 'lanes' value: RoadLinksTable requires "
            "a non-null lanes value and centroid connectors have no natural default."
        )
        raise LinkAddError(msg)

    fit_col = f"{mode}_centroid_fit"

    WranglerLogger.info(
        f"Adding centroid connectors for {len(zones_table):,} zones and mode:{mode}"
    )
    WranglerLogger.debug(f"zones_table:\n{zones_table}")

    candidate_nodes_df = _score_and_filter_candidate_nodes(
        road_net, mode, local_crs, zones_table, zone_buffer_distance, max_mode_graph_degrees
    )
    selected_nodes_df = _select_nodes_by_sector(
        candidate_nodes_df, num_centroid_connectors, fit_col, zones_table
    )
    centroid_links_df = _build_connector_links_df(selected_nodes_df)

    # Assign link IDs and mode access variables.
    max_model_link_id = road_net.links_df.model_link_id.max()
    centroid_links_df["model_link_id"] = centroid_links_df.index + max_model_link_id + 1
    centroid_links_df["shape_id"] = "sh" + centroid_links_df["model_link_id"].astype("str")

    link_mode_variables = {v for vals in MODES_TO_NETWORK_LINK_VARIABLES.values() for v in vals}
    WranglerLogger.debug(f"link_mode_variables:{link_mode_variables}")
    for link_var in link_mode_variables:
        centroid_links_df[link_var] = False
    for link_var in MODES_TO_NETWORK_LINK_VARIABLES[mode]:
        centroid_links_df[link_var] = True

    if "highway" in road_net.links_df.columns:
        centroid_links_df["highway"] = "centroid_connector"

    for colname, default_value in default_link_attribute_dict.items():
        centroid_links_df[colname] = default_value

    # Explicitly mark these new connectors as DO_NOT_USE for every mode's centroid fitness,
    # so future add_centroid_connectors() calls never pick them as candidate nodes.
    for centroid_fit_col in road_net.links_df.columns[
        road_net.links_df.columns.str.endswith("_centroid_fit")
    ]:
        centroid_links_df[centroid_fit_col] = FitForCentroidConnection.DO_NOT_USE

    road_net.add_links(centroid_links_df)
    road_net.add_shapes(centroid_links_df)
    WranglerLogger.info("Added centroid connectors to roadway network")

    # Summarize connectors per zone and attach count to nodes_df.
    summary_df = (
        selected_nodes_df.groupby(by="zone_id")
        .aggregate(num_connectors=pd.NamedAgg(column="model_node_id", aggfunc="nunique"))
        .reset_index(drop=False)
    )
    summary_df = pd.merge(left=zones_table, right=summary_df, how="left", validate="one_to_one")
    summary_df["num_connectors"] = summary_df["num_connectors"].fillna(0).astype(int)
    WranglerLogger.debug(f"summary_df:\n{summary_df}")
    WranglerLogger.info(
        f"num_connectors added per zone (target:{num_centroid_connectors}):\n"
        f"{summary_df['num_connectors'].value_counts()}"
    )

    # Attach the per-zone count to the centroid nodes. Use .map() rather than pd.merge()
    # so that nodes_df keeps its `model_node_id_idx` primary-key index and its attrs.
    # combine_first preserves values from a prior call for a different zone type (e.g.
    # TAZs) instead of this pass's NaNs overwriting them for nodes outside its zone set.
    num_connectors_col = "zone_num_connectors"
    new_num_connectors = (
        road_net.nodes_df["model_node_id"]
        .map(summary_df.set_index("zone_id")["num_connectors"])
        .astype("Int64")
    )
    if num_connectors_col in road_net.nodes_df.columns:
        new_num_connectors = new_num_connectors.combine_first(
            road_net.nodes_df[num_connectors_col]
        ).astype("Int64")
    road_net.nodes_df[num_connectors_col] = new_num_connectors
    road_net._mark_modified()
    WranglerLogger.debug(f"road_net.nodes_df:\n{road_net.nodes_df}")
    return summary_df

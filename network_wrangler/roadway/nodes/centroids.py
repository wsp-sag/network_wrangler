"""Functions to create centroid nodes from a zones table.

Example usage:

```python
zones_table = prepare_zones_table(zones_gdf, zone_id_col="TAZ1454")
add_centroid_nodes(road_net, zones_table)
```

See docs/how_to.md for a fuller end-to-end network creation workflow.
"""

from typing import Any

import geopandas as gpd
from pandera.typing import DataFrame

from ...logger import WranglerLogger
from ...models.roadway.tables import ZonesTable
from ...params import LAT_LON_CRS
from ...utils.models import validate_df_to_model
from ..network import RoadwayNetwork


def prepare_zones_table(
    zones_gdf: gpd.GeoDataFrame,
    zone_id_col: str,
    metadata: dict[str, object] | None = None,
) -> DataFrame[ZonesTable]:
    """Create a validated zones table for centroid workflows.

    Renames the user-specified ``zone_id_col`` to ``zone_id`` and
    validates/coerces against ``ZonesTable``. Downstream centroid code works
    exclusively with this canonical ``zone_id`` column.

    Metadata is preserved in ``attrs`` and can be augmented with ``metadata``.
    """
    if zone_id_col not in zones_gdf.columns:
        msg = f"zones_gdf is missing required zone id column: {zone_id_col}"
        raise ValueError(msg)

    zones_table = validate_df_to_model(
        zones_gdf.rename(columns={zone_id_col: "zone_id"}), ZonesTable
    )
    zones_table.attrs.update(zones_gdf.attrs)
    if metadata:
        zones_table.attrs.update(metadata)
    return zones_table


def zones_table_to_gdf(
    zones_table: DataFrame[ZonesTable],
    zone_id_col: str | None = None,
) -> gpd.GeoDataFrame:
    """Convert a validated ``ZonesTable`` back to a GeoDataFrame.

    Args:
        zones_table: validated zones table in canonical schema form.
        zone_id_col: optional name to rename the ``zone_id`` column to on output.
            If omitted, the column stays named ``zone_id``.

    Returns:
        GeoDataFrame with zone IDs optionally renamed and metadata preserved in ``attrs``.
    """
    zones_gdf = gpd.GeoDataFrame(
        zones_table.copy(),
        geometry="geometry",
        crs=getattr(zones_table, "crs", LAT_LON_CRS),
    )
    if zone_id_col and zone_id_col != "zone_id":
        zones_gdf = zones_gdf.rename(columns={"zone_id": zone_id_col})
    zones_gdf.attrs.update(zones_table.attrs)
    return zones_gdf


def add_centroid_nodes(
    road_net: RoadwayNetwork,
    zones_table: DataFrame[ZonesTable],
    default_node_attribute_dict: dict[str, Any] | None = None,
):
    """Adds the given centroid nodes to the roadway network.

    Args:
        road_net: the RoadwayNetwork to update by adding centroids
        zones_table: prepared zones table from ``prepare_zones_table`` with
            polygon geometry in ``geometry`` and centroid point geometry in
            ``geometry_centroid``.
        default_node_attribute_dict: node attributes to set for the new centroid nodes.
            Defaults to None.
    """
    centroid_nodes_gdf = (
        zones_table[["zone_id", "geometry_centroid"]]
        .rename(columns={"geometry_centroid": "geometry", "zone_id": "model_node_id"})
        .set_geometry("geometry", crs=LAT_LON_CRS)
    )
    centroid_nodes_gdf["X"] = centroid_nodes_gdf["geometry"].x
    centroid_nodes_gdf["Y"] = centroid_nodes_gdf["geometry"].y
    # Centroids are synthetic nodes, so do not assign fabricated OSM IDs.
    centroid_nodes_gdf["osm_node_id"] = None

    # set default node attributes
    centroid_nodes_gdf = centroid_nodes_gdf.assign(**(default_node_attribute_dict or {}))

    len_road_net_nodes = len(road_net.nodes_df)
    WranglerLogger.debug(f"centroid_nodes_gdf:\n{centroid_nodes_gdf}")
    road_net.add_nodes(centroid_nodes_gdf)
    WranglerLogger.info(
        f"Added {len(zones_table):,} centroid nodes: "
        f"increased size of nodes_df from {len_road_net_nodes:,} to {len(road_net.nodes_df):,}"
    )

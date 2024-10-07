"""Edits RoadShapesTable properties.

NOTE: Each public method will return a whole copy of the RoadShapesTable with associated edits.
Private methods may return mutated originals.
"""

from __future__ import annotations

import copy

from pandera.typing import DataFrame

from ...models.roadway.tables import RoadLinksTable, RoadNodesTable, RoadShapesTable
from ...utils.geo import update_nodes_in_linestring_geometry


def edit_shape_geometry_from_nodes(
    shapes_df: DataFrame[RoadShapesTable],
    links_df: DataFrame[RoadLinksTable],
    nodes_df: DataFrame[RoadNodesTable],
    node_ids: list[int],
) -> DataFrame[RoadShapesTable]:
    """Updates the geometry for shapes for a given list of nodes.

    Should be called by any function that changes a node location.

    NOTE: This will mutate the geometry of a shape in place for the start and end node
            ...but not the nodes in-between.  Something to consider.

    Args:
        shapes_df: RoadShapesTable
        links_df: RoadLinksTable
        nodes_df: RoadNodesTable
        node_ids: list of node PKs with updated geometry
    """
    shapes_df = copy.deepcopy(shapes_df)
    links_A_df = links_df.loc[links_df.A.isin(node_ids)]
    _tempshape_A_df = shapes_df[["shape_id", "geometry"]].merge(
        links_A_df[["shape_id", "A"]], on="shape_id", how="inner"
    )
    _shape_ids_A = _tempshape_A_df.shape_id.unique().tolist()
    if _shape_ids_A:
        shapes_df[_shape_ids_A, "geometry"] = update_nodes_in_linestring_geometry(
            _tempshape_A_df, nodes_df, 0
        )

    links_B_df = links_df.loc[links_df.B.isin(node_ids)]
    _tempshape_B_df = shapes_df[["shape_id", "geometry"]].merge(
        links_B_df[["shape_id", "B"]], on="shape_id", how="inner"
    )
    _shape_ids_B = _tempshape_B_df.shape_id.unique().tolist()
    if _shape_ids_A:
        shapes_df[_shape_ids_B, "geometry"] = update_nodes_in_linestring_geometry(
            _tempshape_A_df, nodes_df, -1
        )
    return shapes_df

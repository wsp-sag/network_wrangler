"""Functions to delete shapes from RoadShapesTable."""

from pandera.typing import DataFrame

from ...errors import ShapeDeletionError
from ...logger import WranglerLogger
from ...models.roadway.tables import RoadShapesTable


def delete_shapes_by_ids(
    shapes_df: DataFrame[RoadShapesTable], del_shape_ids: list[int], ignore_missing: bool = False
) -> DataFrame[RoadShapesTable]:
    """Deletes shapes from shapes_df by shape_id.

    Args:
        shapes_df: RoadShapesTable
        del_shape_ids: list of shape_ids to delete
        ignore_missing: if True, will not raise an error if shape_id is not found in shapes_df

    Returns:
        DataFrame[RoadShapesTable]: a copy of shapes_df with shapes removed
    """
    WranglerLogger.debug(f"Deleting shapes with ids: \n{del_shape_ids}")

    _missing = set(del_shape_ids) - set(shapes_df.index)
    if _missing:
        WranglerLogger.warning(f"Shapes in network not there to delete: \n{_missing}")
        if not ignore_missing:
            msg = "Shapes to delete are not in the network."
            raise ShapeDeletionError(msg)
    return shapes_df.drop(labels=del_shape_ids, errors="ignore")

"""Functions to delete shapes from RoadShapesTable."""

from ...logger import WranglerLogger
from ...models.roadway.tables import RoadShapesTable


class ShapeDeletionError(Exception):
    pass


def delete_shapes_by_ids(
    shapes_df: RoadShapesTable, del_shape_ids: list[int], ignore_missing: bool = False
):
    WranglerLogger.debug(f"Deleting shapes with ids:\n{del_shape_ids}")

    _missing = set(del_shape_ids) - set(shapes_df.index)
    if _missing:
        WranglerLogger.warning(f"Shapes in network not there to delete: \n{_missing}")
        if not ignore_missing:
            raise ShapeDeletionError("Shapes to delete are not in the network.")
    return shapes_df.drop(labels=del_shape_ids, errors="ignore")

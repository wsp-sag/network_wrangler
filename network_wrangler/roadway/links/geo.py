"""Functions for updating roadway links with geometry from shapes."""

from pandera.typing import DataFrame

from ...models.roadway.tables import RoadLinksTable, RoadShapesTable
from ...utils.data import update_df_by_col_value


def true_shape(
    links_df: DataFrame[RoadLinksTable], shapes_df: DataFrame[RoadShapesTable]
) -> DataFrame[RoadLinksTable]:
    """Updates geometry to have shape of shapes_df where available."""
    return update_df_by_col_value(links_df, shapes_df, "shape_id", properties=["geometry"])

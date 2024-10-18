"""Helpter functions which filter a RoadShapesTable."""

from pandera.typing import DataFrame

from ...models.roadway.tables import RoadLinksTable, RoadShapesTable


def filter_shapes_to_links(
    shapes_df: DataFrame[RoadShapesTable], links_df: DataFrame[RoadLinksTable]
) -> DataFrame[RoadShapesTable]:
    """Shapes which are referenced in RoadLinksTable."""
    return shapes_df.loc[shapes_df.shape_id.isin(links_df.shape_id)]

"""Helpter functions which filter a RoadShapesTable"""

from ...models.roadway.tables import RoadLinksTable, RoadShapesTable


def filter_shapes_to_links(
    shapes_df: RoadShapesTable, links_df: RoadLinksTable
) -> RoadShapesTable:
    """Shapes which are referenced in RoadLinksTable."""
    return shapes_df.loc[shapes_df.shape_id.isin(links_df.shape_id)]

"""Functions that query RoadShapesTable."""

from pandera.typing import DataFrame

from ...models.roadway.tables import RoadLinksTable, RoadShapesTable


def shape_ids_without_links(
    shapes_df: DataFrame[RoadShapesTable], links_df: DataFrame[RoadLinksTable]
) -> list[int]:
    """List of shape ids that don't have associated links."""
    return list(set(shapes_df.index) - set(links_df.shape_ids.to_list()))

"""Functions to create segments from shapes and shape_links."""

import pandas as pd
from pandera.typing import DataFrame

from ...models.gtfs.tables import WranglerShapesTable


def shape_links_to_segments(shape_links) -> pd.DataFrame:
    """Convert shape_links to segments by shape_id with segments of continuous shape_pt_sequence.

    Returns: DataFrame with shape_id, segment_id, segment_start_shape_pt_seq,
        segment_end_shape_pt_seq
    """
    shape_links["gap"] = shape_links.groupby("shape_id")["shape_pt_sequence_A"].diff().gt(1)
    shape_links["segment_id"] = shape_links.groupby("shape_id")["gap"].cumsum()

    # Define segment starts and ends
    segment_definitions = (
        shape_links.groupby(["shape_id", "segment_id"])
        .agg(
            segment_start_shape_pt_seq=("shape_pt_sequence_A", "min"),
            segment_end_shape_pt_seq=("shape_pt_sequence_B", "max"),
        )
        .reset_index()
    )

    # Optionally calculate segment lengths for further uses
    segment_definitions["segment_length"] = (
        segment_definitions["segment_end_shape_pt_seq"]
        - segment_definitions["segment_start_shape_pt_seq"]
        + 1
    )

    return segment_definitions


def shape_links_to_longest_shape_segments(shape_links) -> pd.DataFrame:
    """Find the longest segment of each shape_id that is in the links.

    Args:
        shape_links: DataFrame with shape_id, shape_pt_sequence_A, shape_pt_sequence_B

    Returns:
        DataFrame with shape_id, segment_id, segment_start_shape_pt_seq, segment_end_shape_pt_seq
    """
    segments = shape_links_to_segments(shape_links)
    idx = segments.groupby("shape_id")["segment_length"].idxmax()
    longest_segments = segments.loc[idx]
    return longest_segments


def filter_shapes_to_segments(
    shapes: DataFrame[WranglerShapesTable], segments: pd.DataFrame
) -> DataFrame[WranglerShapesTable]:
    """Filter shapes dataframe to records associated with segments dataframe.

    Args:
        shapes: shapes dataframe to filter
        segments: segments dataframe to filter by with shape_id, segment_start_shape_pt_seq,
            segment_end_shape_pt_seq . Should have one record per shape_id.

    Returns:
        filtered shapes dataframe
    """
    shapes_w_segs = shapes.merge(segments, on="shape_id", how="left")

    # Retain only those points within the segment sequences
    filtered_shapes = shapes_w_segs[
        (shapes_w_segs["shape_pt_sequence"] >= shapes_w_segs["segment_start_shape_pt_seq"])
        & (shapes_w_segs["shape_pt_sequence"] <= shapes_w_segs["segment_end_shape_pt_seq"])
    ]

    drop_cols = [
        "segment_id",
        "segment_start_shape_pt_seq",
        "segment_end_shape_pt_seq",
        "segment_length",
    ]
    filtered_shapes = filtered_shapes.drop(columns=drop_cols)

    return filtered_shapes

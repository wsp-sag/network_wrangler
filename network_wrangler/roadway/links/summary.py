"""Utilities for summarizing a RoadLinksTable."""

import pandas as pd
from pandera.typing import DataFrame

from ...models.roadway.tables import RoadLinksTable
from .filters import (
    filter_links_access_dummy,
    filter_links_drive_access,
    filter_links_egress_dummy,
    filter_links_general_purpose,
    filter_links_managed_lanes,
    filter_links_pedbike_only,
    filter_links_transit_access,
    filter_links_transit_only,
)
from .links import calc_lane_miles

link_summary_cats = {
    "managed": filter_links_managed_lanes,
    "general_purpose": filter_links_general_purpose,
    "access": filter_links_access_dummy,
    "egress": filter_links_egress_dummy,
    "pedbike only": filter_links_pedbike_only,
    "transit only": filter_links_transit_only,
    "transit ok": filter_links_transit_access,
    "drive access": filter_links_drive_access,
}


def link_summary_cnt(links_df: DataFrame[RoadLinksTable]) -> dict[str, int]:
    """Dictionary of number of links by `link_summary_cats`."""
    return {k: len(v(links_df)) for k, v in link_summary_cats.items()}


def link_summary_miles(links_df: DataFrame[RoadLinksTable]) -> dict[str, float]:
    """Dictionary of miles by `link_summary_cats`."""
    return {k: v(links_df).distance.sum() for k, v in link_summary_cats.items()}


def link_summary_lane_miles(links_df: DataFrame[RoadLinksTable]) -> dict[str, float]:
    """Dictionary of lane miles by `link_summary_cats`."""
    return {k: calc_lane_miles(v(links_df)).sum() for k, v in link_summary_cats.items()}


def link_summary(links_df: DataFrame[RoadLinksTable]) -> pd.DataFrame:
    """Summarizes links by `link_summary_cats`: count, distance, and lane miles."""
    data = {
        "count": link_summary_cnt(links_df),
        "distance": link_summary_miles(links_df),
        "lane miles": link_summary_lane_miles(links_df),
    }
    return pd.DataFrame(data, index=link_summary_cats.keys())

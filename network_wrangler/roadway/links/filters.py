"""Functions to filter a RoadLinksTable based on various properties."""

from typing import List

import pandas as pd

from ...logger import WranglerLogger
from ...models.roadway.tables import RoadLinksTable


def filter_link_properties_managed_lanes(links_df: RoadLinksTable) -> RoadLinksTable:
    return [
        i
        for i in links_df.columns
        if i.startswith("ML_")
        or i.startswith("sc_ML_")
        and i not in ["ML_access_point", "ML_egress_point"]
    ]


def filter_links_managed_lanes(links_df: RoadLinksTable) -> RoadLinksTable:
    """Filters links dataframe to only include managed lanes."""
    return links_df.loc[links_df["managed"] == 1]


def filter_links_parallel_general_purpose(links_df: RoadLinksTable) -> RoadLinksTable:
    """Filters links dataframe to only include general purpose links parallel to managed.

    NOTE This will return Null when not a model network.
    """
    return links_df.loc[links_df["managed"] == -1]


def filter_links_general_purpose(links_df: RoadLinksTable) -> RoadLinksTable:
    """Filters links dataframe to only include all general purpose links.

    NOTE: This will only return links without parallel managed lanes in a non-model-ready network.
    """
    return links_df.loc[links_df["managed"] < 1]


def filter_links_general_purpose_no_parallel_managed(
    links_df: RoadLinksTable,
) -> RoadLinksTable:
    """Filters links dataframe to only include general purpose links without parallel managed lanes.

    NOTE: This will only return links without parallel managed lanes in a non-model-ready network.
    """
    return links_df.loc[links_df["managed"] == 0]


def filter_links_access_dummy(links_df: RoadLinksTable) -> RoadLinksTable:
    """Filters links dataframe to only include all access dummy links connecting managed lanes."""
    return links_df.loc[links_df["roadway"] == "ml_access_point"]


def filter_links_egress_dummy(links_df: RoadLinksTable) -> RoadLinksTable:
    """Filters links dataframe to only include all egress dummy links connecting managed lanes."""
    return links_df.loc[links_df["roadway"] == "ml_egress_point"]


def filter_links_dummy(links_df: RoadLinksTable) -> RoadLinksTable:
    """Filters links dataframe to only include all dummy links connecting managed lanes."""
    return links_df.loc[
        (links_df["roadway"] == "ml_access_point")
        | (links_df["roadway"] == "ml_egress_point")
    ]


def filter_links_centroid_connector(links_df: RoadLinksTable) -> RoadLinksTable:
    """Filters links dataframe to only include all general purpose links."""
    raise NotImplementedError


def filter_links_pedbike_only(links_df: RoadLinksTable) -> RoadLinksTable:
    """Filters links dataframe to only include links that only ped/bikes can be on."""
    return links_df.loc[
        (
            (
                (links_df["walk_access"].astype(bool))
                | (links_df["bike_access"].astype(bool))
            )
            & ~(links_df["drive_access"].astype(bool))
        )
    ]


def filter_links_transit_only(links_df: RoadLinksTable) -> RoadLinksTable:
    """Filters links dataframe to only include all links that only transit can operate on."""
    return links_df.loc[
        (links_df["bus_only"].astype(bool)) | (links_df["rail_only"].astype(bool))
    ]


def filter_links_to_modes(links_df: RoadLinksTable, modes: List[str]) -> RoadLinksTable:
    """Filters links dataframe to only include links that are accessible by the modes in the list.

    Args:
        links_df (RoadLinksTable): links dataframe
        modes (List[str]): list of modes to filter by.

    Returns:
        RoadLinksTable: filtered links dataframe
    """
    if "any" in modes:
        return links_df
    if isinstance(modes, str):
        modes = [modes]
    _mode_link_props = list(
        set(
            [
                m
                for m in modes
                for m in links_df.params.modes_to_network_link_variables[m]
            ]
        )
    )
    return links_df.loc[links_df[_mode_link_props].any(axis=1)]


def filter_links_transit_access(links_df: RoadLinksTable) -> RoadLinksTable:
    """Filters links dataframe to only include all links that transit can operate on."""
    return filter_links_to_modes(links_df, "transit")


def filter_links_drive_access(links_df: RoadLinksTable):
    return filter_links_to_modes(links_df, "drive")


def filter_links_to_node_ids(
    links_df: RoadLinksTable, node_ids: list[int]
) -> RoadLinksTable:
    """Filters links dataframe to only include links with either A or B in node_ids."""
    return links_df.loc[links_df["A"].isin(node_ids) | links_df["B"].isin(node_ids)]


def filter_links_to_ids(links_df: RoadLinksTable, link_ids: List[int]) -> pd.DataFrame:
    """Filters links dataframe by link_ids."""
    return links_df.loc[links_df["model_link_id"].isin(link_ids)]


def filter_links_not_in_ids(
    links_df: RoadLinksTable, link_ids: List[int]
) -> pd.DataFrame:
    """Filters links dataframe to NOT have link_ids."""
    return links_df.loc[~links_df["model_link_id"].isin(link_ids)]


def filter_links_to_path(
    links_df: RoadLinksTable, node_id_path_list: list[int], ignore_missing: bool = False
):
    """Return selection of links dataframe with nodes along path defined by node_id_path_list.

    Args:
        links_df: Links dataframe to select from
        node_id_path_list: List of node ids.
        ignore_missing: if True, will ignore if links noted by path node sequence don't exist in
            links_df and will just return what does exist. Defaults to False.
    """
    ab_pairs = [node_id_path_list[i : i + 2] for i, _ in enumerate(node_id_path_list)][
        :-1
    ]
    path_links_df = pd.DataFrame(ab_pairs, columns=["A", "B"])

    selected_links_df = path_links_df.merge(
        links_df[["A", "B", "model_link_id"]],
        how="left",
        on=["A", "B"],
        indicator=True,
    )
    selected_link_ds = selected_links_df.model_link_id.unique().tolist()

    if not ignore_missing:
        missing_links_df = selected_links_df.loc[
            selected_links_df._merge == "left_only", ["A", "B"]
        ]
        if len(missing_links_df):
            WranglerLogger.error(
                f"! Path links missing in links_df \n {missing_links_df}"
            )
            raise ValueError("Path links missing in links_df.")

    return filter_links_to_ids(links_df, selected_link_ds)


def _filter_link_to_ml_access_egress_points(
    links_df: pd.DataFrame, match_col: str, match_node_col: str
) -> pd.DataFrame:
    """_summary_

    Args:
        links_df (pd.DataFrame): links dataframe.
        match_col (str): column in links_df which determines which links are access/egress
            points. In RoadLinksTable that is either "ML_access_point" or "ML_egress_point"
        match_node_col (str): The node column to match the match_col to. In RoadLinksTable
            that is either "A" or "B" which corresponds with "ML_access_point" or "ML_egress_point"
            respectively.

    Returns:
        pd.DataFrame: _description_
    """
    if match_col not in links_df.columns or links_df[match_col].isnull().all():
        WranglerLogger.warning(
            f"{match_col} either doesn't exist or is all null;\
        Assuming all managed lane links are access/egress points."
        )
        return filter_links_managed_lanes(links_df)

    exploded_df = links_df.explode(match_col)
    mask_all = exploded_df[match_col] == "all"
    mask_match = exploded_df[match_node_col] == exploded_df[match_col]

    return links_df.loc[exploded_df[mask_all | mask_match].index]


def filter_links_to_ml_egress_points(links_df: pd.DataFrame) -> pd.DataFrame:
    return _filter_link_to_ml_access_egress_points(
        links_df,
        match_col="ML_egress_point",
        match_node_col="B",
    )


def filter_links_to_ml_access_points(links_df: pd.DataFrame) -> pd.DataFrame:
    return _filter_link_to_ml_access_egress_points(
        links_df,
        match_col="ML_access_point",
        match_node_col="A",
    )

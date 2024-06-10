from network_wrangler.logger import WranglerLogger
from network_wrangler.models.gtfs.tables import (
    WranglerShapesTable,
    WranglerStopTimesTable,
)
from network_wrangler.transit.feed.feed import Feed


import pandas as pd

from network_wrangler.transit.feed.transit_links import unique_stop_time_links
from network_wrangler.transit.feed.transit_links import unique_shape_links


def transit_nodes_without_road_nodes(
    feed: Feed, nodes_df: "RoadwayNodes" = None, rd_field: str = "model_node_id"
) -> list[int]:
    """Validate all of a transit feeds node foreign keys exist in referenced roadway nodes.

    Args:
        nodes_df (pd.DataFrame, optional): Nodes dataframe from roadway network to validate
            foreign key to. Defaults to self.roadway_net.nodes_df

    Returns:
        boolean indicating if relationships are all valid
    """
    feed_nodes_series = [
        feed.stops["model_node_id"],
        feed.shapes["shape_model_node_id"],
        feed.stop_times["model_node_id"],
    ]
    tr_nodes = set(pd.concat(feed_nodes_series).unique())
    rd_nodes = set(nodes_df[rd_field].unique().tolist())
    # nodes in tr_nodes but not rd_nodes
    missing_tr_nodes = tr_nodes - rd_nodes

    if missing_tr_nodes:
        WranglerLogger.error(
            f"! Transit nodes in missing in roadway network: \n {missing_tr_nodes}"
        )
    return missing_tr_nodes


def shape_links_without_road_links(
    tr_shapes: WranglerShapesTable,
    rd_links_df: "RoadwayLinks",
) -> pd.DataFrame:
    """Validate that links in transit shapes exist in referenced roadway links.

    Args:
        tr_shapes_df: transit shapes from shapes.txt to validate foreign key to.
        rd_links_df: Links dataframe from roadway network to validate

    Returns:
        df with shape_id and A, B
    """
    tr_shape_links = unique_shape_links(tr_shapes)
    # WranglerLogger.debug(f"Unique shape links: \n {tr_shape_links}")
    rd_links_transit_ok = rd_links_df[
        (rd_links_df["drive_access"] == True)
        | (rd_links_df["bus_only"] == True)
        | (rd_links_df["rail_only"] == True)
    ]

    merged_df = tr_shape_links.merge(
        rd_links_transit_ok[["A", "B"]],
        how="left",
        on=["A", "B"],
        indicator=True,
    )

    missing_links_df = merged_df.loc[
        merged_df._merge == "left_only", ["shape_id", "A", "B"]
    ]
    if len(missing_links_df):
        WranglerLogger.error(
            f"! Transit shape links missing in roadway network: \n {missing_links_df}"
        )
    return missing_links_df[["shape_id", "A", "B"]]


def stop_times_without_road_links(
    tr_stop_times: WranglerStopTimesTable,
    rd_links_df: "RoadwayLinks",
) -> pd.DataFrame:
    """Validate that links in transit shapes exist in referenced roadway links.

    Args:
        tr_stop_times: transit stop_times from stop_times.txt to validate foreign key to.
        rd_links_df: Links dataframe from roadway network to validate

    Returns:
        df with shape_id and A, B
    """
    tr_links = unique_stop_time_links(tr_stop_times)

    rd_links_transit_ok = rd_links_df[
        (rd_links_df["drive_access"] == True)
        | (rd_links_df["bus_only"] == True)
        | (rd_links_df["rail_only"] == True)
    ]

    merged_df = tr_links.merge(
        rd_links_transit_ok[["A", "B"]],
        how="left",
        on=["A", "B"],
        indicator=True,
    )

    missing_links_df = merged_df.loc[
        merged_df._merge == "left_only", ["trip_id", "A", "B"]
    ]
    if len(missing_links_df):
        WranglerLogger.error(
            f"! Transit stop_time links missing in roadway network: \n {missing_links_df}"
        )
    return missing_links_df[["trip_id", "A", "B"]]


def transit_road_net_consistency(feed: Feed, road_net: "RoadwayNetwork") -> bool:
    """Checks foreign key and network link relationships between transit feed and a road_net.

    Args:
        transit_net: Feed.
        road_net (RoadwayNetwork): Roadway network to check relationship with.

    Returns:
        bool: boolean indicating if road_net is consistent with transit network.
    """
    _missing_links = shape_links_without_road_links(feed.shapes, road_net.links_df)
    _missing_nodes = transit_nodes_without_road_nodes(feed, road_net.nodes_df)
    _consistency = _missing_links.empty and not _missing_nodes
    return _consistency

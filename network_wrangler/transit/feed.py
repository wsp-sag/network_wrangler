from typing import Union, Literal

import pandas as pd
from pydantic import validate_call

from ..models._base.db import DBModelMixin
from ..models.gtfs.tables import (
    AgenciesTable,
    WranglerStopsTable,
    RoutesTable,
    TripsTable,
    WranglerStopTimesTable,
    WranglerShapesTable,
    FrequenciesTable,
)

from ..utils import update_df_by_col_value
from ..utils.net import point_seq_to_links
from ..logger import WranglerLogger

from .convert import gtfs_to_wrangler_stop_times


# Raised when there is an issue with the validation of the GTFS data.
class FeedValidationError(Exception):
    pass


class Feed(DBModelMixin):
    """
    Wrapper class around Wrangler flavored GTFS feed.

    Most functionality derives from mixin class DBModelMixin which provides:
    - validation of tables to schemas when setting a table attribute (e.g. self.trips = trips_df)
    - validation of fks when setting a table attribute (e.g. self.trips = trips_df)
    - hashing and deep copy functionality
    - overload of __eq__ to apply only to tables in table_names.
    - convenience methods for accessing tables

    Attributes:
        table_names: list of table names in GTFS feed.
        tables: list tables as dataframes.
        stop_times: stop_times dataframe with roadway node_ids
        stops: stops dataframe
        shapes: shapes dataframe
        trips: trips dataframe
        frequencies: frequencies dataframe
        routes: route dataframe
        net: TransitNetwork object
    """

    # the ordering here matters because the stops need to be added before stop_times if
    # stop times needs to be converted
    _table_models = {
        "agencies": AgenciesTable,
        "frequencies": FrequenciesTable,
        "routes": RoutesTable,
        "shapes": WranglerShapesTable,
        "stops": WranglerStopsTable,
        "trips": TripsTable,
        "stop_times": WranglerStopTimesTable,
    }

    _converters = {"stop_times": gtfs_to_wrangler_stop_times}

    table_names = [
        "frequencies",
        "routes",
        "shapes",
        "stops",
        "trips",
        "stop_times",
    ]

    optional_table_names = ["agencies"]

    def __init__(self, **kwargs):
        self._net = None
        self.initialize_tables(**kwargs)

        # Set extra provided attributes but just FYI in logger.
        extra_attr = {k: v for k, v in kwargs.items() if k not in self.table_names}
        if extra_attr:
            WranglerLogger.info(
                f"Adding additional attributes to Feed: {extra_attr.keys()}"
            )
        for k, v in extra_attr:
            self.__setattr__(k, v)

    def set_by_id(
        self,
        table_name: str,
        set_df: pd.DataFrame,
        id_property: str = "trip_id",
        properties: list[str] = None,
    ):
        """
        Set property values in a specific table for a list of IDs.

        Args:
            table_name (str): Name of the table to modify.
            set_df (pd.DataFrame): DataFrame with columns 'trip_id' and 'value' containing
                trip IDs and values to set for the specified property.
            id_property: Property to use as ID to set by. Defaults to "trip_id.
            properties: List of properties to set which are in set_df. If not specified, will set
                all properties.
        """
        table_df = self.get_table(table_name)
        updated_df = update_df_by_col_value(
            table_df, set_df, id_property, properties=properties
        )
        self.__dict__[table_name] = updated_df


PickupDropoffAvailability = Union[
    Literal["either"],
    Literal["both"],
    Literal["pickup_only"],
    Literal["dropoff_only"],
    Literal["any"],
]


def trips_for_shape_id(feed: Feed, shape_id: str) -> TripsTable:
    """Returns a trips records for a given shape_id."""
    return feed.trips.loc[feed.trips.shape_id == shape_id]


def trip_ids_for_shape_id(feed: Feed, shape_id: str) -> list[str]:
    return trips_for_shape_id(feed, shape_id)["trip_id"].unique().tolist()


def shape_with_stops_for_shape_id(feed: Feed, shape_id: str) -> WranglerShapesTable:
    trip_ids = trip_ids_for_shape_id(feed, shape_id)
    all_shape_stop_times = pd.concat(
        [shapes_with_stop_id_for_trip_id(feed, t) for t in trip_ids]
    )
    shapes_with_stops = all_shape_stop_times[all_shape_stop_times["stop_id"].notna()]
    shapes_with_stops = shapes_with_stops.sort_values(by=["shape_pt_sequence"])
    return shapes_with_stops


def stop_times_for_trip_id(feed: Feed, trip_id: str) -> WranglerStopsTable:
    """Returns a stop_time records for a given trip_id."""
    stop_times = feed.stop_times.loc[feed.stop_times.trip_id == trip_id]
    return stop_times.sort_values(by=["stop_sequence"])


@validate_call
def stop_times_for_pickup_dropoff_trip_id(
    feed,
    trip_id: str,
    pickup_dropoff: PickupDropoffAvailability = "either",
) -> list[str]:
    """Returns stop_times for a given trip_id based on pickup type.

    GTFS values for pickup_type and drop_off_type"
        0 or empty - Regularly scheduled pickup/dropoff.
        1 - No pickup/dropoff available.
        2 - Must phone agency to arrange pickup/dropoff.
        3 - Must coordinate with driver to arrange pickup/dropoff.

    args:
        feed: Feed object
        trip_id: trip_id to get stop pattern for
        pickup_dropoff: str indicating logic for selecting stops based on pickup and dropoff
            availability at stop. Defaults to "either".
            "any": all stoptime records
            "either": either pickup_type or dropoff_type != 1
            "both": both pickup_type and dropoff_type != 1
            "pickup_only": dropoff = 1; pickup != 1
            "dropoff_only":  pickup = 1; dropoff != 1
    """
    trip_stop_pattern = stop_times_for_trip_id(feed, trip_id)

    if pickup_dropoff == "any":
        return trip_stop_pattern

    pickup_type_selection = {
        "either": (trip_stop_pattern.pickup_type != 1)
        | (trip_stop_pattern.drop_off_type != 1),
        "both": (trip_stop_pattern.pickup_type != 1)
        & (trip_stop_pattern.drop_off_type != 1),
        "pickup_only": (trip_stop_pattern.pickup_type != 1)
        & (trip_stop_pattern.drop_off_type == 1),
        "dropoff_only": (trip_stop_pattern.drop_off_type != 1)
        & (trip_stop_pattern.pickup_type == 1),
    }

    selection = pickup_type_selection[pickup_dropoff]
    trip_stops = trip_stop_pattern[selection]

    return trip_stops


def route_ids_for_trip_ids(feed: Feed, trip_ids: list[str]) -> pd.DataFrame:
    """Returns route ids for given list of trip_ids"""
    return feed.trips[feed.trips["trip_id"].isin(trip_ids)].route_id.unique().tolist()


def routes_for_trip_ids(feed: Feed, trip_ids: list[str]) -> pd.DataFrame:
    """Returns route records for given list of trip_ids"""
    route_ids = route_ids_for_trip_ids(feed, trip_ids)
    return feed.routes.loc[feed.routes.route_id.isin(route_ids)]


def shape_id_for_trip_id(feed: Feed, trip_id: str) -> str:
    """Returns a shape_id for a given trip_id."""
    return feed.trips.loc[feed.trips.trip_id == trip_id, "shape_id"].values[0]


def shape_ids_for_trip_ids(feed: Feed, trip_ids: list[str]) -> list[str]:
    """Returns a list of shape_ids for a given list of trip_ids."""
    return feed.trips[feed.trips["trip_id"].isin(trip_ids)].shape_id.unique().tolist()


def shapes_for_trip_id(feed: Feed, trip_id: str) -> pd.DataFrame:
    """Returns shape records for a given trip_id."""
    shape_id = shape_id_for_trip_id(feed, trip_id)
    return feed.shapes.loc[feed.shapes.shape_id == shape_id]


def shapes_for_trip_ids(feed: Feed, trip_ids: list[str]) -> pd.DataFrame:
    """Returns shape records for a given trip_id."""
    shape_ids = shape_ids_for_trip_ids(feed, trip_ids)
    return feed.shapes.loc[feed.shapes.shape_id.isin(shape_ids)]


def shapes_for_shape_id(feed: Feed, shape_id: str) -> pd.DataFrame:
    shapes = feed.shapes.loc[feed.shapes.shape_id == shape_id]
    return shapes.sort_values(by=["shape_pt_sequence"])


def stops_for_trip_id(
    feed: Feed, trip_id: str, pickup_dropoff: PickupDropoffAvailability = "any"
) -> list[str]:
    """Returns stops.txt which are used for a given trip_id"""
    stop_ids = stop_id_pattern_for_trip(feed, trip_id, pickup_dropoff=pickup_dropoff)
    return feed.stops.loc[feed.stops.stop_id.isin(stop_ids)]


def node_pattern_for_shape_id(feed: Feed, shape_id: str) -> list[int]:
    """Returns node pattern of a shape.

    args:
        feed: Feed object
        shape_id: string identifier of the shape.
    """
    shape_df = feed.shapes.loc[feed.shapes["shape_id"] == shape_id]
    shape_df = shape_df.sort_values(by=["shape_pt_sequence"])
    return shape_df["shape_model_node_id"].to_list()


def shapes_with_stop_id_for_trip_id(
    feed: Feed, trip_id: str, pickup_dropoff: PickupDropoffAvailability = "either"
) -> pd.DataFrame:
    """Returns shapes.txt for a given trip_id with the stop_id added based on pickup_type.

    args:
        feed: Feed object
        trip_id: trip id to select
        pickup_dropoff: str indicating logic for selecting stops based on piackup and dropoff
            availability at stop. Defaults to "either".
            "either": either pickup_type or dropoff_type > 0
            "both": both pickup_type or dropoff_type > 0
            "pickup_only": only pickup > 0
            "dropoff_only": only dropoff > 0
    """

    shapes = shapes_for_trip_id(feed, trip_id)
    trip_stop_times = stop_times_for_pickup_dropoff_trip_id(
        feed, trip_id, pickup_dropoff=pickup_dropoff
    )

    stop_times_cols = [
        "stop_id",
        "trip_id",
        "pickup_type",
        "drop_off_type",
        "model_node_id",
    ]

    shape_with_trip_stops = shapes.merge(
        trip_stop_times[stop_times_cols],
        how="left",
        right_on="model_node_id",
        left_on="shape_model_node_id",
    )
    shape_with_trip_stops = shape_with_trip_stops.sort_values(by=["shape_pt_sequence"])
    shape_with_trip_stops = shape_with_trip_stops.drop(columns=["model_node_id"])
    return shape_with_trip_stops


def find_nearest_stops(feed, trip_id, node_id, pickup_dropoff="either"):
    shapes = shapes_with_stop_id_for_trip_id(
        feed, trip_id, pickup_dropoff=pickup_dropoff
    )
    WranglerLogger.debug(f"Looking for stops near node_id: {node_id}")
    if node_id not in shapes["shape_model_node_id"].values:
        raise ValueError(f"Node ID {node_id} not found in shapes for trip {trip_id}")
    # Find index of node_id in shapes
    node_idx = shapes[shapes["shape_model_node_id"] == node_id].index[0]

    # Find stops before and after new stop in shapes sequence
    nodes_before = shapes.loc[: node_idx - 1]
    stops_before = nodes_before.loc[nodes_before["stop_id"].notna()]
    if stops_before.empty:
        stop_node_before = 0
    else:
        stop_node_before = stops_before.iloc[-1]["shape_model_node_id"]

    nodes_after = shapes.loc[node_idx + 1 :]
    stops_after = nodes_after.loc[nodes_after["stop_id"].notna()]
    if stops_after.empty:
        stop_node_after = 0
    else:
        stop_node_after = stops_after.iloc[0]["shape_model_node_id"]

    return stop_node_before, stop_node_after


def stop_times_for_trip_node_segment(
    feed, trip_id, node_id_start, node_id_end, include_start=True, include_end=True
) -> WranglerStopTimesTable:
    """Returns stop_times for a given trip_id between two nodes or with those nodes included.

    args:
        feed: Feed object
        trip_id: trip id to select
        node_id_start: int of the starting node
        node_id_end: int of the ending node
        include_start: bool indicating if the start node should be included in the segment.
            Defaults to True.
        include_end: bool indicating if the end node should be included in the segment.
            Defaults to True.
    """
    stop_times = stop_times_for_trip_id(feed, trip_id)
    start_idx = stop_times[stop_times["model_node_id"] == node_id_start].index[0]
    end_idx = stop_times[stop_times["model_node_id"] == node_id_end].index[0]
    if not include_start:
        start_idx += 1
    if include_end:
        end_idx += 1
    return stop_times.loc[start_idx:end_idx]


def node_ids_for_stop_id(feed, stop_id: Union[list[str], str]) -> Union[list[int], int]:
    """Returns node_ids from one or more stop_ids.

    stop_id: a stop_id string or a list of stop_id strings
    """
    if isinstance(stop_id, list):
        return [node_ids_for_stop_id(feed, s) for s in stop_id]
    elif isinstance(stop_id, str):
        return feed.stops.at[feed.stops["stop_id"] == stop_id, "model_node_id"]
    else:
        raise ValueError(
            f"Expecting list of strings or string for stop_id; got {type(stop_id)}"
        )


def node_is_stop(
    feed,
    node_id: Union[int, list[int]],
    trip_id: str,
    pickup_dropoff: PickupDropoffAvailability = "either",
) -> Union[bool, list[bool]]:
    """Returns a boolean indicating if a node (or a list of nodes) is (are) stops for a given trip_id.

    args:
        feed: Feed object
        node_id: node ID for roadway
        trip_id: trip_id to get stop pattern for
        pickup_dropoff: str indicating logic for selecting stops based on piackup and dropoff
            availability at stop. Defaults to "either".
            "either": either pickup_type or dropoff_type > 0
            "both": both pickup_type or dropoff_type > 0
            "pickup_only": only pickup > 0
            "dropoff_only": only dropoff > 0
    """
    trip_stop_nodes = stops_for_trip_id(feed, trip_id, pickup_dropoff=pickup_dropoff)[
        "model_node_id"
    ]
    if isinstance(node_id, list):
        return [n in trip_stop_nodes.values for n in node_id]
    return node_id in trip_stop_nodes.values


@validate_call
def stop_id_pattern_for_trip(
    feed, trip_id: str, pickup_dropoff: PickupDropoffAvailability = "either"
) -> list[str]:
    """Returns a stop pattern for a given trip_id given by a list of stop_ids.

    args:
        feed: Feed object
        trip_id: trip_id to get stop pattern for
        pickup_dropoff: str indicating logic for selecting stops based on piackup and dropoff
            availability at stop. Defaults to "either".
            "either": either pickup_type or dropoff_type > 0
            "both": both pickup_type or dropoff_type > 0
            "pickup_only": only pickup > 0
            "dropoff_only": only dropoff > 0
    """
    trip_stops = stop_times_for_pickup_dropoff_trip_id(
        feed, trip_id, pickup_dropoff=pickup_dropoff
    )
    return trip_stops.stop_id.to_list()


def shapes_to_shape_links(shapes: WranglerShapesTable) -> pd.DataFrame:
    return point_seq_to_links(
        shapes,
        id_field="shape_id",
        seq_field="shape_pt_sequence",
        node_id_field="shape_model_node_id",
    )


def stop_times_to_stop_times_links(
    stop_times: WranglerStopTimesTable, from_field="A", to_field="B"
) -> pd.DataFrame:
    return point_seq_to_links(
        stop_times,
        id_field="trip_id",
        seq_field="stop_sequence",
        node_id_field="model_node_id",
        from_field=from_field,
        to_field=to_field,
    )


def unique_shape_links(
    shapes: WranglerShapesTable, from_field="A", to_field="B"
) -> pd.DataFrame:
    shape_links = shapes_to_shape_links(shapes)
    # WranglerLogger.debug(f"Shape links: \n {shape_links[['shape_id', from_field, to_field]]}")

    _agg_dict = {"shape_id": list}
    _opt_fields = [f"shape_pt_{v}_{t}" for v in ["lat", "lon"] for t in [from_field, to_field]]
    for f in _opt_fields:
        if f in shape_links:
            _agg_dict[f] = "first"

    unique_shape_links = (
        shape_links.groupby([from_field, to_field])
        .agg(_agg_dict)
        .reset_index()
    )
    return unique_shape_links


def unique_stop_time_links(
    stop_times: WranglerStopTimesTable, from_field="A", to_field="B"
) -> pd.DataFrame:
    links = stop_times_to_stop_times_links(
        stop_times, from_field=from_field, to_field=to_field
    )
    unique_links = (
        links.groupby([from_field, to_field])["trip_id"].apply(list).reset_index()
    )
    return unique_links


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


def stop_count_by_trip(
    stop_times: WranglerStopTimesTable,
) -> pd.DataFrame:
    """Returns dataframe with trip_id and stop_count from stop_times."""
    stops_count = stop_times.groupby("trip_id").size()
    return stops_count.reset_index(name="stop_count")


def filter_stop_times_to_min_stops(
    stop_times: WranglerStopTimesTable, min_stops: int
) -> WranglerStopTimesTable:
    """Filter stop_times dataframe to only the records which have >= min_stops for the trip."""
    stop_ct_by_trip_df = stop_count_by_trip(stop_times)

    # Filter to obtain DataFrame of trips with stop counts >= min_stops
    min_stop_ct_trip_df = stop_ct_by_trip_df[stop_ct_by_trip_df.stop_count >= min_stops]
    if len(min_stop_ct_trip_df) == 0:
        raise ValueError(f"No trips meet threshold of minimum stops: {min_stops}")
    WranglerLogger.debug(
        f"Found {len(min_stop_ct_trip_df)} trips with a minimum of {min_stops} stops."
    )

    # Filter the original stop_times DataFrame to only include trips with >= min_stops
    filtered_stop_times = stop_times.merge(
        min_stop_ct_trip_df["trip_id"], on="trip_id", how="inner"
    )
    WranglerLogger.debug(
        f"Filter stop times to {len(filtered_stop_times)}/{len(stop_times)}\
            w/a minimum of {min_stops} stops."
    )

    return filtered_stop_times


def filter_stop_times_to_stops(
    stop_times: WranglerStopTimesTable, stops: WranglerStopsTable
) -> WranglerStopTimesTable:
    """Filter stop_times dataframe to only have stop_times associated with stops records."""
    _sel_stops = stops.stop_id.unique().tolist()
    filtered_stop_times = stop_times[stop_times.stop_id.isin(_sel_stops)]
    WranglerLogger.debug(
        f"Filtered stop_times to {len(filtered_stop_times)}/{len(stop_times)} \
                         records that referenced one of {len(stops)} stops."
    )
    return filtered_stop_times


def filter_stops_to_stop_times(
    stops: WranglerStopsTable, stop_times: WranglerStopTimesTable
) -> WranglerStopsTable:
    """Filter stops dataframe to only have stops associated with stop_times records."""
    _sel_stops_ge_min = stop_times.stop_id.unique().tolist()
    filtered_stops = stops[stops.stop_id.isin(_sel_stops_ge_min)]
    WranglerLogger.debug(
        f"Filtered stops to {len(filtered_stops)}/{len(stops)} \
                         records that referenced one of {len(stop_times)} stop_times."
    )
    return filtered_stops


def filter_trips_to_stop_times(
    trips: TripsTable, stop_times: WranglerStopTimesTable
) -> TripsTable:
    """Filter trips dataframe to records associated with stop_time records."""
    _sel_trips = stop_times.trip_id.unique().tolist()
    filtered_trips = trips[trips.trip_id.isin(_sel_trips)]
    WranglerLogger.debug(
        f"Filtered trips to {len(filtered_trips)}/{len(trips)} \
                         records that referenced one of {len(stop_times)} stop_times."
    )
    return filtered_trips


def filter_routes_to_trips(routes: RoutesTable, trips: TripsTable) -> RoutesTable:
    """Filter routes dataframe to records associated with trip records."""
    _sel_routes = trips.route_id.unique().tolist()
    filtered_routes = routes[routes.route_id.isin(_sel_routes)]
    WranglerLogger.debug(
        f"Filtered routes to {len(filtered_routes)}/{len(routes)} \
                         records that referenced one of {len(trips)} trips."
    )
    return filtered_routes


def filter_shapes_to_trips(
    shapes: WranglerShapesTable, trips: TripsTable
) -> WranglerShapesTable:
    """Filter shapes dataframe to records associated with trips table."""
    _sel_shapes = trips.shape_id.unique().tolist()
    filtered_shapes = shapes[shapes.shape_id.isin(_sel_shapes)]
    WranglerLogger.debug(
        f"Filtered shapes to {len(filtered_shapes)}/{len(shapes)} \
                         records that referenced one of {len(trips)} trips."
    )
    return filtered_shapes


def filter_frequencies_to_trips(
    frequencies: FrequenciesTable, trips: TripsTable
) -> FrequenciesTable:
    """Filter frequenceis dataframe to records associated with trips table."""
    _sel_trips = trips.trip_id.unique().tolist()
    filtered_frequencies = frequencies[frequencies.trip_id.isin(_sel_trips)]
    WranglerLogger.debug(
        f"Filtered frequencies to {len(filtered_frequencies)}/{len(frequencies)} \
                         records that referenced one of {len(trips)} trips."
    )
    return filtered_frequencies


def shape_links_to_segments(shape_links) -> pd.DataFrame:
    """Convert shape_links to segments by shape_id with segments of continuous shape_pt_sequence.

    Returns: DataFrame with shape_id, segment_id, segment_start_shape_pt_seq, segment_end_shape_pt_seq
    """

    shape_links['gap'] = shape_links.groupby('shape_id')['shape_pt_sequence_A'].diff().gt(1)
    shape_links['segment_id'] = shape_links.groupby('shape_id')['gap'].cumsum()

    # Define segment starts and ends
    segment_definitions = shape_links.groupby(['shape_id', 'segment_id']).agg(
        segment_start_shape_pt_seq=('shape_pt_sequence_A', 'min'),
        segment_end_shape_pt_seq=('shape_pt_sequence_B', 'max')
    ).reset_index()

    # Optionally calculate segment lengths for further uses
    segment_definitions['segment_length'] = (
        segment_definitions['segment_end_shape_pt_seq'] - segment_definitions['segment_start_shape_pt_seq'] + 1
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
    idx = segments.groupby('shape_id')['segment_length'].idxmax()
    longest_segments = segments.loc[idx]
    return longest_segments


def filter_shapes_to_segments(shapes: WranglerShapesTable, segments: pd.DataFrame) -> WranglerShapesTable:
    """Filter shapes dataframe to records associated with segments dataframe.

    Args:
        shapes: shapes dataframe to filter
        segments: segments dataframe to filter by with shape_id, segment_start_shape_pt_seq,
            segment_end_shape_pt_seq . Should have one record per shape_id.

    Returns:
        filtered shapes dataframe
    """
    shapes_w_segs = shapes.merge(segments, on='shape_id', how="left")

    # Retain only those points within the segment sequences
    filtered_shapes = shapes_w_segs[
        (shapes_w_segs['shape_pt_sequence'] >= shapes_w_segs['segment_start_shape_pt_seq']) &
        (shapes_w_segs['shape_pt_sequence'] <= shapes_w_segs['segment_end_shape_pt_seq'])
    ]

    drop_cols = ['segment_id', 'segment_start_shape_pt_seq', 'segment_end_shape_pt_seq', 'segment_length']
    filtered_shapes = filtered_shapes.drop(columns=drop_cols)

    return filtered_shapes


def filter_shapes_to_links(shapes: WranglerShapesTable, links_df: pd.DataFrame
                           ) -> WranglerShapesTable:
    """Filter shapes dataframe to records associated with links dataframe.

    EX:

    > shapes = pd.DataFrame({
        "shape_id": ["1", "1", "1", "1", "2", "2", "2", "2", "2"],
        "shape_pt_sequence": [1, 2, 3, 4, 1, 2, 3, 4, 5],
        "shape_model_node_id": [1, 2, 3, 4, 2, 3, 1, 5, 4]
    })

    > links_df = pd.DataFrame({
        "A": [1, 2, 3],
        "B": [2, 3, 4]
    })

    > shapes

    shape_id   shape_pt_sequence   shape_model_node_id *should retain*
    1          1                  1                        TRUE
    1          2                  2                        TRUE
    1          3                  3                        TRUE
    1          4                  4                        TRUE
    1          5                  5                       FALSE
    2          1                  1                        TRUE
    2          2                  2                        TRUE
    2          3                  3                        TRUE
    2          4                  1                       FALSE
    2          5                  5                       FALSE
    2          6                  4                       FALSE
    2          7                  1                       FALSE - not largest segment
    2          8                  2                       FALSE - not largest segment

    > links_df

    A   B
    1   2
    2   3
    3   4
    """

    """
    > shape_links

    shape_id  shape_pt_sequence_A  shape_model_node_id_A shape_pt_sequence_B shape_model_node_id_B
    1          1                        1                       2                        2
    1          2                        2                       3                        3
    1          3                        3                       4                        4
    1          4                        4                       5                        5
    2          1                        1                       2                        2
    2          2                        2                       3                        3
    2          3                        3                       4                        1
    2          4                        1                       5                        5
    2          5                        5                       6                        4
    2          6                        4                       7                        1
    2          7                        1                       8                        2
    """
    shape_links = shapes_to_shape_links(shapes)

    """
    > shape_links_w_links

    shape_id  shape_pt_sequence_A shape_pt_sequence_B  A  B
    1          1                         2             1  2
    1          2                         3             2  3
    1          3                         4             3  4
    2          1                         2             1  2
    2          2                         3             2  3
    2          7                         8             1  2
    """

    shape_links_w_links = shape_links.merge(
        links_df[["A", "B"]],
        how="inner",
        on=["A", "B"],
    )
    _debug_AB = [
        {"A": 45983, "B": 57484},
        {"A": 45983, "B": 171267},
        {"A": 171268, "B": 171269},
        {"A": 171270, "B": 57484},
    ]
    WranglerLogger.debug(f"DEBUG AB:\n\
                         {shape_links_w_links[shape_links_w_links[['A', 'B']].isin(_debug_AB).all(axis=1)]}")

    """
    Find largest segment of each shape_id that is in the links

    > longest_shape_segments
    shape_id, segment_id, segment_start_shape_pt_seq, segment_end_shape_pt_seq
    1          1                        1                       4
    2          1                        1                       3
    """
    longest_shape_segments = shape_links_to_longest_shape_segments(shape_links_w_links)

    """
    > shapes

    shape_id   shape_pt_sequence   shape_model_node_id
    1          1                  1
    1          2                  2
    1          3                  3
    1          4                  4
    2          1                  1
    2          2                  2
    2          3                  3
    """
    filtered_shapes = filter_shapes_to_segments(shapes, longest_shape_segments)
    filtered_shapes = filtered_shapes.reset_index(drop=True)
    return filtered_shapes


def merge_shapes_to_stop_times(
    stop_times: WranglerStopTimesTable, shapes: WranglerShapesTable, trips: TripsTable
) -> WranglerStopTimesTable:
    """Add shape_id and shape_pt_sequence to stop_times dataframe.

    Args:
        stop_times: stop_times dataframe to add shape_id and shape_pt_sequence to.
        shapes: shapes dataframe to add to stop_times.
        trips: trips dataframe to link stop_times to shapes

    Returns:
        stop_times dataframe with shape_id and shape_pt_sequence added.
    """
    stop_times_w_shape_id = stop_times.merge(
        trips[["trip_id", "shape_id"]],
        on="trip_id",
        how="left"
    )

    stop_times_w_shapes = stop_times_w_shape_id.merge(
        shapes,
        how="left",
        left_on=["shape_id", "model_node_id"],
        right_on=["shape_id", "shape_model_node_id"],
    )
    stop_times_w_shapes = stop_times_w_shapes.drop(columns=["shape_model_node_id"])
    return stop_times_w_shapes


def filter_stop_times_to_longest_segments(stop_times: WranglerStopTimesTable) -> pd.DataFrame:
    """Find the longest segment of each trip_id that is in the stop_times.

    Segment ends defined based on interruptions in `stop_sequence`.
    """
    stop_times = stop_times.sort_values(by=['trip_id', 'stop_sequence'])

    stop_times['prev_stop_sequence'] = stop_times.groupby('trip_id')['stop_sequence'].shift(1)
    stop_times['gap'] = (stop_times['stop_sequence'] - stop_times['prev_stop_sequence']).ne(1) | stop_times['prev_stop_sequence'].isna()

    stop_times['segment_id'] = stop_times['gap'].cumsum()
    # WranglerLogger.debug(f"stop_times with segment_id:\n{stop_times}")

    # Calculate the length of each segment
    segment_lengths = stop_times.groupby(['trip_id', 'segment_id']).size().reset_index(name='segment_length')

    # Identify the longest segment for each trip
    idx = segment_lengths.groupby('trip_id')['segment_length'].idxmax()
    longest_segments = segment_lengths.loc[idx]

    # Merge longest segment info back to stop_times
    stop_times = stop_times.merge(
        longest_segments[['trip_id', 'segment_id']],
        on=['trip_id', 'segment_id'],
        how='inner'
    )

    # Drop temporary columns used for calculations
    stop_times.drop(columns=['prev_stop_sequence', 'gap', 'segment_id'], inplace=True)
    # WranglerLogger.debug(f"stop_timesw/longest segments:\n{stop_times}")
    return stop_times


def filter_stop_times_to_shapes(
    stop_times: WranglerStopTimesTable,
    shapes: WranglerShapesTable,
    trips: TripsTable
) -> WranglerStopTimesTable:
    """Filter stop_times dataframe to records associated with shapes dataframe.

    Where multiple segments of stop_times are found to match shapes, retain only the longest.

    Args:
        stop_times: stop_times dataframe to filter
        shapes: shapes dataframe to stop_times to.
        trips: trips to link stop_times to shapess

    Returns:
        filtered stop_times dataframe

    EX:
    * should be retained
    > stop_times

    trip_id   stop_sequence   stop_id   model_node_id
    *t1          1                  t1       1
    *t1          2                  t2       2
    *t1          3                  t3       3
    t1           4                  t5       5
    *t2          1                  t1       1
    *t2          2                  t3       3
    t2           3                  t7       7

    > shapes

    shape_id   shape_pt_sequence   shape_model_node_id
    s1          1                  1
    s1          2                  2
    s1          3                  3
    s1          4                  4
    s2          1                  1
    s2          2                  2
    s2          3                  3

    > trips

    trip_id   shape_id
    t1          s1
    t2          s2
    """

    """
    > stop_times_w_shapes

    trip_id   stop_sequence   stop_id   model_node_id   shape_id   shape_pt_sequence
    *t1          1                  t1       1               s1          1
    *t1          2                  t2       2               s1          2
    *t1          3                  t3       3               s1          3
    t1           4                  t5       5               NA          NA
    *t2          1                  t1       1               s2          1
    *t2          2                  t3       3               s2          2
    t2           3                  t7       7               NA          NA

    """
    stop_times_w_shapes = merge_shapes_to_stop_times(stop_times, shapes, trips)
    # WranglerLogger.debug(f"stop_times_w_shapes :\n{stop_times_w_shapes}")
    """
    > stop_times_w_shapes

    trip_id   stop_sequence   stop_id   model_node_id   shape_id   shape_pt_sequence
    *t1          1                  t1       1               s1          1
    *t1          2                  t2       2               s1          2
    *t1          3                  t3       3               s1          3
    *t2          1                  t1       1               s2          1
    *t2          2                  t3       3               s2          2

    """
    filtered_stop_times = stop_times_w_shapes[stop_times_w_shapes["shape_pt_sequence"].notna()]
    # WranglerLogger.debug(f"filtered_stop_times:\n{filtered_stop_times}")

    # Filter out any stop_times the shape_pt_sequence is not ascending
    valid_stop_times = filtered_stop_times.groupby('trip_id').filter(
        lambda x: x['shape_pt_sequence'].is_monotonic_increasing
    )
    # WranglerLogger.debug(f"valid_stop_times:\n{valid_stop_times}")

    valid_stop_times = valid_stop_times.drop(columns=["shape_id", "shape_pt_sequence"])

    longest_valid_stop_times = filter_stop_times_to_longest_segments(valid_stop_times)
    longest_valid_stop_times = longest_valid_stop_times.reset_index(drop=True)

    return longest_valid_stop_times


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

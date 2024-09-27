"""Functions for adding a transit route to a TransitNetwork."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional, Union

from ...logger import WranglerLogger

import pandas as pd
from pandera.typing import DataFrame as paDataFrame

from ...utils.time import str_to_time_list
from ...utils.utils import fill_str_ids
from ...utils.models import fill_df_with_defaults_from_model
from ...models._base.types import TimeString
from ...models.gtfs.tables import (
    TripsTable,
    WranglerShapesTable,
    WranglerStopTimesTable,
    WranglerStopsTable,
    FrequenciesTable,
)

if TYPE_CHECKING:
    from ...roadway.network import RoadwayNetwork
    from ..network import TransitNetwork
    from ..feed.feed import Feed


class TransitRouteAddError(Exception):
    """Error raised when applying add transit route."""

    pass


def apply_transit_route_addition(
    net: TransitNetwork,
    transit_route_addition: dict,
    reference_road_net: Optional[RoadwayNetwork] = None,
) -> TransitNetwork:
    """Add transit route to TransitNetwork.

    Args:
        net (TransitNetwork): Network to modify.
        transit_route_addition: route dictionary to add to the feed.
        reference_road_net: (RoadwayNetwork, optional): Reference roadway network to use for adding shapes and stops. Defaults to None.

    Returns:
        TransitNetwork: Modified network.
    """
    WranglerLogger.debug("Applying add transit route project.")

    add_routes = transit_route_addition["routes"]

    road_net = net.road_net if reference_road_net is None else reference_road_net
    if road_net is None:
        WranglerLogger.error(
            "! Must have a reference road network set in order to update transit \
                         routin.  Either provide as an input to this function or set it for the \
                         transit network: >> transit_net.road_net = ..."
        )
        raise ValueError(
            "Must have a reference road network set in order to update transit \
                         routing."
        )

    net.feed = _add_route_to_feed(net.feed, add_routes, road_net)

    return net


def _add_route_to_feed(
    feed: Feed,
    add_routes: list[dict],
    road_net: RoadwayNetwork,
) -> Feed:
    """Adds routes to a transit feed, updating routes, shapes, trips, stops, stop times, and freqs.

    Args:
        feed: Input transit feed.
        add_routes: List of route dictionaries to add to the feed.
        road_net: Reference roadway network to use for adding shapes and stops. Defaults to None.

    Returns:
        Feed: transit feed.
    """
    WranglerLogger.debug(f"Adding route {len(add_routes)} to feed.")
    add_routes_df = pd.DataFrame(
        [{k: v for k, v in r.items() if k != "trips"} for r in add_routes]
    )
    feed.routes = pd.concat([feed.routes, add_routes_df], ignore_index=True, sort=False)

    for route in add_routes:
        WranglerLogger.debug(
            f"Adding {len(route['trips'])} trips for route {route['route_id']} to feed."
        )

        for i, trip in enumerate(route["trips"]):
            shape_id = fill_str_ids(pd.Series([None]), feed.shapes["shape_id"]).iloc[0]
            add_shape_df = _create_new_shape(trip["routing"], shape_id, road_net, route)
            feed.shapes = pd.concat([feed.shapes, add_shape_df], ignore_index=True, sort=False)

            for j, headway in enumerate(trip["headway_secs"]):
                trip_id = f"trip{j}_shp{shape_id}"
                stop_dicts = _get_stops_from_routing(trip["routing"])
                add_trips_df = _create_new_trips(trip, route, trip_id, shape_id)
                add_freqs_df = _create_new_frequencies(headway, trip_id, route)
                add_stop_times_df = _create_new_stop_times(stop_dicts, trip_id, route)
                add_stops_df = _create_new_stops(
                    add_stop_times_df["stop_id"], feed.stops["stop_id"], road_net
                )

                # Add new data to existing dataframes
                feed.trips = pd.concat([feed.trips, add_trips_df], ignore_index=True, sort=False)
                feed.frequencies = pd.concat(
                    [feed.frequencies, add_freqs_df], ignore_index=True, sort=False
                )
                feed.stops = pd.concat([feed.stops, add_stops_df], ignore_index=True, sort=False)
                feed.stop_times = pd.concat(
                    [feed.stop_times, add_stop_times_df], ignore_index=True, sort=False
                )

    return feed


def _add_extra_route_attributes(
    route: dict, target_df: pd.DataFrame, attributes: list[str] = None
) -> pd.DataFrame:
    """Add attributes from route dictionary to target dataframe.
    
    Args:
        route: The route dictionary to copy attributes from.
        target_df: The DataFrame to copy attributes to.
        attributes: List of attributes to check and add.
    """
    if attributes is None:
        attributes = ["agency_raw_name"]

    for attr in attributes:
        if attr in route and route[attr]:
            target_df[attr] = route[attr]
    return target_df


def _create_new_trips(
    trip: dict, route: dict, trip_id: str, shape_id: str,
) -> paDataFrame[TripsTable]:
    """Create new trips for a route.

    Args:
        trip: Trip dictionaries with trip_id, shape_id, and other trip information.
        route: Route dictionaries.
        trip_id: Trip ID for the trips.
        shape_id: Shape ID for the trips.
    """
    add_trips_df = pd.DataFrame(
        [
            {
                "route_id": route["route_id"],
                "direction_id": trip["direction_id"],
                "trip_id": trip_id,
                "shape_id": shape_id,
            }
        ]
    )
    add_trips_df = _add_extra_route_attributes(route, add_trips_df)
    return add_trips_df


def _create_new_shape(
    routing: list[Union[dict, int]], shape_id: str, road_net: RoadwayNetwork, route: dict
) -> paDataFrame[WranglerShapesTable]:
    """Create new shape for a trip.

    Args:
        routing: Routing list with stop and board/alight information.
            e.g. [{1: {"stop": True}}, 2, 3, {"4": {"stop": True, "alight": False}}, 5, {"6": {"stop": True}}]
        shape_id: Shape ID for the shape.
        road_net: Roadway network to get node coordinates.
        route: Route dictionaries.
    """
    shape_model_node_id_list = [
        int(list(item.keys())[0]) if isinstance(item, dict) else int(item) for item in routing
    ]
    coords = [road_net.node_coords(n) for n in shape_model_node_id_list]
    lon, lat = zip(*coords)
    add_shapes_df = pd.DataFrame(
        {
            "shape_model_node_id": shape_model_node_id_list,
            "shape_pt_lat": lat,
            "shape_pt_lon": lon,
            "shape_pt_sequence": list(range(1, len(shape_model_node_id_list) + 1)),
        }
    )
    add_shapes_df["shape_id"] = shape_id
    add_shapes_df = _add_extra_route_attributes(route, add_shapes_df)
    return add_shapes_df


def _get_stops_from_routing(routing: list[Union[dict, int]]) -> list[dict]:
    """Converts a routing list to stop_id_list, drop_off_type, and pickup_type.

    Default for board and alight is True unless specified to be False.

    Args:
        routing: Routing list with stop and board/alight information.
            e.g. [{1: {"stop": True, "board": False}}, 2, 3]

    Returns:
        List of dictionaries for stops with stop_id and other values in stop dictionary such
            as board and alight. Example:

            ```python
            [
                {"stop_id": 1, pickup_type": 1, "drop_off_type": 0, "some_prop": "some_value"},
                {"stop_id": 4, "pickup_type": 1, "drop_off_type": 0},
                {"stop_id": 6, "pickup_type": 0, "drop_off_type": 0}}
            ]
            ```
    """
    FILTER_OUT = ["stop", "board", "alight"]
    stop_dicts = []
    for i in routing:
        if isinstance(i, dict):
            stop_d = {}
            stop_info = list(i.values())[0]  # dict with stop, board, alight
            if not stop_info.get("stop", False):
                continue
            stop_d["stop_id"] = int(list(i.keys())[0])
            # Default for board and alight is True unless specified to be False
            stop_d["pickup_type"] = 0 if stop_info.get("board", True) else 1
            stop_d["drop_off_type"] = 0 if stop_info.get("alight", True) else 1
            stop_d.update({k: v for k, v in stop_info.items() if k not in FILTER_OUT})
            stop_dicts.append(stop_d)
    return stop_dicts


def _create_new_stop_times(
    stop_dicts: list[dict], trip_id: str, route: dict
) -> paDataFrame[WranglerStopTimesTable]:
    """Create new stop times for a trip.

    Args:
        stop_dicts: List of dictionaries for stops with stop_id and other values in stop dictionary
            such as board and alight. Example:

            ```python
            [
                {"stop_id": 1, pickup_type": 1, "drop_off_type": 0, "some_prop": "some_value"},
                {"stop_id": 4, "pickup_type": 1, "drop_off_type": 0},
                {"stop_id": 6, "pickup_type": 0, "drop_off_type": 0}}
            ]
            ```
        trip_id: Trip ID for the stop times.
        route: Route dictionaries.

    Returns:
        Dataframe with new stop times.
    """
    add_stop_times_df = pd.DataFrame(stop_dicts)
    add_stop_times_df["trip_id"] = trip_id
    add_stop_times_df["stop_sequence"] = list(range(1, len(add_stop_times_df) + 1))
    add_stop_times_df = _add_extra_route_attributes(route, add_stop_times_df)
    return add_stop_times_df


def _create_new_stops(
    routing_node_ids: pd.Series, existing_stop_ids: pd.Series, road_net: RoadwayNetwork
) -> paDataFrame[WranglerStopsTable]:
    """Create new stops entries for a trip if they don't already exist in the feed.

    Args:
        routing_node_ids: Series of node IDs from routing.
        existing_stop_ids: Series of existing stop IDs.
        road_net: Roadway network to get node coordinates.
    """
    add_stop_ids = routing_node_ids[~routing_node_ids.isin(existing_stop_ids)].unique()
    add_stops_df = pd.DataFrame(columns=["stop_id", "stop_lat", "stop_lon"])
    if add_stop_ids.size:
        coords = [road_net.node_coords(n) for n in add_stop_ids]
        lon, lat = zip(*coords)
        add_stops_df = pd.DataFrame({"stop_id": add_stop_ids, "stop_lat": lat, "stop_lon": lon})
    return add_stops_df


def _create_new_frequencies(
    headway: dict[tuple[TimeString], int], trip_id: str, route: dict
) -> paDataFrame[FrequenciesTable]:
    """Create new frequencies entries for a trip.

    Args:
        headway: Headway dictionaries with time range as key and headway in seconds as
            value. e.g.:
            ```python
            {('6:00', '12:00'): 600}
            ```
        trip_id: Trip ID for the frequencies.
        route: Route dictionaries.
    """
    add_freqs_df = pd.DataFrame(
        [_parse_headway_record(headway)],
        columns=["start_time", "end_time", "headway_secs"],
    )
    add_freqs_df["trip_id"] = trip_id
    add_freqs_df = _add_extra_route_attributes(route, add_freqs_df)
    return add_freqs_df


def _parse_headway_record(headway: dict[tuple[TimeString], int]) -> tuple[datetime, datetime, int]:
    """Converts a headway dictionary to start_time, end_time, headway_secs.

    Args:
        headway: Headway dictionary with time range as key and headway in seconds as value.
            e.g. {('6:00', '12:00'): 600}
    """
    ((timespan, headway_secs),) = headway.items()
    timespan_clean = [time.strip().strip("'") for time in timespan.strip("()").split(",")]
    start_time, end_time = str_to_time_list(timespan_clean)
    return start_time, end_time, headway_secs

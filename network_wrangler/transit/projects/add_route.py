"""Functions for adding a transit route to a TransitNetwork."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from ...logger import WranglerLogger

import pandas as pd

from ...params import TRANSIT_SHAPE_ID_SCALAR, TRANSIT_STOP_ID_SCALAR
from ...utils.time import str_to_time
from ...utils.utils import generate_new_id

if TYPE_CHECKING:
    from ...roadway.network import RoadwayNetwork
    from ..network import TransitNetwork
    from ..feed.feed import Feed

class TransitRouteAddError(Exception):
    """Error raised when applying add transit route."""

    pass


def apply_transit_route_addtion(
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
    
    add_routes = transit_route_addition.get("routes", [])

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
    
    net.feed = _add_route_to_feed(
        net.feed, 
        add_routes, 
        road_net
    )

    WranglerLogger.debug("Validating to network.")
    # TODO: add validation
    return net

def _add_route_to_feed(
    feed: Feed, 
    add_routes: list,
    road_net: RoadwayNetwork,
    shape_id_scalar: int = TRANSIT_SHAPE_ID_SCALAR,
    stop_id_scalar: int = TRANSIT_STOP_ID_SCALAR,
) -> Feed:
    """Adds routes to a transit feed, updating routes, shapes, trips, stops, stop times, and frequencies.

    Args:
        feed: Input transit feed.
        add_routes: List of route dictionaries to add to the feed.
        road_net: Reference roadway network to use for adding shapes and stops. Defaults to None.
        shape_id_scalar: Scalar used to generate unique shape IDs.
        stop_id_scalar: Scalar used to generate unique stop IDs.

    Returns:
        Feed: transit feed.
    """
    WranglerLogger.debug("Adding route to feed.")

    routes_df = feed.routes.copy()
    shapes_df = feed.shapes.copy()
    trips_df = feed.trips.copy()
    stop_times_df = feed.stop_times.copy()
    stops_df = feed.stops.copy()
    frequencies_df = feed.frequencies.copy()

    nodes = road_net.nodes_df.copy()

    stop_id_xref_dict = (
        stops_df
        .set_index("model_node_id")["stop_id"]
        .to_dict()
    )
    stop_id_xref_dict = {int(float(key)): int(float(value)) for key, value in stop_id_xref_dict.items()}
    model_node_coord_dict = (
        nodes
        .set_index("model_node_id")[['X', 'Y']]
        .apply(tuple, axis=1)
        .to_dict()
    )
    model_node_coord_dict = {int(float(key)): value for key, value in model_node_coord_dict.items()}
    
    stop_id_max = max(stop_id_xref_dict.values())
    shape_id_max = pd.to_numeric(shapes_df['shape_id'].str.extract(r'(\d+)')[0], errors='coerce').max()

    for route in add_routes:
        # add route
        add_routes_df = pd.DataFrame([{
            "route_id": route["route_id"],
            "route_short_name": route["route_short_name"],
            "route_long_name": route["route_long_name"],
            "route_type": route["route_type"],
            "agency_raw_name": route["agency_raw_name"],
            "agency_id": route["agency_id"]
        }])
        routes_df = pd.concat([routes_df, add_routes_df], ignore_index=True, sort=False)

        trip_index = 1
        for trip in route["trips"]:
            # add shape
            shape_id = generate_new_id(shape_id_max, shapes_df["shape_id"], shape_id_scalar)
            shape_model_node_id_list = [int(list(item.keys())[0]) if isinstance(item, dict) else int(item) for item in trip["routing"]]
            add_shapes_df = pd.DataFrame({
                "shape_id": shape_id,
                "shape_model_node_id": shape_model_node_id_list,
                "shape_pt_lat": [model_node_coord_dict[node][1] for node in shape_model_node_id_list],
                "shape_pt_lon": [model_node_coord_dict[node][0] for node in shape_model_node_id_list],
                "shape_pt_sequence": list(range(1,len(shape_model_node_id_list)+1)),
                "agency_raw_name": route["agency_raw_name"]
            }) 
            shapes_df = pd.concat([shapes_df, add_shapes_df], ignore_index=True, sort=False)

            for headway in trip["headway_sec"]:
                # add trip
                trip_id = f"trip{trip_index}_shp{shape_id}"
                add_trips_df = pd.DataFrame([{
                    "route_id": route["route_id"],
                    "direction_id": trip["direction_id"],
                    "trip_id": trip_id,
                    "shape_id": shape_id,
                    "agency_raw_name": route["agency_raw_name"],
                    'service_id': trip_id
                }])
                trips_df = pd.concat([trips_df, add_trips_df], ignore_index=True, sort=False)

                # add frequency
                headway_secs = list(headway.values())[0]
                time_range = list(headway.keys())[0]
                time_range = [time.strip().strip("'") for time in time_range.strip("()").split(',')]
                start_time = str_to_time(time_range[0])
                end_time = str_to_time(time_range[1])
                add_freqs_df =  pd.DataFrame([{
                    "trip_id": trip_id,
                    "headway_secs": headway_secs,
                    "start_time": start_time,
                    "end_time": end_time,
                    "agency_raw_name": route["agency_raw_name"]
                }])
                frequencies_df = pd.concat([frequencies_df, add_freqs_df], ignore_index=True, sort=False)

                # add stop and stop_times
                stop_model_node_id_list = []
                pickup_type = []
                drop_off_type = []

                for i in trip['routing']:
                    if (isinstance(i, dict) and 
                        list(i.values())[0] is not None and 
                        list(i.values())[0].get('stop')
                    ):
                        stop_model_node_id_list.append(int(list(i.keys())[0]))
                        drop_off_type.append(0 if list(i.values())[0].get('alight', True) else 1)
                        pickup_type.append(0 if list(i.values())[0].get('board', True) else 1) 

                # used to build stop_time
                stop_id_list = [] 

                for s in stop_model_node_id_list:
                    s = int(float(s))
                    if s in stop_id_xref_dict.keys():
                        existing_agency_raw_name = (
                            stops_df[
                                stops_df["model_node_id"]
                                .astype(float)
                                .astype(int) == s
                            ]['agency_raw_name'].to_list()
                        )
                        existing_trip_ids = (
                            stops_df[
                                stops_df["model_node_id"]
                                .astype(float)
                                .astype(int) == s
                            ]['trip_id'].to_list()
                        )
                        existing_stop_id = (
                            stops_df[
                                stops_df["model_node_id"]
                                .astype(float)
                                .astype(int) == s
                            ]['stop_id'].iloc[0]
                        )
                        if ((route["agency_raw_name"] not in existing_agency_raw_name)
                            | (trip_id not in existing_trip_ids)
                        ):
                            new_stop_id = existing_stop_id
                            stop_id_list.append(new_stop_id)
                            stop_id_xref_dict.update({s: new_stop_id})
                            # add new stop to stops_df
                            add_stops_df = pd.DataFrame([{
                                "stop_id" : new_stop_id,
                                "stop_lat" : model_node_coord_dict[s][1],
                                "stop_lon" : model_node_coord_dict[s][0],
                                "model_node_id" : s,
                                'trip_id': trip_id,
                                "agency_raw_name": route["agency_raw_name"]
                            }])
                            stops_df = pd.concat([stops_df, add_stops_df], ignore_index=True, sort=False)
                        else:
                            stop_id_list.append(stop_id_xref_dict[s])
                    else:
                        new_stop_id = generate_new_id(stop_id_max, stops_df["stop_id"], stop_id_scalar)
                        stop_id_list.append(new_stop_id)
                        stop_id_xref_dict.update({s: new_stop_id})
                        # add new stop to stops_df
                        add_stops_df = pd.DataFrame([{
                            "stop_id" : new_stop_id,
                            "stop_lat" : model_node_coord_dict[s][1],
                            "stop_lon" : model_node_coord_dict[s][0],
                            "model_node_id" : s,
                            'trip_id': trip_id,
                            "agency_raw_name": route["agency_raw_name"]
                        }])
                        stops_df = pd.concat([stops_df, add_stops_df], ignore_index=True, sort=False)

                # add stop_times
                # TODO: time_to_next_node_sec
                stop_sequence = list(range(1, len(stop_id_list) + 1))
                add_stop_times_df = pd.DataFrame({
                    "trip_id": trip_id,
                    "stop_sequence": stop_sequence,
                    "arrival_time": 0,
                    "departure_time": 0,
                    "pickup_type": pickup_type,
                    "drop_off_type": drop_off_type,
                    "stop_id": stop_id_list,
                    "model_node_id": stop_model_node_id_list,
                    "agency_raw_name": route["agency_raw_name"]
                })
                stop_times_df = pd.concat([stop_times_df, add_stop_times_df], ignore_index=True, sort=False)

                trip_index += 1

    feed.routes = routes_df
    feed.shapes = shapes_df
    feed.trips = trips_df
    feed.stop_times = stop_times_df
    feed.stops = stops_df
    feed.frequencies = frequencies_df

    return feed

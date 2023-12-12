from typing import Union, Literal

import numpy as np
import pandas as pd

from pandera.decorators import check_output

from ..transit.schemas import ShapesSchema, StopsSchema, StopTimesSchema
from ..logger import WranglerLogger
from ..utils import generate_new_id

# Default initial scalar value to add to duplicated shape_ids to create a new shape_id
SHAPE_ID_SCALAR = 1000000

# Default initial scalar value to add to node id to create a new stop_id
STOP_ID_SCALAR = 1000000


class TransitRoutingChangeError(Exception):
    pass


@check_output(StopTimesSchema, inplace=True)
def _create_stop_times(
    set_stops_node_ids: list[int], trip_id: str, net: "TransitNetwork"
) -> pd.DataFrame:
    """Modifies a list of nodes from project card routing key to a shape dataframe.

    Args:
        set_stops_node_ids: List of roadway node ids from project card to stop at.
        trip_id: trip_id to associate with the new shape
        net: transit ntwork

    """

    node_to_stop = net.feed.stops.set_index(net.feed.stops_node_id)["stop_id"].to_dict()

    new_stoptime_rows = pd.DataFrame(
        {
            "trip_id": trip_id,
            "stop_id": [node_to_stop[n] for n in set_stops_node_ids],
            net.feed.stops_node_id: set_stops_node_ids,
            "stop_sequence": np.arange(len(set_stops_node_ids)),
            "arrival_time": None,
            "departure_time": None,
            "pickup_type": 0,
            "drop_off_type": 0,
        }
    )

    return new_stoptime_rows


@check_output(ShapesSchema, inplace=True)
def _create_shapes(
    nodes_list: list[Union[str, int]], shape_id: str, net: "TransitNetwork"
) -> pd.DataFrame:
    """Modifies a list of nodes from project card routing key to rows in a shapes.txt dataframe.

    Args:
        nodes_list (list[Union[str,int]]): List of nodes where stops are either integers with
            positive integers representing stops and negative integers representing waypoints
            or strings. e.g. ["-123","321",...] or [-123,321]
        shape_id: shape_id to associate with the new shape
        net: transit network

    Returns:
        pd.DataFrame: shapes.txt rows
    """

    shapes_node_id, hwy_node_id = net.TRANSIT_FOREIGN_KEYS_TO_ROADWAY["shapes"]["links"]

    # Build a pd.DataFrame of new shape records
    new_shape_rows_df = pd.DataFrame(
        {
            "shape_id": shape_id,
            shapes_node_id: nodes_list,
            "shape_pt_sequence": np.arange(len(nodes_list)),
        }
    )

    if net.road_net:
        if not net._shape_links_in_road_net(shapes_df=new_shape_rows_df):
            # ADD FIX SHAPES WITH MISSING NODES
            pass

        WranglerLogger.debug("Getting attributes from highway nodes")
        nodes_df = net.road_net.nodes_df[[hwy_node_id, "X", "Y", "osm_node_id"]]
        new_shape_rows_df = new_shape_rows_df.merge(
            nodes_df, how="left", left_on=shapes_node_id, right_on=hwy_node_id
        )
        new_shape_rows_df = new_shape_rows_df.rename(
            columns={
                "Y": "shape_pt_lat",
                "X": "shape_pt_lon",
                "osm_node_id": "shape_osm_node_id",
            }
        )
        new_shape_rows_df = new_shape_rows_df.drop(columns=[hwy_node_id])
    else:
        new_shape_rows_df["shape_pt_lat"] = 0.0
        new_shape_rows_df["shape_pt_lon"] = 0.0
        WranglerLogger.warning(
            "Roadway network not set: unable to confirm valid routing,  \
                access roadway attributes, or add lat/long."
        )

    # WranglerLogger.debug(f"New Shape Rows:\n{new_shape_rows_df}")
    return new_shape_rows_df


def _add_new_shape_copy(
    old_shape_id: str,
    trip_ids: list[str],
    net: "TransitNetwork",
    id_scalar=SHAPE_ID_SCALAR,
) -> tuple[Union[pd.DataFrame, str]]:
    """Create an identical new shape_id from shape matching old_shape_id for the trip_ids.

    Args:
        old_shape_id (str): ID of shape to copy
        trip_ids (list[str]): list of trip IDS to associate with new shape
        net: Input TransitNetwork.
        id_scalar (int): scalar value to add to old_shape_id to create new_shape_id. Defaults to
            SHAPE_ID_SCALAR.

    Returns:
        tuple[pd.DataFrame]: updated (shapes_df, trips_df, new_shape_id)
    """
    shapes = net.feed.shapes.copy()
    trips = net.feed.trips.copy()
    new_shape = shapes[shapes.shape_id == old_shape_id].copy()
    new_shape_id = generate_new_id(old_shape_id, shapes["shape_id"], id_scalar)
    new_shape["shape_id"] = new_shape_id
    shapes = pd.concat([shapes, new_shape], ignore_index=True)

    trips.loc[trips.trip_id.isin(trip_ids), "shape_id"] = new_shape_id
    return shapes, trips, new_shape_id


@check_output(StopsSchema, inplace=True)
def _create_stop(
    new_stop_node_id: str,
    stops: pd.DataFrame,
    net: "TransitNetwork",
    id_scalar: int = STOP_ID_SCALAR,
) -> pd.DataFrame:
    """Adds a stop for the given node ID to the stops table and checks that it exists in hwy.

    Args:
        new_stop_node_id (_type_): Node ID used in highway network
        stops (pd.DataFrame): Current stops table
        net (TransitNetwork): Transit network
        id_scalar: scalar to add to the node ID to get the stop_id

    Returns:
        pd.DataFrame: Updated stops Table
    """
    WranglerLogger.debug(
        f"Adding a new stop in stops.txt for node ID: {new_stop_node_id} \
        using scalar: {id_scalar}"
    )

    new_stop = pd.DataFrame(
        {
            "stop_id": generate_new_id(new_stop_node_id, stops["stop_id"], id_scalar),
            net.feed.stops_node_id: new_stop_node_id,
            "stop_lat": 0.0,
            "stop_lon": 0.0,
            "wheelchair_boarding": None,
        },
        index=[0],
    )
    if net.road_net:
        if not net.hwy_net.has_node(int(new_stop_node_id)):
            raise TransitRoutingChangeError(
                f"Node specified not found in highway net: {new_stop_node_id}"
            )
        WranglerLogger.debug("Getting stop attributes from highway nodes")
        hwy_node = net.hwy_net.nodes_df.loc[int(new_stop_node_id)]
        new_stop[["stop_lat", "stop_lon"]] = hwy_node[["Y", "X"]]
    else:
        WranglerLogger.warning(
            "Roadway network not set: unable to confirm valid routing or \
                               access roadway attributes."
        )

    # WranglerLogger.debug(f"NewStop:\n{new_stop}")

    return new_stop


@check_output(ShapesSchema, inplace=True)
def _replace_shapes_segment(
    existing_routing: list[int],
    shape_id: str,
    set_routing: list[int],
    net: "TransitNetwork",
) -> pd.DataFrame:
    """Returns shapes with a replaced segment for a given shape_id.

    Segment to replace is defined by existing_routing but will be updated based on:
    1. Expanding to shape start or shape end if it is the first or last stop respectfully
    2. Shrinking if replacement segment has overlap with existing_routing so that existing
        data can be preserved.

    Args:
        existing_routing: list of depicting start and end node ids for segment to replace
        shape_id: shape_id to be modified.
        set_routing (list): list of node ids to replace existing routing with
        net: TransitNetwork object

    Returns:
        pd.DataFrame: Updated shape records
    """
    existing_shape_df = net.feed.shapes.loc[net.feed.shapes.shape_id == shape_id]
    existing_shape_df = existing_shape_df.sort_values(by=["shape_pt_sequence"])
    existing_nodes = existing_shape_df[net.feed.shapes_node_id]

    _disp_col = ["shape_id", "shape_pt_sequence", "shape_model_node_id"]
    WranglerLogger.debug(f"\nExisting Shape\n{existing_shape_df[_disp_col]}")

    # Define start and end of segment to replace and make sure they are valid
    start_node, end_node = existing_routing[0], existing_routing[-1]
    if len(existing_routing) > 2:
        WranglerLogger.warning("3+ nodes provided. Only using start and end nodes.")
    WranglerLogger.debug(f"Replacing shapes segment: {start_node}-{end_node}")

    _missing_nodes = set([start_node, end_node]) - set(existing_nodes.to_list())
    if _missing_nodes:
        raise TransitRoutingChangeError(
            f"Missing segment end in shape:{_missing_nodes}"
        )

    trip_ids = net.feed.trips_with_shape_id(shape_id).trip_id.unique()

    # Get first and last stop node for all trips that use this shape
    all_shape_stop_times = pd.concat(
        [net.feed.shape_with_trip_stops(t) for t in trip_ids]
    )
    shapes_with_stops = all_shape_stop_times[all_shape_stop_times['stop_id'].notna()]
    shapes_with_stops = shapes_with_stops.sort_values(by=["shape_pt_sequence"])
    
    # if start_node == first stop, change start_node to first shape node
    first_stop_node_id = shapes_with_stops[net.feed.shapes_node_id].iloc[0]
    first_shape_node_id = existing_nodes.iat[0]

    if first_stop_node_id == start_node & start_node != first_shape_node_id:
        WranglerLogger.debug(
            f"Defined segment start node ({start_node}) is first stop ({first_stop_node_id}). \
            Updating segment first-node to start of shape node ({first_shape_node_id})."
        )
        start_node = first_shape_node_id
    # if end_node == last stop, change end_node to last shape node
    last_stop_node_id = shapes_with_stops[net.feed.shapes_node_id].iloc[-1]
    last_shape_node_id = existing_nodes.iat[-1]
    
    if last_stop_node_id == end_node & end_node != last_shape_node_id:
        WranglerLogger.debug(
            f"Defined segment end node ({end_node}) is last stop ({last_stop_node_id}). \
            Updating segment end-node to end of shape node ({last_shape_node_id})."
        )
        end_node = last_shape_node_id

    # Find indices for replacement, using first occurance of start node and last occurance of end
    replacement_start_idx = existing_nodes[existing_nodes == start_node].index[0]
    replacement_end_idx = existing_nodes[existing_nodes == end_node].index[0]
    WranglerLogger.debug(
        f"Replacement segment now node(idx): \
            from {start_node}({replacement_start_idx}) to {end_node}({replacement_end_idx})."
    )
    # If there is overlap between existing and replacement, update idx to use existing segment.
    if len(set_routing) > 0 and set_routing[0] == start_node:
        replacement_start_idx += 1
        set_routing = set_routing[1:]
        WranglerLogger.debug(f"Shape start overlaps with replacement. Set routing: {set_routing}")

    if len(set_routing) > 0 and set_routing[-1] == end_node:
        replacement_end_idx -= 1
        set_routing = set_routing[:-1]
        WranglerLogger.debug(f"Shape end overlaps with replacement. Set routing: {set_routing}")

    WranglerLogger.debug(
        f"Replacement segment now node(idx): \
            from {start_node}({replacement_start_idx}) to {end_node}({replacement_end_idx})."
    )

    # Slice existing_shape_df for replacement
    before_segment = existing_shape_df.loc[: replacement_start_idx - 1]
    after_segment = existing_shape_df.loc[replacement_end_idx + 1 :]

    # Create new segment
    segment_shapes_df = _create_shapes(set_routing, shape_id, net)

    #WranglerLogger.debug(f"\nBefore Shapes Segment:\n{before_segment[_disp_col]}")
    #WranglerLogger.debug(f"\nReplm't Shapes Segment:\n{segment_shapes_df[_disp_col]}")
    #WranglerLogger.debug(f"\nAfter Shapes Segment:\n{after_segment[_disp_col]}")

    # Concatenate the shape dataframes

    # Only concatenate those that aren't empty bc NaN values will transfer integers to floats.
    dfs = [before_segment, segment_shapes_df, after_segment]
    concat_dfs = [df for df in dfs if not df.empty]

    updated_shape = pd.concat(
        concat_dfs,
        ignore_index=True,
        sort=False,
    )
    updated_shape["shape_pt_sequence"] = np.arange(len(updated_shape))

    WranglerLogger.debug(f"\nShape w/Segment Replaced:\n {updated_shape[_disp_col]}")
    return updated_shape


@check_output(StopTimesSchema, inplace=True)
def _replace_stop_times_segment_for_trip(
    existing_stops_node_ids: list[str],
    trip_id: str,
    set_stops_node_ids: list[str],
    net: "TransitNetwork",
):
    """Replaces a segment of a specific set of records with the same shape_id.

    Args:
        existing_stops_node_ids (list): list of roadway node ids from routing project card
        trip_id (pd.DataFrame): trip_id
        set_stops_node_ids (list): list of roadway node ids in order to replace existing stop ids with
        net (_type_): transit network

    Returns:
        _type_: Updated shape records
    """
    WranglerLogger.debug(f"Replacing existing nodes pattern: {existing_stops_node_ids}")

    feed = net.feed
    existing_trip_stoptime_df = feed.trip_stop_times(trip_id)
    existing_trip_stoptime_df = existing_trip_stoptime_df.sort_values(
        by=["stop_sequence"]
    )
    existing_nodes = existing_trip_stoptime_df[feed.stops_node_id]

    _disp_col = ["stop_id", "model_node_id", "stop_sequence"]
    WranglerLogger.debug(
        f"existing_trip_stoptime_df:\n{existing_trip_stoptime_df[_disp_col]}"
    )

    # Define start and end of segment and make sure those nodes exist in stoptimes
    start_node, end_node = existing_stops_node_ids[0], existing_stops_node_ids[-1]
    WranglerLogger.debug(f"\nSegment start/end: {start_node} - {end_node}")
    if len(existing_stops_node_ids) > 2:
        WranglerLogger.warning(
            "Existing r> than 2 nodes - only using start and end node."
        )
    _missing = set([start_node, end_node]) - set(existing_nodes.to_list())
    if _missing:
        raise TransitRoutingChangeError(
            f"Missing existing segment end in stoptime:{_missing}"
        )

    # Find indices for replacement - updating to use existing if there is overlap
    replacement_start_idx = existing_nodes[existing_nodes == start_node].index[0]
    if len(set_stops_node_ids) > 0 and set_stops_node_ids[0] == start_node:
        WranglerLogger.debug("Start overlaps with replacement")
        replacement_start_idx += 1
        set_stops_node_ids = set_stops_node_ids[1:]
    replacement_end_idx = existing_nodes[existing_nodes == end_node].index[-1]
    if len(set_stops_node_ids) > 0 and set_stops_node_ids[-1] == end_node:
        WranglerLogger.debug("End overlaps with replacement")
        replacement_end_idx -= 1
        set_stops_node_ids = set_stops_node_ids[:-1]

    WranglerLogger.debug(
        f"\nSegment idx: {replacement_start_idx} to {replacement_end_idx}"
    )

    # Slice existing df for replacement
    before_segment = existing_trip_stoptime_df.loc[: replacement_start_idx - 1]
    after_segment = existing_trip_stoptime_df.loc[replacement_end_idx + 1 :]

    # Create new segment
    segment_stoptime_rows = _create_stop_times(set_stops_node_ids, trip_id, net)

    #WranglerLogger.debug(f"Before Segment:\n{before_segment[_disp_col]}")
    #WranglerLogger.debug(f"Segment:\n{segment_stoptime_rows[_disp_col]}")
    #WranglerLogger.debug(f"After Segment:\n{after_segment[_disp_col]}")

    # Concatenate the dataframes

    # ...Only concatenate those that aren't empty bc NaN values will transfer integers to floats.
    dfs = [before_segment, segment_stoptime_rows, after_segment]
    concat_dfs = [df for df in dfs if not df.empty]

    updated_this_trip_stop_times = pd.concat(
        concat_dfs,
        ignore_index=True,
        sort=False,
    )

    updated_this_trip_stop_times["stop_sequence"] = np.arange(
        len(updated_this_trip_stop_times)
    )
    WranglerLogger.debug(
        f"Updated Stoptimees:\n{updated_this_trip_stop_times[_disp_col]}"
    )
    return updated_this_trip_stop_times


def _update_shapes_and_trips(
    net: "TransitNetwork",
    shape_id: str,
    trip_ids: list[str],
    set_routing: list[int],
    existing_routing: list[int],
    shape_id_scalar: int,
) -> tuple[pd.DataFrame]:
    """_summary_

    Args:
        shapes (pd.DataFrame): _description_
        trips (pd.DataFrame): _description_
        shape_id (str): _description_
        trip_ids (list[str]):
        set_routing (list[int]): _description_
        existing_routing (list[int]): _description_
        shape_id_scalar (int):
        net (TransitNetwork): _description_

    Returns:
        tuple[pd.DataFrame]: updated shapes and trips dataframes
    """
    feed = net.feed
    shapes = feed.shapes
    trips = feed.trips

    # Don't need a new shape if its only the stops that change
    existing_pattern = feed.shape_node_pattern(shape_id)
    no_routing_change = '|'.join(map(str, set_routing)) in '|'.join(map(str, existing_pattern))
    if no_routing_change:
        WranglerLogger.debug("No routing change, returning shapes and trips as-is.")
        return feed.shapes, feed.trips

    # Create new shape if `shape_id` is used by trips that are not in selected trip_ids
    all_trips_using_shape_id = set(
        feed.trips_with_shape_id(shape_id)["trip_id"].to_list()
    )
    selected_trips_using_shape_id = set(trip_ids) & set(all_trips_using_shape_id)
 
    if selected_trips_using_shape_id != all_trips_using_shape_id:
        feed.shapes, feed.trips, shape_id = _add_new_shape_copy(
            shape_id,
            selected_trips_using_shape_id,
            feed,
            id_scalar=shape_id_scalar,
        )

    # If "existing" is specified, replace only that segment Else, replace the whole thing
    if existing_routing:
        this_shape = _replace_shapes_segment(
            existing_routing,
            shape_id,
            set_routing,
            net,
        )
    else:
        this_shape = _create_shapes(set_routing, shape_id, net)

    # Add rows back into shapes
    feed.shapes = pd.concat(
        [feed.shapes[feed.shapes.shape_id != shape_id], this_shape],
        ignore_index=True,
        sort=False,
    )

    return feed.shapes, feed.trips


@check_output(StopsSchema, inplace=True)
def _update_stops(
    net: "TransitNetwork", set_stops_node_ids: list[int], stop_id_scalar: int
) -> pd.DataFrame:
    feed = net.feed

    trn_net_stop_nodes = feed.stops[feed.stops_node_id].tolist()

    # add new stops to stops.txt
    missing_stops_node_ids = set(set_stops_node_ids) - set(trn_net_stop_nodes)
    if not missing_stops_node_ids:
        return feed.stops
    for new_stop_node_id in missing_stops_node_ids:
        new_stop = _create_stop(
            new_stop_node_id, feed.stops, net, id_scalar=stop_id_scalar
        )
        stops = pd.concat([feed.stops, new_stop], ignore_index=True)
    return stops


@check_output(StopTimesSchema, inplace=True)
def _update_stop_times_for_trip(
    net: "TransitNetwork",
    trip_id: str,
    set_stops_node_ids: list[int],
    existing_stops_node_ids: list[int],
) -> pd.DataFrame:
    if existing_stops_node_ids:
        this_trip_stop_times = _replace_stop_times_segment_for_trip(
            existing_stops_node_ids,
            trip_id,
            set_stops_node_ids,
            net,
        )
    else:
        this_trip_stop_times = _create_stop_times(set_stops_node_ids, trip_id, net)

    stop_times = net.feed.get("stop_times")
    stop_times = pd.concat(
        [stop_times[stop_times.trip_id != trip_id], this_trip_stop_times],
        ignore_index=True,
        sort=False,
    )
    _show_col = [
        "trip_id",
        "stop_id",
        net.feed.stops_node_id,
        "stop_sequence",
        "departure_time",
        "arrival_time",
    ]
    WranglerLogger.debug(
        f"ST for trip: {stop_times.loc[stop_times.trip_id == trip_id,_show_col]}"
    )

    return stop_times


def apply_transit_routing_change(
    net: "TransitNetwork",
    selection: "Selection",
    routing_change: dict,
    shape_id_scalar: int = SHAPE_ID_SCALAR,
    stop_id_scalar: int = STOP_ID_SCALAR,
) -> "TransitNetwork":
    """Apply a routing change to the transit network, including stop updates.

    TODO: find "stop segment" if existing stops_node_ids not actual stops, but are shapes.

    Args:
        net (TransitNetwork): TransitNetwork object to apply routing change to.
        selection (Selection): TransitSelection object, created from a selection dictionary.
        routing_change (dict): Routing Change dictionary, e.g.
            ```python
            {
                "existing":[46665,150855],
                "set":[-46665,150855,46665,150855],
            }
            ```
            NOTE: right now, existing span must be between two
        shape_id_scalar (int, optional): Initial scalar value to add to duplicated shape_ids to
            create a new shape_id. Defaults to SHAPE_ID_SCALAR.
        stop_id_scalar (int, optional): Initial scalar value to add to duplicated stop_ids to
            create a new stop_id. Defaults to STOP_ID_SCALAR.
    """
    WranglerLogger.debug("Applying transit routing change project.")
    WranglerLogger.debug(f"...selection: {selection.selection_dict}")
    WranglerLogger.debug(f"...routing: {routing_change}")

    trip_ids = selection.selected_trips
    shape_ids = net.feed.trips[net.feed.trips["trip_id"].isin(trip_ids)].shape_id

    # update each shape that is used by selected trips to use new routing
    for shape_id in shape_ids:

        net.feed.shapes, net.feed.trips = _update_shapes_and_trips(
            net,
            shape_id,
            trip_ids,
            [int(abs(int(i))) for i in routing_change["set"]],
            [int(abs(int(i))) for i in routing_change.get("existing", [])],
            shape_id_scalar,
        )

    # Check if any stops need adding to stops.txt and add if they do
    set_stops_node_ids = [int(i) for i in routing_change["set"] if int(i) > 0]
    net.feed.stops = _update_stops(net, set_stops_node_ids, stop_id_scalar)

    existing_node_ids = [abs(int(i)) for i in routing_change.get("existing", [])]

    if set(set_stops_node_ids) != set(existing_node_ids):
        for trip_id in trip_ids:
            net.feed.stop_times = _update_stop_times_for_trip(
                net, trip_id, set_stops_node_ids, existing_node_ids
            )

    _show_col = [
        "trip_id",
        "stop_id",
        net.feed.stops_node_id,
        "stop_sequence",
        "departure_time",
        "arrival_time",
    ]
    _ex_stoptimes = net.feed.stop_times.loc[net.feed.stop_times.trip_id == trip_ids[0],_show_col]
    #WranglerLogger.debug(f"ST for trip: {_ex_stoptimes}")

    return net

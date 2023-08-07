from ..logger import WranglerLogger

import numpy as np
import pandas as pd

def apply_transit_routing_change(   
    net: 'TransitNetwork', selection: 'Selection', routing_change: dict
) -> 'TransitNetwork':
    WranglerLogger.debug("Applying transit routing change project.")
 
    trip_ids = selection.selected_trips
    routing = pd.Series(routing_change["set"])

    # Copy the tables that need to be edited since they are immutable within partridge
    shapes = net.feed.shapes.copy()
    stop_times = net.feed.stop_times.copy()
    stops = net.feed.stops.copy()

    # A negative sign in "set" indicates a traversed node without a stop
    # If any positive numbers, stops have changed
    stops_change = False
    if any(x > 0 for x in routing_change["set"]):
        # Simplify "set" and "existing" to only stops
        routing_change["set_stops"] = [
            str(i) for i in routing_change["set"] if i > 0
        ]
        if routing_change.get("existing") is not None:
            routing_change["existing_stops"] = [
                str(i) for i in routing_change["existing"] if i > 0
            ]
        stops_change = True

    # Convert ints to objects
    routing_change["set_shapes"] = [str(abs(i)) for i in routing_change["set"]]
    if routing_change.get("existing") is not None:
        routing_change["existing_shapes"] = [
            str(abs(i)) for i in routing_change["existing"]
        ]

    # Replace shapes records
    trips = net.feed.trips  # create pointer rather than a copy
    shape_ids = trips[trips["trip_id"].isin(trip_ids)].shape_id
    for shape_id in shape_ids:
        # Check if `shape_id` is used by trips that are not in
        # parameter `trip_ids`
        trips_using_shape_id = trips.loc[trips["shape_id"] == shape_id, ["trip_id"]]
        if not all(trips_using_shape_id.isin(trip_ids)["trip_id"]):
            # In this case, we need to create a new shape_id so as to leave
            # the trips not part of the query alone
            WranglerLogger.warning(
                "Trips that were not in your query selection use the "
                "same `shape_id` as trips that are in your query. Only "
                "the trips' shape in your query will be changed."
            )
            old_shape_id = shape_id
            shape_id = str(int(shape_id) + net.ID_SCALAR)
            if shape_id in shapes["shape_id"].tolist():
                WranglerLogger.error("Cannot create a unique new shape_id.")
            dup_shape = shapes[shapes.shape_id == old_shape_id].copy()
            dup_shape["shape_id"] = shape_id
            shapes = pd.concat([shapes, dup_shape], ignore_index=True)

        # Pop the rows that match shape_id
        this_shape = shapes[shapes.shape_id == shape_id]

        # Make sure they are ordered by shape_pt_sequence
        this_shape = this_shape.sort_values(by=["shape_pt_sequence"])

        shape_node_fk, rd_field = net.TRANSIT_FOREIGN_KEYS_TO_ROADWAY[
            "shapes"
        ]["links"]
        # Build a pd.DataFrame of new shape records
        new_shape_rows = pd.DataFrame(
            {
                "shape_id": shape_id,
                "shape_pt_lat": None,  # FIXME Populate from self.road_net?
                "shape_pt_lon": None,  # FIXME
                "shape_osm_node_id": None,  # FIXME
                "shape_pt_sequence": None,
                shape_node_fk: routing_change["set_shapes"],
            }
        )

        # If "existing" is specified, replace only that segment
        # Else, replace the whole thing
        if routing_change.get("existing") is not None:
            # Match list
            nodes = this_shape[shape_node_fk].tolist()
            index_replacement_starts = [
                i
                for i, d in enumerate(nodes)
                if d == routing_change["existing_shapes"][0]
            ][0]
            index_replacement_ends = [
                i
                for i, d in enumerate(nodes)
                if d == routing_change["existing_shapes"][-1]
            ][-1]
            this_shape = pd.concat(
                [
                    this_shape.iloc[:index_replacement_starts],
                    new_shape_rows,
                    this_shape.iloc[index_replacement_ends + 1 :],
                ],
                ignore_index=True,
                sort=False,
            )
        else:
            this_shape = new_shape_rows

        # Renumber shape_pt_sequence
        this_shape["shape_pt_sequence"] = np.arange(len(this_shape))

        # Add rows back into shapes
        shapes = pd.concat(
            [shapes[shapes.shape_id != shape_id], this_shape],
            ignore_index=True,
            sort=False,
        )

    # Replace stop_times and stops records (if required)
    if stops_change:
        # If node IDs in routing_change["set_stops"] are not already
        # in stops.txt, create a new stop_id for them in stops
        existing_fk_ids = set(stops[net.STOPS_FOREIGN_KEY].tolist())
        nodes_df = net.road_net.nodes_df.loc[
            :, [net.STOPS_FOREIGN_KEY, "X", "Y"]
        ]
        for fk_i in routing_change["set_stops"]:
            if fk_i not in existing_fk_ids:
                WranglerLogger.info(
                    "Creating a new stop in stops.txt for node ID: {}".format(fk_i)
                )
                # Add new row to stops
                new_stop_id = str(int(fk_i) + net.ID_SCALAR)
                if new_stop_id in stops["stop_id"].tolist():
                    WranglerLogger.error("Cannot create a unique new stop_id.")
                stops.loc[
                    len(stops.index) + 1,
                    [
                        "stop_id",
                        "stop_lat",
                        "stop_lon",
                        net.STOPS_FOREIGN_KEY,
                    ],
                ] = [
                    new_stop_id,
                    nodes_df.loc[
                        nodes_df[net.STOPS_FOREIGN_KEY] == int(fk_i), "Y"
                    ],
                    nodes_df.loc[
                        nodes_df[net.STOPS_FOREIGN_KEY] == int(fk_i), "X"
                    ],
                    fk_i,
                ]

        # Loop through all the trip_ids
        for trip_id in trip_ids:
            # Pop the rows that match trip_id
            this_stoptime = stop_times[stop_times.trip_id == trip_id]

            # Merge on node IDs using stop_id (one node ID per stop_id)
            this_stoptime = this_stoptime.merge(
                stops[["stop_id", net.STOPS_FOREIGN_KEY]],
                how="left",
                on="stop_id",
            )

            # Make sure the stop_times are ordered by stop_sequence
            this_stoptime = this_stoptime.sort_values(by=["stop_sequence"])

            # Build a pd.DataFrame of new shape records from properties
            new_stoptime_rows = pd.DataFrame(
                {
                    "trip_id": trip_id,
                    "arrival_time": None,
                    "departure_time": None,
                    "pickup_type": None,
                    "drop_off_type": None,
                    "stop_distance": None,
                    "timepoint": None,
                    "stop_is_skipped": None,
                    net.STOPS_FOREIGN_KEY: routing_change["set_stops"],
                }
            )

            # Merge on stop_id using node IDs (many stop_id per node ID)
            new_stoptime_rows = (
                new_stoptime_rows.merge(
                    stops[["stop_id", net.STOPS_FOREIGN_KEY]],
                    how="left",
                    on=net.STOPS_FOREIGN_KEY,
                )
                .groupby([net.STOPS_FOREIGN_KEY])
                .head(1)
            )  # pick first

            # If "existing" is specified, replace only that segment
            # Else, replace the whole thing
            if routing_change.get("existing") is not None:
                # Match list (remember stops are passed in with node IDs)
                nodes = this_stoptime[net.STOPS_FOREIGN_KEY].tolist()
                index_replacement_starts = nodes.index(
                    routing_change["existing_stops"][0]
                )
                index_replacement_ends = nodes.index(
                    routing_change["existing_stops"][-1]
                )
                this_stoptime = pd.concat(
                    [
                        this_stoptime.iloc[:index_replacement_starts],
                        new_stoptime_rows,
                        this_stoptime.iloc[index_replacement_ends + 1 :],
                    ],
                    ignore_index=True,
                    sort=False,
                )
            else:
                this_stoptime = new_stoptime_rows

            # Remove node ID
            del this_stoptime[net.STOPS_FOREIGN_KEY]

            # Renumber stop_sequence
            this_stoptime["stop_sequence"] = np.arange(len(this_stoptime))

            # Add rows back into stoptime
            stop_times = pd.concat(
                [stop_times[stop_times.trip_id != trip_id], this_stoptime],
                ignore_index=True,
                sort=False,
            )

        net.feed.shapes = shapes
        net.feed.stops = stops
        net.feed.stop_times = stop_times
    return net
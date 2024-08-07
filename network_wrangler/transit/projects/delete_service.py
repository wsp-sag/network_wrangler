"""Functions for adding a transit route to a TransitNetwork."""

from __future__ import annotations

import copy

from typing import TYPE_CHECKING, Optional

from ...logger import WranglerLogger

if TYPE_CHECKING:
    from ...roadway.network import RoadwayNetwork
    from ..network import TransitNetwork
    from ..feed.feed import Feed
    from ..selection import TransitSelection

class TransitRouteAddError(Exception):
    """Error raised when applying add transit route."""

    pass


def apply_transit_service_deletion(
    net: TransitNetwork, 
    selection: TransitSelection,
) -> TransitNetwork:
    """Delete transit service to TransitNetwork.

    Args:
        net (TransitNetwork): Network to modify.
        selection: TransitSelection object, created from a selection dictionary.
    
    Returns:
        TransitNetwork: Modified network.
    """
    WranglerLogger.debug("Applying delete transit service project.")
    
    trip_ids = selection.selected_trips
    net.feed = _delete_service_from_feed(net.feed, trip_ids)

    WranglerLogger.debug("Validating to network.")
    # TODO: add validation
    return net


def _delete_service_from_feed(
    feed: Feed, 
    trip_ids: list,
) -> Feed:
    WranglerLogger.debug("Deleting service from feed.")

    stop_times_df = feed.stop_times.copy()
    stops_df = feed.stops.copy()
    frequencies_df = feed.frequencies.copy()
    trips_df = feed.trips.copy()

    stop_times_df = stop_times_df[~stop_times_df.trip_id.isin(trip_ids)]
    stops_df = stops_df[~stops_df.trip_id.isin(trip_ids)]
    frequencies_df = frequencies_df[~frequencies_df.trip_id.isin(trip_ids)]
    trips_df = trips_df[~trips_df.trip_id.isin(trip_ids)]

    feed.stop_times = stop_times_df
    feed.stops = stops_df
    feed.frequencies = frequencies_df
    feed.trips = trips_df

    return feed

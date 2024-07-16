"""Functions for adding a transit route to a TransitNetwork."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ...logger import WranglerLogger

if TYPE_CHECKING:
    from ..network import TransitNetwork
    from ..feed.feed import Feed


class TransitRouteAddError(Exception):
    """Error raised when applying add transit route."""

    pass


def apply_add_transit_route_change(net: TransitNetwork, add_route: dict) -> TransitNetwork:
    """Add transit route to TransitNetwork.

    Args:
        net (TransitNetwork): Network to modify.
        add_route: #todo

    Returns:
        TransitNetwork: Modified network.
    """
    WranglerLogger.debug("Applying add transit route project.")

    net.feed = _add_route_to_feed(net.feed, add_route)

    WranglerLogger.debug("Validating to network.")
    # TODO: add validation
    return net


def _add_route_to_feed(feed: Feed, add_route):
    WranglerLogger.debug("Adding route to feed.")

    # TODO: Implement this function
    return feed

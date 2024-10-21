"""Module for applying calculated transit projects to a transit network object.

These projects are stored in project card `pycode` property as python code strings which are
executed to change the transit network object.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ...logger import WranglerLogger

if TYPE_CHECKING:
    from ...transit.network import TransitNetwork


def apply_calculated_transit(
    net: TransitNetwork,
    pycode: str,
) -> TransitNetwork:
    """Changes transit network object by executing pycode.

    Args:
        net: transit network to manipulate
        pycode: python code which changes values in the transit network object
    """
    WranglerLogger.debug("Applying calculated transit project.")
    exec(pycode)

    return net

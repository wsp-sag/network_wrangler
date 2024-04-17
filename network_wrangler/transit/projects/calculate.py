from ...logger import WranglerLogger


def apply_calculated_transit(
    net: "TransitNetwork",
    pycode: str,
) -> "TransitNetwork":
    """
    Changes transit network object by executing pycode.

    Args:
        net: transit network to manipulate
        pycode: python code which changes values in the transit network object
    """
    WranglerLogger.debug("Applying calculated transit project.")
    exec(pycode)

    return net

def apply_calculated_roadway(
    roadway_net: "RoadwayNetwork", pycode: str
) -> "RoadwayNetwork":
    """
    Changes roadway network object by executing pycode.

    Args:
        net: network to manipulate
        pycode: python code which changes values in the roadway network object
    """
    exec(pycode)
    return roadway_net

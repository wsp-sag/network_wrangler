import numbers

from ..logger import WranglerLogger
from ..utils import parse_time_spans_to_secs, meters_to_projected_distance


def apply_parallel_managed_lanes(
    roadway_net: "RoadwayNetwork",
    selection: "Selection",
    properties: dict,
    geometry_meters_offset=10,
) -> "RoadwayNetwork":
    """
    Apply the managed lane feature changes to the roadway network

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        selection : Selection instance
        properties : list of dictionarys roadway properties to change
        geometry_meters_offset: meters to the left to offset the parallel managed lanes from the
            roadway centerline. Can offset to the right by using a negative number. Default is
            10 meters.

    .. todo:: decide on connectors info when they are more specific in project card
    """
    link_idx = selection.selected_links

    # add ML flag to relevant links
    if "managed" in roadway_net.links_df.columns:
        roadway_net.links_df.loc[link_idx, "managed"] = 1
    else:
        roadway_net.links_df["managed"] = 0
        roadway_net.links_df.loc[link_idx, "managed"] = 1

    # figure out how far to offset geometry based on CRS being used
    geometry_projected_offset = meters_to_projected_distance(
        geometry_meters_offset, roadway_net.links_df
    )

    # create managed lane geometries
    roadway_net.links_df.loc[
        link_idx, "ML_geometry"
    ] = roadway_net.links_df.geometry.apply(
        lambda x: x.offset_curve(geometry_projected_offset)
    )

    for p in properties:
        attribute = p["property"]
        attr_value = ""

        for idx in link_idx:
            if "group" in p.keys():
                attr_value = {}

                if "set" in p.keys():
                    attr_value["default"] = p["set"]
                elif "change" in p.keys():
                    attr_value["default"] = (
                        roadway_net.links_df.at[idx, attribute] + p["change"]
                    )

                attr_value["timeofday"] = []

                for g in p["group"]:
                    category = g["category"]
                    for tod in g["timeofday"]:
                        if "set" in tod.keys():
                            attr_value["timeofday"].append(
                                {
                                    "category": category,
                                    "time": parse_time_spans_to_secs(tod["time"]),
                                    "value": tod["set"],
                                }
                            )
                        elif "change" in tod.keys():
                            attr_value["timeofday"].append(
                                {
                                    "category": category,
                                    "time": parse_time_spans_to_secs(tod["time"]),
                                    "value": roadway_net.links_df.at[idx, attribute]
                                    + tod["change"],
                                }
                            )

            elif "timeofday" in p.keys():
                attr_value = {}

                if "set" in p.keys():
                    attr_value["default"] = p["set"]
                elif "change" in p.keys():
                    attr_value["default"] = (
                        roadway_net.links_df.at[idx, attribute] + p["change"]
                    )

                attr_value["timeofday"] = []

                for tod in p["timeofday"]:
                    if "set" in tod.keys():
                        attr_value["timeofday"].append(
                            {
                                "time": parse_time_spans_to_secs(tod["time"]),
                                "value": tod["set"],
                            }
                        )
                    elif "change" in tod.keys():
                        attr_value["timeofday"].append(
                            {
                                "time": parse_time_spans_to_secs(tod["time"]),
                                "value": roadway_net.links_df.at[idx, attribute]
                                + tod["change"],
                            }
                        )
            elif "set" in p.keys():
                attr_value = p["set"]

            elif "change" in p.keys():
                attr_value = roadway_net.links_df.at[idx, attribute] + p["change"]

            if attribute in roadway_net.links_df.columns and not isinstance(
                attr_value, numbers.Number
            ):
                # if the attribute already exists
                # and the attr value we are trying to set is not numeric
                # then change the attribute type to object
                roadway_net.links_df[attribute] = roadway_net.links_df[
                    attribute
                ].astype(object)

            if attribute not in roadway_net.links_df.columns:
                # if it is a new attribute then initialize with NaN values
                roadway_net.links_df[attribute] = "NaN"

            roadway_net.links_df.at[idx, attribute] = attr_value

    WranglerLogger.debug(f"{len(roadway_net.nodes_df)} Nodes in Network")

    return roadway_net

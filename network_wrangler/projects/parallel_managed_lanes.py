from ..roadwaynetwork import RoadwayNetwork
from ..roadway.selection import RoadwaySelection
from ..logger import WranglerLogger


def apply_parallel_managed_lanes(
    roadway_net: RoadwayNetwork,
    selection: RoadwaySelection,
    property_changes: dict,
    geometry_meters_offset=10,
    meters_crs: int = 4326,
) -> RoadwayNetwork:
    """
    Apply the managed lane feature changes to the roadway network

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        selection : Selection instance
        property_changes : dictionary of  roadway properties to change
        geometry_meters_offset: meters to the left to offset the parallel managed lanes from the
            roadway centerline. Can offset to the right by using a negative number. Default is
            10 meters.
        meters_crs: meters-based coordinate reference system to use when doing the meters offset.
            Defaults to 4326 or web-mercador

    .. todo:: decide on connectors info when they are more specific in project card
    """
    link_idx = selection.selected_links

    # add ML flag to relevant links
    roadway_net.links_df = roadway_net.links_df.set_link_prop(
        link_idx, "managed", {"set": 1, "default": 0}
    )

    # --- create managed lane geometries by offsetting by X meters
    og_crs = roadway_net.links_df.crs

    # TODO: consider a shortcut if the re-projecting takes a long time
    roadway_net.links_df.to_crs(meters_crs)
    WranglerLogger.info(f"og_crs: {og_crs}; meters_crs: {roadway_net.links_df.crs}")

    roadway_net.links_df.loc[
        link_idx, "ML_geometry"
    ] = roadway_net.links_df.geometry.apply(
        lambda x: x.offset_curve(geometry_meters_offset)
    )
    roadway_net.links_df.to_crs(og_crs)

    # --- Copy properties to nested dict.
    for property, property_dict in property_changes.items():
        roadway_net.links_df = roadway_net.links_df.set_link_prop(
            link_idx, property, property_dict
        )

    WranglerLogger.debug(f"{len(roadway_net.nodes_df)} Nodes in Network")

    return roadway_net

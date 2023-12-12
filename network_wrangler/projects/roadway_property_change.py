from ..logger import WranglerLogger
from ..roadway.selection import SelectionFormatError

class RoadwayPropertyChangeError(Exception):
    pass

def apply_roadway_property_change(
    roadway_net: "RoadwayNetwork",
    selection: "RoadwaySelection",
    property_change: dict,
) -> "RoadwayNetwork":
    """
    Changes the roadway attributes for the selected features based on the
    project card information passed

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        selection : roadway selection object
        property_change : dictionary of roadway properties to change.
            e.g.

            ```yml
            #changes number of lanes 3 to 2 (reduction of 1) and adds a bicycle lane
            lanes:
                existing: 3
                change: -1
            bicycle_facility:
                set: 2
            ```
    """
    WranglerLogger.debug("Applying roadway property change project.")
    # should only be for links or nodes at once, not both
    if not len(selection.feature_types) == 1:
        raise SelectionFormatError(
            f"Should have exactly 1 feature type for roadway\
            property change. Found: {selection.feature_types}"
        )

    if "links" in selection.feature_types:
        for property, property_dict in property_change.items():
            roadway_net.links_df = roadway_net.links_df.set_link_prop(
                selection.selected_links, property, property_dict
            )
    elif "nodes" in selection.feature_types:
        for property, property_dict in property_change.items():
            roadway_net.nodes_df = roadway_net.nodes_df.set_node_prop(
                selection.selected_nodes, property, property_dict, _geometry_ok=True
            )

        _ok_geom_props = [
            roadway_net.nodes_df.params.x_field,
            roadway_net.nodes_df.params.y_field,
        ]
        if not set(list(property_change.keys())).isdisjoint(_ok_geom_props):
            roadway_net.update_network_geometry_from_node_xy(selection.selected_nodes)

    else:
        raise RoadwayPropertyChangeError("geometry_type must be either 'links' or 'nodes'")

    return roadway_net

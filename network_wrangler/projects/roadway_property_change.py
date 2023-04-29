import numpy as np
import pandas as pd

from ..logger import WranglerLogger


def apply_roadway_property_change(
    roadway_net: "RoadwayNetwork",
    df_idx: list,
    properties: dict,
    geometry_type="links",
) -> "RoadwayNetwork":
    """
    Changes the roadway attributes for the selected features based on the
    project card information passed

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        df_idx : list
            lndices of all links or nodes to apply change to
        properties : list of dictionarys
            roadway properties to change
        geometry_type: either 'links' or 'nodes'. Defaults to 'link'
    """
    if geometry_type == "links":
        roadway_net._apply_links_feature_change(df_idx, properties)
    elif geometry_type == "nodes":
        roadway_net._apply_nodes_feature_change(df_idx, properties)
    else:
        raise ValueError("geometry_type must be either 'links' or 'nodes'")

    return roadway_net


def _apply_nodes_feature_change(
    roadway_net: "RoadwayNetwork",
    node_idx: list,
    properties: dict,
) -> "RoadwayNetwork":
    """
    Changes the roadway attributes for the selected nodes based on the
    project card information passed

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        df_idx : list of indices of all links or nodes to apply change to
        properties : list of dictionarys
            roadway properties to change
    """
    WranglerLogger.debug("Updating Nodes")

    roadway_net.validate_properties(roadway_net.nodes_df, properties)
    for p in properties:
        if not p["property"] in roadway_net.nodes_df.columns:
            roadway_net.nodes_df = roadway_net._add_property(roadway_net.nodes_df, p)

        _updated_nodes_df = roadway_net._update_property(
            roadway_net.nodes_df.loc[node_idx], p
        )
        roadway_net.nodes_df.update(_updated_nodes_df)

    _property_names = [p["property"] for p in properties]

    WranglerLogger.info(
        f"Updated following node properties: \
        {','.join(_property_names)}"
    )

    if [p for p in _property_names if p in roadway_net.GEOMETRY_PROPERTIES]:
        roadway_net.update_node_geometry(node_idx)
        WranglerLogger.debug("Updated node geometry and associated links/shapes.")
    return roadway_net.nodes_df


def _apply_links_feature_change(
    roadway_net: "RoadwayNetwork",
    link_idx: list,
    properties: dict,
) -> "RoadwayNetwork":
    """
    Changes the roadway attributes for the selected links based on the
    project card information passed

    Args:
    roadway_net: input RoadwayNetwork to apply change to
        link_idx : list od indices of all links to apply change to
        properties : list of dictionarys
            roadway properties to change
    """
    WranglerLogger.debug("Updating Links.")

    roadway_net.validate_properties(roadway_net.links_df, properties)
    for p in properties:
        if not p["property"] in roadway_net.links_df.columns:
            roadway_net.links_df = roadway_net._add_property(roadway_net.links_df, p)

        _updated_links_df = roadway_net._update_property(
            roadway_net.links_df.loc[link_idx], p
        )
        roadway_net.links_df.update(_updated_links_df)

    WranglerLogger.info(
        f"Updated following link properties: \
        {','.join([p['property'] for p in properties])}"
    )

    return roadway_net.links_df


def _add_property(roadway_net, df: pd.DataFrame, property_dict: dict) -> pd.DataFrame:
    """
    Adds a property to a dataframe. Infers type from the property_dict "set" value.

    Args:
        df: dataframe to add property to
        property_dict: dictionary of property to add with "set" value.

    Returns:
        pd.DataFrame: dataframe with property added filled with NaN.
    """
    WranglerLogger.info(f"Adding property: {property_dict['property']}")
    df[property_dict["property"]] = np.nan
    return df


def _update_property(roadway_net, existing_facilities_df: pd.DataFrame, property: dict):
    """_summary_

    Args:
        existing_facilities_df: selected existing facility df
        property (dict): project property update
    """
    # WranglerLogger.debug(f"property:\n{property}")
    # WranglerLogger.debug(f"existing_facilities_df:\n{existing_facilities_df}")
    if "existing" in property:
        if (
            not existing_facilities_df[property["property"]]
            .eq(property["existing"])
            .all()
        ):
            WranglerLogger.warning(
                "Existing value defined for {} in project card does "
                "not match the value in the roadway network for the "
                "selected links".format(property["property"])
            )

    if "set" in property:
        _updated_series = pd.Series(
            property["set"],
            name=property["property"],
            index=existing_facilities_df.index,
        )

    elif "change" in property:
        _updated_series = (
            existing_facilities_df[property["property"]] + property["change"]
        )
    else:
        WranglerLogger.debug(f"Property: \n {property}")
        raise ValueError(
            f"No 'set' or 'change' specified for property {property['property']} \
                in Roadway Network Change project card"
        )
    return _updated_series

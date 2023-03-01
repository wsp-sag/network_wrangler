import copy
from typing import Tuple

import geopandas as gpd
import pandas as pd

from network_wrangler import RoadwayNetwork, WranglerLogger
from network_wrangler.utils import offset_location_reference, create_unique_shape_id
from network_wrangler.utils import (
    line_string_from_location_references,
    haversine_distance,
)
from network_wrangler.utils import location_reference_from_nodes

MANAGED_LANES_NODE_ID_SCALAR = 500000
MANAGED_LANES_LINK_ID_SCALAR = 1000000


def create_managed_lane_network(net: RoadwayNetwork) -> RoadwayNetwork:
    """Create a roadway network with managed lanes links separated out.

    Add new parallel managed lane links, access/egress links,
    and add shapes corresponding to the new links

    net: RoadwayNetwork instance
    returns: A RoadwayNetwork instance with model nodes, links and shapes stored as;
        m_nodes_df
        m_links_df
        m_shapes_df
    """
    # WranglerLogger.debug(f"1-net.nodes_df: \n {net.nodes_df[['model_node_id']]}")
    WranglerLogger.info("Creating model network with separate managed lanes")
    net.m_links_df, net.m_nodes_df = _create_separate_managed_lane_links_nodes(net)
    # WranglerLogger.debug(f"1-net.m_links_df: \n {net.m_links_df[['model_link_id','name']]}")
    # WranglerLogger.debug(f"1-net.m_nodes_df: \n {net.m_nodes_df[['model_node_id']]}")
    # Adds dummy connector links
    access_egress_links_df = _create_dummy_connector_links(net)
    net.m_links_df = pd.concat([net.m_links_df, access_egress_links_df])
    # WranglerLogger.debug(f"2-net.m_links_df: \n {net.m_links_df[['model_link_id','name']]}")
    # WranglerLogger.debug(f"2-net.m_nodes_df: \n {net.m_nodes_df[['model_node_id']]}")

    net.m_shapes_df = copy.deepcopy(net.shapes_df)

    _compare_table = (
        "\nProperty |  Input network | Managed Lane Network\n" + "=" * 48 + "\n"
    )
    _compare_table += f"links:   | {len(net.links_df):14} | {len(net.m_links_df):14}\n"
    _compare_table += f"nodes:   | {len(net.nodes_df):14} | {len(net.m_nodes_df):14}\n"
    _compare_table += (
        f"shapes:  | {len(net.shapes_df):14} | {len(net.m_shapes_df):14}\n"
    )
    WranglerLogger.debug(_compare_table)

    WranglerLogger.debug(
        f"Managed Lane Links:\n {net.m_links_df[net.m_links_df['managed']==1]}"
    )
    WranglerLogger.debug(
        f"Managed Lane Access Links:\n {net.m_links_df[net.m_links_df['roadway']=='ml_access']}"
    )
    WranglerLogger.debug(
        f"Managed Lane Egress Links:\n {net.m_links_df[net.m_links_df['roadway']=='ml_egress']}"
    )

    return net


def _create_separate_managed_lane_links_nodes(
    net: RoadwayNetwork,
) -> Tuple[gpd.GeoDataFrame]:
    """Creates self.m_links_df and self.m_nodes_df which has separate links for managed lanes.

    net: RoadwayNetwork instance
    returns: A tuple of model links and nodes dataframe
        m_links_df
        m_nodes_df
    """

    link_properties = net.links_df.columns.values.tolist()

    ml_properties = [i for i in link_properties if i.startswith("ML_")]

    # no_ml_links are links in the network where there is no managed lane.
    # gp_links are the gp lanes and ml_links are ml lanes respectively for the ML roadways.

    no_ml_links_df = copy.deepcopy(net.links_df[net.links_df["managed"] != 1])
    no_ml_links_df = no_ml_links_df.drop(ml_properties, axis=1)

    ml_links_df = copy.deepcopy(net.links_df[net.links_df["managed"] == 1])
    gp_links_df = ml_links_df.drop(ml_properties, axis=1)

    # copy relevant properties from GP lanes to managed lanes
    for prop in link_properties:
        if prop == "name":
            ml_links_df["name"] = "Managed Lane " + gp_links_df["name"]
        elif prop in ml_properties and prop not in ["ML_ACCESS", "ML_EGRESS"]:
            gp_prop = prop.split("_", 1)[1]
            ml_links_df.loc[:, gp_prop] = ml_links_df[prop]
        elif (
            prop not in RoadwayNetwork.KEEP_SAME_ATTRIBUTES_ML_AND_GP
            and prop not in RoadwayNetwork.MANAGED_LANES_REQUIRED_ATTRIBUTES
        ):
            ml_links_df[prop] = ""

    ml_links_df = ml_links_df.drop(ml_properties, axis=1)

    ml_links_df["managed"] = 1
    gp_links_df["managed"] = -1

    ml_links_df["GP_A"] = ml_links_df["A"]
    ml_links_df["A"] = ml_links_df["A"].apply(
        lambda x: _node_id_to_managed_lane_node_id(x)
    )
    ml_links_df["GP_B"] = ml_links_df["B"]
    ml_links_df["B"] = ml_links_df["B"].apply(
        lambda x: _node_id_to_managed_lane_node_id(x)
    )

    _ref_gp_node_list = list(set(list(ml_links_df["GP_A"]) + list(ml_links_df["GP_B"])))
    WranglerLogger.debug(f"_ref_gp_node_list: \n{_ref_gp_node_list}")
    # update geometry and location references
    ml_links_df["locationReferences"] = ml_links_df["locationReferences"].apply(
        # lambda x: _update_location_reference(x)
        lambda x: offset_location_reference(x)
    )

    ml_links_df["geometry"] = ml_links_df["locationReferences"].apply(
        lambda x: line_string_from_location_references(x)
    )

    # Get new node,link and shape IDs for managed lane mirrors
    ml_nodes_df = copy.deepcopy(net.nodes_df.loc[_ref_gp_node_list])
    WranglerLogger.debug(f"1-ml_nodes_df: \n{ml_nodes_df}")
    ml_nodes_df["GP_" + RoadwayNetwork.UNIQUE_NODE_KEY] = ml_nodes_df[
        RoadwayNetwork.UNIQUE_NODE_KEY
    ]
    WranglerLogger.debug(f"2-ml_nodes_df: \n{ml_nodes_df}")
    ml_nodes_df[RoadwayNetwork.UNIQUE_NODE_KEY] = ml_nodes_df[
        RoadwayNetwork.UNIQUE_NODE_KEY
    ].apply(lambda x: _node_id_to_managed_lane_node_id(x))
    ml_nodes_df[RoadwayNetwork.UNIQUE_NODE_KEY + "_idx"] = ml_nodes_df[
        RoadwayNetwork.UNIQUE_NODE_KEY
    ]
    ml_nodes_df.set_index(RoadwayNetwork.UNIQUE_NODE_KEY + "_idx", inplace=True)
    WranglerLogger.debug(f"3-ml_nodes_df: \n{ml_nodes_df[['model_node_id']]}")
    ml_links_df[RoadwayNetwork.UNIQUE_LINK_KEY] = ml_links_df[
        RoadwayNetwork.UNIQUE_LINK_KEY
    ].apply(lambda x: _link_id_to_managed_lane_link_id(x))

    ml_links_df[RoadwayNetwork.UNIQUE_LINK_KEY + "_idx"] = ml_links_df[
        RoadwayNetwork.UNIQUE_LINK_KEY
    ]
    ml_links_df.set_index(RoadwayNetwork.UNIQUE_LINK_KEY + "_idx", inplace=True)
    ml_links_df[RoadwayNetwork.UNIQUE_SHAPE_KEY] = ml_links_df["geometry"].apply(
        lambda x: create_unique_shape_id(x)
    )

    # Adds any missing A-nodes or B-nodes
    a_nodes_df = net._nodes_from_link(ml_links_df[["A", "geometry"]], 0, "A")
    # WranglerLogger.debug(f"a_nodes_df:\n {a_nodes_df}")
    ml_nodes_df = ml_nodes_df.combine_first(a_nodes_df)
    # WranglerLogger.debug(f"4-ml_nodes_df: \n{ml_nodes_df[['model_node_id']]}")
    b_nodes_df = net._nodes_from_link(ml_links_df[["B", "geometry"]], -1, "B")
    ml_nodes_df = ml_nodes_df.combine_first(b_nodes_df)
    # WranglerLogger.debug(f"5-ml_nodes_df: \n{ml_nodes_df[['model_node_id']]}")
    m_links_df = pd.concat([ml_links_df, gp_links_df, no_ml_links_df])
    m_nodes_df = pd.concat([net.nodes_df, ml_nodes_df])
    # WranglerLogger.debug(f"6-ml_nodes_df: \n{ml_nodes_df[['model_node_id']]}")
    WranglerLogger.debug(
        f"Added {len(ml_nodes_df)} additional managed lane nodes\
        to the {len(net.nodes_df)} in net.nodes_df"
    )

    return m_links_df, m_nodes_df


def _node_id_to_managed_lane_node_id(model_node_id):
    return MANAGED_LANES_NODE_ID_SCALAR + model_node_id


def _get_managed_lane_node_ids(nodes_list):
    return [_node_id_to_managed_lane_node_id(x) for x in nodes_list]


def _link_id_to_managed_lane_link_id(model_link_id):
    return MANAGED_LANES_LINK_ID_SCALAR + model_link_id


def _access_model_link_id(model_link_id):
    return 1 + model_link_id + _link_id_to_managed_lane_link_id(model_link_id)


def _egress_model_link_id(model_link_id):
    return 2 + model_link_id + _link_id_to_managed_lane_link_id(model_link_id)


def _create_dummy_connector_links(net: RoadwayNetwork) -> gpd.GeoDataFrame:
    """
    Create dummy connector links between the general purpose and managed lanes

    If no specified access or egree points exist, as determined by the existance of either
            "ML_access_point" or  "ML_egress_point" **anywhere in the network**, assumes that
            access and egress can happen at any point.

    TODO Right now it assumes that if "ML_ACCESS_POINT" is specified that all Managed Lanes
    have their access points specified and the same for "ML_EGRESS_POINT"

    net: RoadwayNetwork instance with m_nodes_df and m_links_df
    returns: GeoDataFrame of access and egress dummy connector links to add to m_links_df
    """

    # 1. Align the managed lane and associated general purpose lanes in the same records
    _keep_cols = [
        "A",
        "B",
        "GP_A",
        "GP_B",
        "name",
        "model_link_id",
        "access",
        "drive_access",
        "locationReferences",
    ]

    _optional_cols_to_keep = ["ML_access_point", "ML_egress_point", "ref"]
    for c in _optional_cols_to_keep:
        if c in net.m_links_df.columns:
            _keep_cols.append(c)

    _managed_lanes_df = net.m_links_df.loc[net.m_links_df["managed"] == 1, _keep_cols]
    _gp_w_parallel_ml_df = net.m_links_df.loc[
        net.m_links_df["managed"] == -1, _keep_cols
    ]

    #    Gen Purp   |  Managed Lane | name_GP  | name_ML |
    #   GP_A | GP_B |   A  |   B    |
    gp_ml_links_df = _gp_w_parallel_ml_df.merge(
        _managed_lanes_df,
        suffixes=("_GP", "_ML"),
        left_on=["A", "B"],
        right_on=["GP_A", "GP_B"],
        how="inner",
    )

    # 2 - Create access and egress link dataframes from aligned records
    # if ML_access_point is specified, only have access at those points. Same for egress.
    if "ML_access_point" in gp_ml_links_df.columns:
        access_df = copy.deepcopy(
            gp_ml_links_df.loc[
                gp_ml_links_df[RoadwayNetwork.LINK_FOREIGN_KEY[0]]
                == gp_ml_links_df["ML_access_point"]
            ]
        )
    else:
        access_df = copy.deepcopy(gp_ml_links_df)

    if "ML_egress_point" in gp_ml_links_df.columns:
        egress_df = copy.deepcopy(
            gp_ml_links_df.loc[
                gp_ml_links_df[RoadwayNetwork.LINK_FOREIGN_KEY[1]]
                == gp_ml_links_df["ML_egress_point"]
            ]
        )
    else:
        egress_df = copy.deepcopy(gp_ml_links_df)

    # access link should go from A_GP to A_ML
    access_df["A"] = access_df["A_GP"]
    access_df["B"] = access_df["A_ML"]
    access_df["model_link_id"] = access_df["model_link_id_GP"].apply(
        _access_model_link_id
    )
    access_df[RoadwayNetwork.UNIQUE_LINK_KEY + "_idx"] = access_df[
        RoadwayNetwork.UNIQUE_LINK_KEY
    ]
    access_df.set_index(RoadwayNetwork.UNIQUE_LINK_KEY + "_idx", inplace=True)
    access_df["name"] = "Access Dummy " + access_df["name_GP"]
    access_df["roadway"] = "ml_access"

    # egress link should go from B_ML to B_GP
    egress_df["A"] = egress_df["B_ML"]
    egress_df["B"] = egress_df["B_GP"]
    egress_df["model_link_id"] = egress_df["model_link_id_GP"].apply(
        _egress_model_link_id
    )
    egress_df[RoadwayNetwork.UNIQUE_LINK_KEY + "_idx"] = egress_df[
        RoadwayNetwork.UNIQUE_LINK_KEY
    ]
    egress_df.set_index(RoadwayNetwork.UNIQUE_LINK_KEY + "_idx", inplace=True)
    egress_df["name"] = "Egress Dummy " + egress_df["name_GP"]
    egress_df["roadway"] = "ml_egress"

    # combine to one dataframe
    access_egress_df = pd.concat([access_df, egress_df])

    # 3 - Determine property values
    access_egress_df["lanes"] = 1
    access_egress_df["access"] = access_egress_df["access_ML"]
    access_egress_df["drive_access"] = access_egress_df["drive_access_ML"]
    if "ref_GP" in access_egress_df.columns:
        access_egress_df["ref"] = access_egress_df["ref_GP"]

    # remove extraneous fields
    _keep_ae_cols = [
        "A",
        "B",
        "name",
        "model_link_id",
        "roadway",
        "lanes",
        "access",
        "drive_access",
        "ref",
    ]
    _keep_ae_cols = [c for c in _keep_ae_cols if c in access_egress_df.columns]
    access_egress_df = access_egress_df[_keep_ae_cols]

    # 4 - Create various geometry fields from A and B nodes
    WranglerLogger.debug(f"m_nodes_df length: {len(net.m_nodes_df)}")
    # LocationReferences
    access_egress_df["locationReferences"] = access_egress_df.apply(
        lambda x: location_reference_from_nodes(
            [
                net.m_nodes_df[
                    net.m_nodes_df[RoadwayNetwork.NODE_FOREIGN_KEY_TO_LINK] == x["A"]
                ].squeeze(),
                net.m_nodes_df[
                    net.m_nodes_df[RoadwayNetwork.NODE_FOREIGN_KEY_TO_LINK] == x["B"]
                ].squeeze(),
            ]
        ),
        axis=1,
    )

    # Geometry
    access_egress_df["geometry"] = access_egress_df["locationReferences"].apply(
        lambda x: line_string_from_location_references(x)
    )

    WranglerLogger.debug(
        f"access_egress_df['locationReferences']: \n {access_egress_df['locationReferences']}"
    )
    # Distance
    # TODO make this a shapely call instead?
    access_egress_df["distance"] = access_egress_df["locationReferences"].apply(
        lambda x: haversine_distance(
            x[0]["point"],
            x[-1]["point"],
        )
    )

    # Shape
    access_egress_df[RoadwayNetwork.UNIQUE_SHAPE_KEY] = access_egress_df[
        "geometry"
    ].apply(lambda x: create_unique_shape_id(x))

    WranglerLogger.debug(f"Returning {len(access_egress_df)} access and egress links.")

    return access_egress_df

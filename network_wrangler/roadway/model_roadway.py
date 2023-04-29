import os
import copy
from typing import Tuple

import geopandas as gpd
import pandas as pd

from network_wrangler import WranglerLogger
from network_wrangler.utils import offset_location_reference, create_unique_shape_id
from network_wrangler.utils import (
    line_string_from_location_references,
    haversine_distance,
)
from network_wrangler.utils import location_reference_from_nodes


"""
scalar value added to the general purpose lanes' `model_node_id` when creating
    an associated node for a parallel managed lane
"""
MANAGED_LANES_NODE_ID_SCALAR = 500000

"""
scalar value added to the general purpose lanes' `model_link_id` when creating
    an associated link for a parallel managed lane
"""
MANAGED_LANES_LINK_ID_SCALAR = 1000000

"""
(list(str)): list of attributes
that must be provided in managed lanes
"""
MANAGED_LANES_REQUIRED_ATTRIBUTES = [
    "A",
    "B",
    "model_link_id",
    "locationReferences",
]

"""
(list(str)): list of attributes
to copy from a general purpose lane to managed lane
"""
KEEP_SAME_ATTRIBUTES_ML_AND_GP = [
    "distance",
    "bike_access",
    "drive_access",
    "transit_access",
    "walk_access",
    "maxspeed",
    "name",
    "oneway",
    "ref",
    "roadway",
    "length",
    "segment_id",
]


class ModelRoadwayNetwork:
    """Roadway Network Object compatible with travel modeling.

    Compatability includes:
    (1) separation of managed lane facilities and their connection to general purpose lanes
        using dummy links.

    Attr:
        net: associated RoadwayNetwork object
        m_links_df: dataframe of model-compatible links
        m_nodes_df: dataframe of model-compatible nodes
        managed_lanes_link_id_scalar (_type_, optional):  scalar value added to the general
            purpose lanes' `model_link_id` when creating an associated link for a parallel
            managed lane. Defaults to MANAGED_LANES_LINK_ID_SCALAR which defaults to 100,000.
        managed_lanes_node_id_scalar (_type_, optional): scalar value added to the general
            purpose lanes' `model_node_id` when creating an associated node for a parallel
            managed lane. Defaults to MANAGED_LANES_NODE_ID_SCALAR which defaults to
            500,000
        _net_hash: hash of the the input links and nodes in order to detect changes.

    """

    def __init__(
        self,
        net,
        managed_lanes_link_id_scalar=MANAGED_LANES_LINK_ID_SCALAR,
        managed_lanes_node_id_scalar=MANAGED_LANES_NODE_ID_SCALAR,
    ):
        """
        Constructor for ModelRoadwayNetwork.

        NOTE: in order to be associated with the RoadwayNetwork, this should be called from
        RoadwayNetwork.model_net which will lazily construct it.

        Args:
            net (_type_): Associated roadway network.
            managed_lanes_link_id_scalar (_type_, optional):  scalar value added to the general
                purpose lanes' `model_link_id` when creating an associated link for a parallel
                managed lane. Defaults to MANAGED_LANES_LINK_ID_SCALAR which defaults to 100,000.
            managed_lanes_node_id_scalar (_type_, optional): scalar value added to the general
                purpose lanes' `model_node_id` when creating an associated node for a parallel
                managed lane. Defaults to MANAGED_LANES_NODE_ID_SCALAR which defaults to
                500,000
        """

        self.net = net
        self.m_links_df, self.m_nodes_df = self.model_links_nodes_from_net(net)

        self.managed_lanes_link_id_scalar = managed_lanes_link_id_scalar
        self.managed_lanes_node_id_scalar = managed_lanes_node_id_scalar

        self._net_hash = copy(net.network_hash)

    def _node_id_to_managed_lane_node_id(self, model_node_id):
        return self.managed_lanes_node_id_scalar + model_node_id

    def _get_managed_lane_node_ids(self, nodes_list):
        return [self._node_id_to_managed_lane_node_id(x) for x in nodes_list]

    def _link_id_to_managed_lane_link_id(self, model_link_id):
        return self.managed_lanes_link_id_scalar + model_link_id

    def _access_model_link_id(self, model_link_id):
        return 1 + model_link_id + self._link_id_to_managed_lane_link_id(model_link_id)

    def _egress_model_link_id(self, model_link_id):
        return 2 + model_link_id + self._link_id_to_managed_lane_link_id(model_link_id)

    def write(
        self,
        path: str = ".",
        filename: str = "",
    ) -> None:
        """
        Writes the links/nodes to CSVs. Merges geometry from self.net.shapes_df before writing.

        args:
            path: the path were the output will be saved
            filename: the name prefix of the roadway files that will be generated
        """

        if not os.path.exists(path):
            WranglerLogger.debug("\nPath [%s] doesn't exist; creating." % path)
            os.mkdir(path)

        links_file = os.path.join(path, f"{filename}{'_' if filename else ''}link.csv")
        nodes_file = os.path.join(path, f"{filename}{'_' if filename else ''}node.csv")

        link_shapes_df = self.m_links_df.merge(
            self.net.shapes_df,
            left_on=self.net.UNIQUE_SHAPE_KEY,
            right_on=self.net.UNIQUE_SHAPE_KEY,
            how="left",
        )

        link_shapes_df.to_csv(links_file)
        self.m_nodes_df.to_csv(nodes_file)
        WranglerLogger.info(
            f"Wrote ModelRoadwayNetwork to files:\n - {links_file}\n - {nodes_file}"
        )

    def model_links_nodes_from_net(self, net) -> tuple[pd.DataFrame]:
        """Create a roadway network with managed lanes links separated out.

        Add new parallel managed lane links, access/egress links,
        and add shapes corresponding to the new links

        net: RoadwayNetwork instance
        returns: A RoadwayNetwork instance with model nodes, links and shapes stored as;
            m_nodes_df
            m_links_df
        """
        WranglerLogger.info("Creating model network with separate managed lanes")
        m_links_df, m_nodes_df = self._create_separate_managed_lane_links_nodes(net)

        # Adds dummy connector links
        access_egress_links_df = self._create_dummy_connector_links(net)
        m_links_df = pd.concat([m_links_df, access_egress_links_df])

        WranglerLogger.debug(
            f"Managed Lane Links:\n {self.m_links_df[self].m_links_df['managed']==1}"
        )
        WranglerLogger.debug(
            f"Managed Lane Access Links:\n \
                {self.m_links_dfs[self.m_links_df['roadway']=='ml_access']}"
        )
        WranglerLogger.debug(
            f"Managed Lane Egress Links:\n \
                {self.m_links_df[self.m_links_df['roadway']=='ml_egress']}"
        )
        _compare_table = basic_compare_networks(self.net, self)
        WranglerLogger.info(_compare_table)

        return m_links_df, m_nodes_df

    def _create_separate_managed_lane_links_nodes(
        self,
    ) -> Tuple[gpd.GeoDataFrame]:
        """Creates self.m_links_df and self.m_nodes_df which has separate links for managed lanes.

        net: RoadwayNetwork instance
        returns: A tuple of model links and nodes dataframe
            m_links_df
            m_nodes_df
        """

        link_properties = self.net.links_df.columns.values.tolist()

        ml_properties = [i for i in link_properties if i.startswith("ML_")]

        # no_ml_links are links in the network where there is no managed lane.
        # gp_links are the gp lanes and ml_links are ml lanes respectively for the ML roadways.

        no_ml_links_df = copy.deepcopy(
            self.net.links_df[self.net.links_df["managed"] != 1]
        )
        no_ml_links_df = no_ml_links_df.drop(ml_properties, axis=1)

        ml_links_df = copy.deepcopy(
            self.net.links_df[self.net.links_df["managed"] == 1]
        )
        gp_links_df = ml_links_df.drop(ml_properties, axis=1)

        # copy relevant properties from GP lanes to managed lanes
        for prop in link_properties:
            if prop == "name":
                ml_links_df["name"] = "Managed Lane " + gp_links_df["name"]
            elif prop in ml_properties and prop not in ["ML_ACCESS", "ML_EGRESS"]:
                gp_prop = prop.split("_", 1)[1]
                ml_links_df.loc[:, gp_prop] = ml_links_df[prop]
            elif (
                prop not in KEEP_SAME_ATTRIBUTES_ML_AND_GP
                and prop not in MANAGED_LANES_REQUIRED_ATTRIBUTES
            ):
                ml_links_df[prop] = ""

        ml_links_df = ml_links_df.drop(ml_properties, axis=1)

        ml_links_df["managed"] = 1
        gp_links_df["managed"] = -1

        ml_links_df[f"GP_{self.net.links_df.params.from_node}"] = ml_links_df[
            self.net.links_df.params.from_node
        ]
        ml_links_df[self.net.links_df.params.from_node] = ml_links_df[
            self.net.links_df.params.from_node
        ].apply(lambda x: self._node_id_to_managed_lane_node_id(x))
        ml_links_df[f"GP_{self.net.links_df.params.to_node}"] = ml_links_df[
            self.net.links_df.params.to_node
        ]
        ml_links_df[self.net.links_df.params.to_node] = ml_links_df[
            self.net.links_df.params.to_node
        ].apply(lambda x: self._node_id_to_managed_lane_node_id(x))

        _ref_gp_node_list = list(
            set(
                list(ml_links_df[f"GP_{self.net.links_df.params.from_node}"])
                + list(ml_links_df[f"GP_{self.net.links_df.params.to_node}"])
            )
        )
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
        ml_nodes_df = copy.deepcopy(self.net.nodes_df.loc[_ref_gp_node_list])
        WranglerLogger.debug(f"1-ml_nodes_df: \n{ml_nodes_df}")
        ml_nodes_df["GP_" + self.net.nodes_df.params.primary_key] = ml_nodes_df[
            self.net.nodes_df.params.primary_key
        ]
        WranglerLogger.debug(f"2-ml_nodes_df: \n{ml_nodes_df}")
        ml_nodes_df[self.net.nodes_df.params.primary_key] = ml_nodes_df[
            self.net.nodes_df.params.primary_key
        ].apply(lambda x: self._node_id_to_managed_lane_node_id(x))
        ml_nodes_df[self.net.nodes_df.params.idx_col] = ml_nodes_df[
            self.net.nodes_df.params.primary_key
        ]
        ml_nodes_df.set_index(self.net.nodes_df.params.idx_col, inplace=True)
        WranglerLogger.debug(
            f"3-ml_nodes_df: \n{ml_nodes_df[[self.net.nodes_df.params.primary_key]]}"
        )
        ml_links_df[self.net.links_df.params.primary_key] = ml_links_df[
            self.net.links_df.params.primary_key
        ].apply(lambda x: self._link_id_to_managed_lane_link_id(x))

        ml_links_df[self.net.nodes_df.params.idx_col] = ml_links_df[
            self.net.nodes_df.params.primary_key
        ]
        ml_links_df.set_index(self.net.links_df.params.idx_col, inplace=True)
        ml_links_df[self.net.links_df.params.fk_to_shape] = ml_links_df[
            "geometry"
        ].apply(lambda x: create_unique_shape_id(x))

        # Adds any missing A-nodes or B-nodes
        a_nodes_df = self.net._nodes_from_link(
            ml_links_df[[self.net.links_df.params.from_node, "geometry"]],
            0,
            self.net.links_df.params.from_node,
        )
        # WranglerLogger.debug(f"a_nodes_df:\n {a_nodes_df}")
        ml_nodes_df = ml_nodes_df.combine_first(a_nodes_df)
        # WranglerLogger.debug(f"4-ml_nodes_df: \n{ml_nodes_df[['model_node_id']]}")
        b_nodes_df = self.net._nodes_from_link(
            ml_links_df[[self.net.links_df.params.to_node, "geometry"]],
            -1,
            self.net.links_df.params.to_node,
        )
        ml_nodes_df = ml_nodes_df.combine_first(b_nodes_df)
        # WranglerLogger.debug(f"5-ml_nodes_df: \n{ml_nodes_df[['model_node_id']]}")
        m_links_df = pd.concat([ml_links_df, gp_links_df, no_ml_links_df])
        m_nodes_df = pd.concat([self.net.nodes_df, ml_nodes_df])
        # WranglerLogger.debug(f"6-ml_nodes_df: \n{ml_nodes_df[['model_node_id']]}")
        WranglerLogger.debug(
            f"Added {len(ml_nodes_df)} additional managed lane nodes\
            to the {len(self.net.nodes_df)} in net.nodes_df"
        )

        return m_links_df, m_nodes_df

    def _create_dummy_connector_links(self) -> gpd.GeoDataFrame:
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
        _keep_cols = self.net.links_df.params.fks_to_nodes
        _keep_cols += [f"GP_{i}" for i in self.net.links_df.params.fks_to_nodes]
        _keep_cols += [
            "name",
            "model_link_id",
            "access",
            "drive_access",
            "locationReferences",
        ]

        _optional_cols_to_keep = ["ML_access_point", "ML_egress_point", "ref"]
        for c in _optional_cols_to_keep:
            if c in self.m_links_df.columns:
                _keep_cols.append(c)

        _managed_lanes_df = self.m_links_df.loc[
            self.m_links_df["managed"] == 1, _keep_cols
        ]
        _gp_w_parallel_ml_df = self.m_links_df.loc[
            self.m_links_df["managed"] == -1, _keep_cols
        ]

        #    Gen Purp   |  Managed Lane | name_GP  | name_ML |
        #   GP_A | GP_B |   A  |   B    |
        gp_ml_links_df = _gp_w_parallel_ml_df.merge(
            _managed_lanes_df,
            suffixes=("_GP", "_ML"),
            left_on=self.net.links_df.params.fks_to_nodes,
            right_on=[f"GP_{i}" for i in self.net.links_df.params.fks_to_nodes],
            how="inner",
        )

        # 2 - Create access and egress link dataframes from aligned records
        # if ML_access_point is specified, only have access at those points. Same for egress.
        if "ML_access_point" in gp_ml_links_df.columns:
            access_df = copy.deepcopy(
                gp_ml_links_df.loc[
                    gp_ml_links_df[self.net.links_df.params.from_node]
                    == gp_ml_links_df["ML_access_point"]
                ]
            )
        else:
            access_df = copy.deepcopy(gp_ml_links_df)

        if "ML_egress_point" in gp_ml_links_df.columns:
            egress_df = copy.deepcopy(
                gp_ml_links_df.loc[
                    gp_ml_links_df[self.net.links_df.params.to_node]
                    == gp_ml_links_df["ML_egress_point"]
                ]
            )
        else:
            egress_df = copy.deepcopy(gp_ml_links_df)

        # access link should go from A_GP to A_ML
        access_df[self.net.links_df.params.from_node] = access_df[
            f"{self.net.links_df.params.to_node}_GP"
        ]
        access_df[self.net.links_df.params.to_node] = access_df[
            f"{self.net.links_df.params.from_node}_ML"
        ]
        access_df[self.net.links_df.params.primary_key] = access_df[
            f"{self.net.links_df.params.primary_key}_GP"
        ].apply(self._access_model_link_id)
        access_df[self.net.links_df.params.primary_key + "_idx"] = access_df[
            self.net.links_df.params.primary_key
        ]
        access_df.set_index(self.net.links_df.params.primary_key + "_idx", inplace=True)
        access_df["name"] = "Access Dummy " + access_df["name_GP"]
        access_df["roadway"] = "ml_access"

        # egress link should go from B_ML to B_GP
        egress_df["A"] = egress_df["B_ML"]
        egress_df["B"] = egress_df["B_GP"]
        egress_df["model_link_id"] = egress_df["model_link_id_GP"].apply(
            self._egress_model_link_id
        )
        egress_df[self.net.links_df.params.primary_key + "_idx"] = egress_df[
            self.net.links_df.params.primary_key
        ]
        egress_df.set_index(self.net.links_df.params.primary_key + "_idx", inplace=True)
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

        _keep_ae_cols = self.net.links_df.params.fks_to_nodes
        _keep_ae_cols += [self.net.links_df.params.primary_key]
        _keep_ae_cols = [
            "name",
            "roadway",
            "lanes",
            "access",
            "drive_access",
            "ref",
        ]
        _keep_ae_cols = [c for c in _keep_ae_cols if c in access_egress_df.columns]
        access_egress_df = access_egress_df[_keep_ae_cols]

        # 4 - Create various geometry fields from A and B nodes
        WranglerLogger.debug(f"m_nodes_df length: {len(self.m_nodes_df)}")
        # LocationReferences
        access_egress_df["locationReferences"] = access_egress_df.apply(
            lambda x: location_reference_from_nodes(
                [
                    self.m_nodes_df[
                        self.m_nodes_df[self.net.nodes_df.params.primary_key]
                        == x[self.net.links_df.params.from_node]
                    ].squeeze(),
                    self.m_nodes_df[
                        self.m_nodes_df[self.net.nodes_df.params.primary_ke]
                        == x[self.net.links_df.params.to_node]
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
        access_egress_df[self.net.links_df.params.fk_to_shape] = access_egress_df[
            "geometry"
        ].apply(lambda x: create_unique_shape_id(x))

        WranglerLogger.debug(
            f"Returning {len(access_egress_df)} access and egress links."
        )

        return access_egress_df


def basic_compare_networks(
    net_1, net_2, net_1_name: str = "Network 1", net_2_name: str = "Network 2"
):
    """Basic comparison of values of links and nodes between two networks.

    args:
        net_1: First RoadwayNetwork or ModelRoadwayNetwork object
        net_2: Second RoadwayNetwork or ModelRoadwayNetwork object
        net_1_name: Name of first network. Defaults to `Network 1`.
        net_2_name: Name of second network. Defaults to `Network 2`.
    """
    _col1len = min(14, len(net_1_name) + 2)
    _col2len = min(14, len(net_2_name) + 2)

    def _links_nodes_shapes_from_net(net):
        if isinstance(net, ModelRoadwayNetwork):
            links = net.m_links_df
            nodes = net.m_nodes_df
            shapes = None
        else:
            links = net.links_df
            nodes = net.nodes_df
            shapes = net.shapes_df
        return links, nodes, shapes

    links_1, nodes_1, shapes_1 = _links_nodes_shapes_from_net(net_1)
    links_2, nodes_2, shapes_2 = _links_nodes_shapes_from_net(net_2)

    t = "\nProperty |  {net_1_name:_col1len} | {net_2_name:_col2len}\n"
    t += "=" * 10 + _col1len + _col2len + "\n"
    t += f"links:   | {len(links_1):_col1len} | {len(links_2):_col2len}\n"
    t += f"nodes:   | {len(nodes_1):_col1len} | {len(links_2):_col2len}\n"

    if shapes_1 is not None and shapes_2 is not None:
        t += f"shapes:  | {len(shapes_1):_col1len} | {len(shapes_2):_col2len}\n"

    return t

"""Functions to create a model roadway network from a roadway network."""

from __future__ import annotations

from pathlib import Path
import copy
from typing import Tuple, Union, TYPE_CHECKING

import geopandas as gpd
import pandas as pd

from pandera.typing import DataFrame

from ..logger import WranglerLogger
from ..params import (
    COPY_FROM_GP_TO_ML,
    COPY_TO_ACCESS_EGRESS,
    MANAGED_LANES_LINK_ID_SCALAR,
    MANAGED_LANES_NODE_ID_SCALAR,
)
from ..models.roadway.tables import RoadNodesTable, RoadLinksTable, RoadShapesTable
from .links.edit import _initialize_links_as_managed_lanes
from .links.create import data_to_links_df
from .links.filters import (
    filter_links_to_ml_access_points,
    filter_links_to_ml_egress_points,
    filter_link_properties_managed_lanes,
)
from .nodes.create import _create_nodes_from_link
from .io import write_roadway
from .utils import compare_networks, compare_links

if TYPE_CHECKING:
    from .network import RoadwayNetwork


class ModelRoadwayNetwork:
    """Roadway Network Object compatible with travel modeling.

    Compatability includes:
    (1) separation of managed lane facilities and their connection to general purpose lanes
        using dummy links.

    Attr:
        net: associated RoadwayNetwork object
        links_df: dataframe of model-compatible links
        nodes_df: dataframe of model-compatible nodes
        ml_link_id_scalar (_type_, optional):  scalar value added to the general
            purpose lanes' `model_link_id` when creating an associated link for a parallel
            managed lane. Defaults to MANAGED_LANES_LINK_ID_SCALAR which defaults to 100,000.
        ml_node_id_scalar (_type_, optional): scalar value added to the general
            purpose lanes' `model_node_id` when creating an associated node for a parallel
            managed lane. Defaults to MANAGED_LANES_NODE_ID_SCALAR which defaults to
            500,000
        _net_hash: hash of the the input links and nodes in order to detect changes.

    """

    def __init__(
        self,
        net,
        ml_link_id_scalar=MANAGED_LANES_LINK_ID_SCALAR,
        ml_node_id_scalar=MANAGED_LANES_NODE_ID_SCALAR,
    ):
        """Constructor for ModelRoadwayNetwork.

        NOTE: in order to be associated with the RoadwayNetwork, this should be called from
        RoadwayNetwork.model_net which will lazily construct it.

        Args:
            net (_type_): Associated roadway network.
            ml_link_id_scalar (_type_, optional):  scalar value added to the general
                purpose lanes' `model_link_id` when creating an associated link for a parallel
                managed lane. Defaults to MANAGED_LANES_LINK_ID_SCALAR which defaults to 100,000.
            ml_node_id_scalar (_type_, optional): scalar value added to the general
                purpose lanes' `model_node_id` when creating an associated node for a parallel
                managed lane. Defaults to MANAGED_LANES_NODE_ID_SCALAR which defaults to
                500,000
        """
        self.net = net
        self.ml_link_id_scalar = ml_link_id_scalar
        self.ml_node_id_scalar = ml_node_id_scalar

        self.links_df, self.nodes_df = model_links_nodes_from_net(
            self.net, ml_link_id_scalar, ml_node_id_scalar
        )
        self._net_hash = copy.deepcopy(net.network_hash)

    @property
    def shapes_df(self) -> DataFrame[RoadShapesTable]:
        """Shapes dataframe."""
        return self.net.shapes_df

    @property
    def ml_links_df(self) -> pd.DataFrame:
        """Managed lanes links."""
        return self.links_df.of_type.managed

    @property
    def gp_links_df(self) -> pd.DataFrame:
        """GP lanes on links that have managed lanes next to them."""
        return self.links_df.of_type.parallel_general_purpose

    @property
    def dummy_links_df(self) -> pd.DataFrame:
        """GP lanes on links that have managed lanes next to them."""
        return self.links_df.of_type.dummy

    @property
    def summary(self) -> dict:
        """Quick summary dictionary of number of links, nodes."""
        d = {"links": len(self.links_df), "nodes": len(self.nodes_df)}
        return d

    @property
    def compare_links_df(self) -> pd.DataFrame:
        """Comparison of the original network and the model network."""
        return compare_links([self.net.links_df, self.links_df], names=["Roadway", "ModelRoadway"])

    @property
    def compare_net_df(self) -> pd.DataFrame:
        """Comparison of the original network and the model network."""
        return compare_networks([self.net, self], names=["Roadway", "ModelRoadway"])

    def _node_id_to_managed_lane_node_id(self, model_node_id):
        return self.ml_node_id_scalar + model_node_id

    def _get_managed_lane_node_ids(self, nodes_list):
        return [self._node_id_to_managed_lane_node_id(x) for x in nodes_list]

    def _link_id_to_managed_lane_link_id(self, model_link_id):
        return self.ml_link_id_scalar + model_link_id

    def _access_model_link_id(self, model_link_id):
        return 1 + model_link_id + self.ml_link_id_scalar

    def _egress_model_link_id(self, model_link_id):
        return 2 + model_link_id + self.ml_link_id_scalar

    def write(
        self,
        out_dir: Union[Path, str] = ".",
        prefix: str = "",
        file_format: str = "geojson",
        overwrite: bool = True,
        true_shape: bool = False,
    ) -> None:
        """Writes a network in the roadway network standard.

        Args:
            out_dir: the path were the output will be saved
            prefix: the name prefix of the roadway files that will be generated
            file_format: the format of the output files. Defaults to "geojson"
            overwrite: if True, will overwrite the files if they already exist. Defaults to True
            true_shape: if True, will write the true shape of the links as found from shapes.
                Defaults to False
        """
        write_roadway(self, out_dir, prefix, file_format, overwrite, true_shape)


def model_links_nodes_from_net(
    net: "RoadwayNetwork", ml_link_id_scalar: int, ml_node_id_scalar: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create a roadway network with managed lanes links separated out.

    Add new parallel managed lane links, access/egress links,
    and add shapes corresponding to the new links

    Args:
        net: RoadwayNetwork instance
        ml_link_id_scalar: scalar value added to the general purpose lanes'
            `model_link_id` when creating an associated link for a parallel managed lane
        ml_node_id_scalar: scalar value added to the general purpose lanes'
            `model_node_id` when creating an associated node for a parallel managed lane

    returns: tuple of links and nodes dataframes with managed lanes separated out
    """
    WranglerLogger.info("Separating managed lane links from general purpose links")

    _m_links_df = _separate_ml_links(net.links_df, ml_link_id_scalar, ml_node_id_scalar)
    _m_nodes_df = _create_ml_nodes_from_links(_m_links_df, ml_node_id_scalar)
    m_nodes_df = pd.concat([net.nodes_df, _m_nodes_df])

    _access_egress_links_df = _create_dummy_connector_links(
        net.links_df, m_nodes_df, ml_link_id_scalar, ml_node_id_scalar
    )
    m_links_df = pd.concat([_m_links_df, _access_egress_links_df])
    return m_links_df, m_nodes_df


def _separate_ml_links(
    links_df: DataFrame[RoadLinksTable],
    ml_link_id_scalar: int,
    ml_node_id_scalar: int,
) -> gpd.GeoDataFrame:
    """Separate managed lane links from general purpose links."""
    no_ml_links_df = copy.deepcopy(links_df.of_type.general_purpose_no_parallel_managed)
    gp_links_df = _create_parallel_gp_lane_links(links_df)
    ml_links_df = _create_separate_managed_lane_links(
        links_df, ml_link_id_scalar, ml_node_id_scalar
    )
    WranglerLogger.debug(
        f"Separated ML links: \
        \n  no parallel ML: {len(no_ml_links_df)}\
        \n  parallel GP: {len(gp_links_df)}\
        \n  separate ML: {len(ml_links_df)}"
    )

    m_links_df = pd.concat([ml_links_df, gp_links_df, no_ml_links_df])

    return m_links_df


def _create_parallel_gp_lane_links(links_df: DataFrame[RoadLinksTable]) -> pd.DataFrame:
    """Create df with parallel general purpose lane links."""
    ml_properties = filter_link_properties_managed_lanes(links_df)
    keep_c = [c for c in links_df.columns if c not in ml_properties]
    gp_links_df = copy.deepcopy(links_df[keep_c].of_type.managed)
    gp_links_df["managed"] = -1
    return gp_links_df


def _create_separate_managed_lane_links(
    links_df: DataFrame[RoadLinksTable],
    ml_link_id_scalar: int,
    ml_node_id_scalar: int,
) -> Tuple[gpd.GeoDataFrame]:
    """Creates df with separate links for managed lanes."""
    # make sure there are correct fields even if managed = 1 was set outside of wrangler
    links_df = _initialize_links_as_managed_lanes(links_df, links_df.of_type.managed.index.values)

    # columns to copy from GP to ML
    copy_cols = [c for c in COPY_FROM_GP_TO_ML if c in links_df.columns]

    # columns to keep in order to use to create ML versions of them
    keep_for_calcs_cols = [
        "A",
        "B",
        "model_link_id",
        "name",
        "managed",
        "ML_access_point",
        "ML_egress_point",
    ]

    # columns with specific ML values
    ml_properties = filter_link_properties_managed_lanes(links_df)

    keep_cols = list(set(copy_cols + keep_for_calcs_cols + ml_properties))

    ml_links_df = copy.deepcopy(links_df[keep_cols].of_type.managed)

    # add special properties for managed lanes
    ml_links_df["name"] = "Managed Lane " + ml_links_df["name"]
    ml_links_df["model_link_id"] = ml_links_df["model_link_id"] + ml_link_id_scalar
    ml_links_df["model_link_id_idx"] = ml_links_df["model_link_id"]
    # WranglerLogger.debug(f"ml_links_df.1:\n{ml_links_df.iloc[0]}")

    # rename ML_ properties to the `main` properties for managed lane links
    ml_links_df = ml_links_df.rename(
        columns=dict(zip(ml_properties, strip_ML_from_prop_list(ml_properties)))
    )
    # WranglerLogger.debug(f"ml_links_df.2:\n{ml_links_df.iloc[0]}")
    ml_links_df[["GP_A", "GP_B"]] = ml_links_df[["A", "B"]]
    ml_links_df[["A", "B"]] = ml_links_df[["GP_A", "GP_B"]] + ml_node_id_scalar

    ml_links_df.set_index("model_link_id_idx", inplace=True)
    ml_links_df = RoadLinksTable.validate(ml_links_df, lazy=True)
    # WranglerLogger.debug(f"ml_links_df.3:\n{ml_links_df.iloc[0]}")
    return ml_links_df


def _create_dummy_connector_links(
    links_df: DataFrame[RoadLinksTable],
    m_nodes_df: DataFrame[RoadNodesTable],
    ml_link_id_scalar: int,
    ml_node_id_scalar: int,
) -> DataFrame[RoadLinksTable]:
    """Create dummy connector links between the general purpose and managed lanes.

    Args:
        links_df: RoadLinksTable of network links
        m_nodes_df: GeoDataFrame of model nodes
        ml_link_id_scalar: scalar value added to the general purpose lanes'
            `model_link_id` when creating an associated link for a parallel managed lane
        ml_node_id_scalar: scalar value added to the general purpose lanes'
            `model_node_id` when creating an associated node for a parallel managed lane

    returns: GeoDataFrame of access and egress dummy connector links to add to m_links_df
    """
    WranglerLogger.debug("Creating access and egress dummy connector links")
    # 1. Align the managed lane and associated general purpose lanes in the same records
    copy_cols = [c for c in COPY_TO_ACCESS_EGRESS if c in links_df.columns]

    keep_for_calcs_cols = [
        "A",
        "B",
        "model_link_id",
        "name",
        "ML_access_point",
        "ML_egress_point",
    ]
    keep_cols = list(set(copy_cols + keep_for_calcs_cols))

    # 2 - Create access and egress link dataframes from aligned records
    # if ML_access_point is specified, only have access at those points. Same for egress.
    access_df = copy.deepcopy(filter_links_to_ml_access_points(links_df)[keep_cols])
    egress_df = copy.deepcopy(filter_links_to_ml_egress_points(links_df)[keep_cols])

    # access link should go from A_GP to A_ML
    access_df["B"] = access_df["A"] + ml_node_id_scalar
    access_df["model_link_id"] = 1 + access_df["model_link_id"] + ml_link_id_scalar
    access_df["name"] = "Access Dummy " + access_df["name"]
    access_df["roadway"] = "ml_access_point"
    access_df["model_link_id_idx"] = access_df["model_link_id"]
    access_df.set_index("model_link_id_idx", inplace=True)

    # egress link should go from B_ML to B_GP
    egress_df["A"] = egress_df["A"] + ml_node_id_scalar
    egress_df["model_link_id"] = 2 + egress_df["model_link_id"] + ml_link_id_scalar
    egress_df["name"] = "Egress Dummy " + egress_df["name"]
    egress_df["roadway"] = "ml_egress_point"
    egress_df["model_link_id_idx"] = egress_df["model_link_id"]
    egress_df.set_index("model_link_id_idx", inplace=True)

    # combine to one dataframe
    access_egress_df = pd.concat([access_df, egress_df])

    # 3 - Determine property values
    access_egress_df["lanes"] = 1
    access_egress_df = access_egress_df.rename(
        columns=dict(zip(copy_cols, strip_ML_from_prop_list(copy_cols)))
    )

    # 5 - Add geometry
    access_egress_df = data_to_links_df(access_egress_df, nodes_df=m_nodes_df)
    WranglerLogger.debug(f"access_egress_df['geometry']: \n {access_egress_df['geometry']}")

    WranglerLogger.debug(f"Access Egress Links Created: {len(access_egress_df)}")

    return access_egress_df


def _create_ml_nodes_from_links(
    ml_links_df, ml_node_id_scalar: int = MANAGED_LANES_NODE_ID_SCALAR
) -> DataFrame[RoadNodesTable]:
    """Creates managed lane nodes from geometry already generated by links."""
    a_nodes_df = _create_nodes_from_link(ml_links_df.of_type.managed, 0, "A")
    b_nodes_df = _create_nodes_from_link(ml_links_df.of_type.managed, -1, "B")
    ml_nodes_df = a_nodes_df.combine_first(b_nodes_df)

    ml_nodes_df["GP_model_node_id"] = ml_nodes_df["model_node_id"] - ml_node_id_scalar
    return ml_nodes_df


def strip_ML_from_prop_list(property_list: list[str]) -> list[str]:
    """Strips 'ML_' from property list but keeps necessary access/egress point cols."""
    keep_same = ["ML_access_point", "ML_egress_point"]
    pl = [p.removeprefix("ML_") if p not in keep_same else p for p in property_list]
    pl = [p.replace("_ML_", "_") if p not in keep_same else p for p in pl]
    return pl

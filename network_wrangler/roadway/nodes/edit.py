"""Edits RoadNodesTable properties.

NOTE: Each public method will return a new, whole copy of the RoadNodesTable with associated edits.
Private methods may return mutated originals.
"""

import geopandas as gpd
from typing import Union

from pydantic import validate_call
from pandera.typing import DataFrame

from ...logger import WranglerLogger
from ...utils.data import validate_existing_value_in_df, update_df_by_col_value
from ...models._base.validate import validate_df_to_model
from ...models.roadway.tables import RoadNodesTable
from ...models.projects.roadway_property_change import NodeGeometryChangeTable, RoadPropertyChange
from ...params import LAT_LON_CRS


class NodeChangeError(Exception):
    """Raised when there is an issue with applying a node change."""

    pass


@validate_call(config=dict(arbitrary_types_allowed=True))
def edit_node_geometry(
    nodes_df: DataFrame[RoadNodesTable],
    node_geometry_change_table: DataFrame[NodeGeometryChangeTable],
) -> DataFrame[RoadNodesTable]:
    """Returns copied nodes table with geometry edited.

    Should be called from network so that accompanying links and shapes are also updated.

    Args:
        nodes_df: RoadNodesTable to edit
        node_geometry_change_table: NodeGeometryChangeTable with geometry changes

    """
    WranglerLogger.debug(f"Updating node geometry for {len(node_geometry_change_table)} nodes.")
    WranglerLogger.debug(f"Original nodes_df: \n{nodes_df.head()}")
    # for now, require in_crs is the same for whole column
    if node_geometry_change_table.in_crs.nunique() != 1:
        WranglerLogger.error(
            "in_crs must be the same for all nodes. \
                             Got: {node_geometry_change_table.in_crs}"
        )
        raise (NodeChangeError("in_crs must be the same for all nodes."))

    in_crs = node_geometry_change_table.loc[0, "in_crs"]

    # Create a table with all the new node geometry
    geo_s = gpd.points_from_xy(node_geometry_change_table.X, node_geometry_change_table.Y)
    geo_df = gpd.GeoDataFrame(node_geometry_change_table, geometry=geo_s, crs=in_crs)
    geo_df = geo_df.to_crs(LAT_LON_CRS)
    WranglerLogger.debug(f"Updated geometry geo_df: \n{geo_df}")

    # Update the nodes table with the new geometry
    nodes_df = update_df_by_col_value(
        nodes_df, geo_df, "model_node_id", properties=["X", "Y", "geometry"]
    )
    nodes_df = validate_df_to_model(nodes_df, RoadNodesTable)

    WranglerLogger.debug(f"Updated nodes_df: \n{nodes_df.head()}")

    return nodes_df


def edit_node_property(
    nodes_df: DataFrame[RoadNodesTable],
    node_idx: list[int],
    prop_name: str,
    prop_change: Union[dict, RoadPropertyChange],
    existing_value_conflict_error: bool = False,
    _geometry_ok: bool = False,
) -> DataFrame[RoadNodesTable]:
    """Return copied nodes table with node property edited.

    Args:
        nodes_df: RoadNodesTable to edit
        node_idx: list of node indices to change
        prop_name: property name to change
        prop_change: dictionary of value from project_card
        existing_value_conflict_error: If True, will trigger an error if the existing
            specified value in the project card doesn't match the value in nodes_df.
            Otherwise, will only trigger a warning. Defaults to False.
        _geometry_ok: if False, will not let you change geometry-related fields. Should
            only be changed to True by internal processes that know that geometry is changing
            and will update it in appropriate places in network. Defaults to False.
            GENERALLY DO NOT TURN THIS ON.
    """
    if not isinstance(prop_change, RoadPropertyChange):
        prop_change = RoadPropertyChange(**prop_change)
    prop_dict = prop_change.model_dump(exclude_none=True, by_alias=True)

    # Should not be used to update node geometry fields unless explicity set to OK:
    if prop_name in nodes_df.params.geometry_props and not _geometry_ok:
        raise NodeChangeError("Cannot unilaterally change geometry property.")

    # check existing if necessary
    if "existing" in prop_dict:
        exist_ok = validate_existing_value_in_df(
            nodes_df, node_idx, prop_name, prop_dict["existing"]
        )
        if not exist_ok and existing_value_conflict_error:
            raise NodeChangeError("Conflict between specified existing and actual existing values")

    nodes_df = nodes_df.copy()

    # if it is a new attribute then initialize with NaN values
    if prop_name not in nodes_df:
        nodes_df[prop_name] = None

    sel_nodes_df = nodes_df.loc[node_idx]

    # `set` and `change` just affect the simple property
    if "set" in prop_dict:
        sel_nodes_df[prop_name] = prop_dict["set"]
    elif "change" in prop_dict:
        sel_nodes_df[prop_name] = sel_nodes_df.loc[prop_name].apply(
            lambda x: x + prop_dict["change"]
        )
    else:
        raise NodeChangeError("Couldn't find correct node change spec in: {prop_dict}")
    nodes_df = validate_df_to_model(nodes_df, RoadNodesTable)
    return nodes_df

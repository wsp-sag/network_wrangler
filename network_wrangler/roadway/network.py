#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import copy
import hashlib

from collections import defaultdict
from typing import Collection, List, Union

import geopandas as gpd
import networkx as nx
import pandas as pd

from geopandas.geodataframe import GeoDataFrame
from pandas.core.frame import DataFrame
from pandera import check_input
from projectcard import ProjectCard, SubProject

from .projects import (
    apply_new_roadway,
    apply_calculated_roadway,
    apply_parallel_managed_lanes,
    apply_roadway_deletion,
    apply_roadway_property_change,
)
from ..logger import WranglerLogger
from ..utils import (
    point_from_xy,
    get_point_geometry_from_linestring,
    update_nodes_in_linestring_geometry,
)
from ..utils.time import parse_timespans_to_secs
from .model_roadway import ModelRoadwayNetwork
from .links import LinksSchema
from .nodes import NodesSchema
from .shapes import read_shapes, ShapesParams, df_to_shapes_df
from .selection import RoadwaySelection


class RoadwayNetwork(object):
    """
    Representation of a Roadway Network.

    Typical usage example:

    ```py
    net = load_roadway(
        links_file=MY_LINK_FILE,
        nodes_file=MY_NODE_FILE,
        shapes_file=MY_SHAPE_FILE,
    )
    my_selection = {
        "link": [{"name": ["I 35E"]}],
        "A": {"osm_node_id": "961117623"},  # start searching for segments at A
        "B": {"osm_node_id": "2564047368"},
    }
    net.get_selection(my_selection)

    my_change = [
        {
            'property': 'lanes',
            'existing': 1,
            'set': 2,
        },
        {
            'property': 'drive_access',
            'set': 0,
        },
    ]

    my_net.apply_roadway_feature_change(
        my_net.get_selection(my_selection),
        my_change
    )

        net.model_net
        net.is_network_connected(mode="drive", nodes=self.m_nodes_df, links=self.m_links_df)
        _, disconnected_nodes = net.assess_connectivity(
            mode="walk",
            ignore_end_nodes=True,
            nodes=self.m_nodes_df,
            links=self.m_links_df
        )
        write_roadway(net,filename=my_out_prefix, path=my_dir, for_model = True)
    ```

    Attributes:
        nodes_df (GeoDataFrame): dataframe of of node records. Contains
            `NodesParams` dataclass which is mapped to the `.params` attribute of the dataframe.
            primary key: `.params.primary_key`

        links_df (GeoDataFrame): dataframe of link records and associated properties. Contains
            `LinksParams` dataclass which is mapped to the `.params` attribute of the dataframe.
            primary key: `.params.primary_key`
            foreign key to from node: `.params.from_node`
            foreign key to to node: `.params.to_node`
            foreign key to shapes: `.params.fk_to_shape`


        shapes_df (GeoDataFrame): data from of detailed shape records. Contains
            `ShapesParams` dataclass which is mapped to the `.params` attribute of the dataframe.
            primary key: `.params.primary_key`.  This is lazily created iff it is called because
            shapes files can be expensive to read.

        selections (dict): dictionary of stored `RoadwaySelection` objects, mapped by
            `RoadwaySelection.sel_key` in case they are made repeatedly.

        crs (str): coordinate reference system in ESPG number format. Defaults to DEFAUULT_CRS
            which is set to 4326, WGS 84 Lat/Long

        network_hash: dynamic property of the hashed value of links_df and nodes_df. Used for
            quickly identifying if a network has changed since various expensive operations have
            taken place (i.e. generating a ModelRoadwayNetwork or a network graph)

        model_net (ModelRoadwayNetwork): referenced `ModelRoadwayNetwork` object which will be
            lazily created if None or if the `network_hash` has changed.

        num_managed_lane_links (int): dynamic property number of managed lane links.
    """

    def __init__(
        self,
        links_df: GeoDataFrame,
        nodes_df: GeoDataFrame,
        shapes_df: GeoDataFrame = None,
        shapes_file: str = None,
        shapes_params: ShapesParams = None,
    ):
        """
        Constructor for RoadwayNetwork object.

        Args:
            nodes_df: GeoDataFrame of of node records.
            links_df: GeoDataFrame of of link records.
            shapes_df: GeoDataFrame of detailed shape records
            shapes_file: path to shapes file to lazily read it in when needed. Defaults to None.
            shapes_params: ShapesParams instance. Defaults to default initialization of
                ShapesParams.
            crs: coordinate reference system in ESPG number format. Defaults to DEFAULT_CRS
                which is set to 4326, WGS 84 Lat/Long

        """
        if links_df.crs != nodes_df.crs:
            WranglerLogger.error(
                f"CRS of links_df ({links_df.crs}) and nodes_df ({nodes_df.crs}) don't match."
            )
            raise ValueError("CRS of links_df and nodes_df don't match.")
        self.crs = links_df.crs
        self.nodes_df = nodes_df
        self.links_df = links_df

        self._links_file = ""
        self._nodes_file = ""

        if shapes_df is None and shapes_file is None:
            WranglerLogger.warning("No shapes associated with network!")
            raise ValueError("Should specify either shapes or shapes_file.")

        self._shapes_df = shapes_df
        self._shapes_file = shapes_file
        if shapes_df is None:
            if shapes_params is None:
                shapes_params = ShapesParams()
            self._shapes_params = shapes_params

        # Model network
        self._model_net = None

        # cached selections
        self._selections = {}

        # cached modal graphs of full network
        self._modal_graphs = defaultdict(lambda: {"graph": None, "hash": None})

    @property
    def shapes_df(self):
        if self._shapes_df is None:
            self._shapes_df = read_shapes(
                self._shapes_file, crs=self.crs, shapes_params=self._shapes_params
            )
        return self._shapes_df

    @shapes_df.setter
    def shapes_df(self, value):
        self._shapes_df = df_to_shapes_df(value, shapes_params=self._shapes_params)

    @property
    def network_hash(self):
        _value = str.encode(self.links_df.df_hash() + "-" + self.nodes_df.df_hash())

        _hash = hashlib.sha256(_value).hexdigest()
        return _hash

    @property
    def model_net(self):
        if self._model_net is None or self._model_net._net_hash != self.network_hash:
            self._model_net = ModelRoadwayNetwork(self)
        return self._model_net

    @property
    def num_managed_lane_links(self):
        if "managed" in self.links_df.columns:
            return len((self.links_df[self.links_df["managed"] == 1]).index)
        else:
            return 0

    @property
    def summary(self) -> dict:
        """Quick summary dictionary of number of links, nodes"""
        d = {
            "links": len(self.links_df),
            "nodes": len(self.nodes_df),
        }
        return d

    @property
    def link_shapes_df(self) -> gpd.GeoDataFrame:
        """
        Add shape geometry to liks if available

        returns: shapes merged to nodes dataframe
        """

        _links_df = copy.deepcopy(self.links_df)
        link_shapes_df = _links_df.merge(
            self.shapes_df,
            left_on=self.links_df.params.fk_to_shape,
            right_on=self.shapes_df.params.primary_key,
            how="left",
        )
        return link_shapes_df

    def get_selection(
        self,
        selection_dict: dict,
        overwrite: bool = False,
        ignore_missing: bool = True,
    ) -> RoadwaySelection:
        """Return selection if it already exists, otherwise performs selection.

        Will raise an error if no links or nodes found.

        Args:
            selection_dict (dict): _description_
            overwrite: if True, will overwrite any previously cached searches. Defaults to False.
            ignore_missing: if True, will error if explicit ID is selected but not found.

        Returns:
            Selection: Selection object
        """
        key = RoadwaySelection._assign_selection_key(selection_dict)
        WranglerLogger.debug(f"Getting selection from key: {key}")
        if (key not in self._selections) or overwrite:
            WranglerLogger.debug(f"Performing selection from key: {key}")
            self._selections[key] = RoadwaySelection(
                self, selection_dict, ignore_missing=ignore_missing
            )
        else:
            WranglerLogger.debug(f"Using cached selection from key: {key}")

        if not self._selections[key]:
            WranglerLogger.debug(
                f"No links or nodes found for selection dict:\n {selection_dict}"
            )
            raise ValueError("Selection not successful.")
        return self._selections[key]

    def modal_graph_hash(self, mode):
        """Hash of the links in order to detect a network change from when graph created."""
        _value = str.encode(self.links_df.df_hash() + "-" + mode)
        _hash = hashlib.sha256(_value).hexdigest()

        return _hash

    def get_modal_graph(self, mode):
        from .graph import net_to_graph

        if self._modal_graphs[mode]["hash"] != self.modal_graph_hash(mode):
            self._modal_graphs[mode]["graph"] = net_to_graph(self, mode)

        return self._modal_graphs[mode]["graph"]

    def validate_properties(
        self,
        df: pd.DataFrame,
        properties: dict,
        ignore_existing: bool = False,
        require_existing_for_change: bool = False,
    ) -> bool:
        """
        If there are change or existing commands, make sure that that
        property exists in the network.

        Args:
            properties : properties dictionary to be evaluated
            df: links_df or nodes_df or shapes_df to check for compatibility with
            ignore_existing: If True, will only warn about properties
                that specify an "existing" value.  If False, will fail.
            require_existing_for_change: If True, will fail if there isn't
                a specified value in theproject card for existing when a
                change is specified.

        Returns: boolean value as to whether the properties dictonary is valid.
        """

        valid = True
        for p_name, p in properties.items():
            if p_name not in df.columns and p.get("change"):
                WranglerLogger.error(
                    f'"Change" is specified for attribute {p_name}, but doesn\'t \
                            exist in base network'
                )
                valid = False
            if p_name not in df.columns and p.get("existing") and not ignore_existing:
                WranglerLogger.error(
                    f'"Existing" is specified for attribute {p_name}, but doesn\'t \
                        exist in base network'
                )
                valid = False
            if p.get("change") and not p.get("existing"):
                if require_existing_for_change:
                    WranglerLogger.error(
                        f'"Change" is specified for attribute {p_name}, but there \
                            isn\'t a value for existing.\nTo proceed, run with the setting \
                            require_existing_for_change=False'
                    )
                    valid = False
                else:
                    WranglerLogger.warning(
                        f'"Change" is specified for attribute {p_name}, but there \
                            isn\'t a value for existing'
                    )

        if not valid:
            raise ValueError("Property changes are not valid:\n  {properties")

    def apply(self, project_card: Union[ProjectCard, dict]) -> "RoadwayNetwork":
        """
        Wrapper method to apply a roadway project, returning a new RoadwayNetwork instance.

        Args:
            project_card: either a dictionary of the project card object or ProjectCard instance
        """

        if not (
            isinstance(project_card, ProjectCard)
            or isinstance(project_card, SubProject)
        ):
            project_card = ProjectCard(project_card)

        project_card.validate()

        if project_card.sub_projects:
            for sp in project_card.sub_projects:
                WranglerLogger.debug(f"- applying subproject: {sp.change_type}")
                self._apply_change(sp)
            return self
        else:
            return self._apply_change(project_card)

    def _apply_change(self, change: Union[ProjectCard, SubProject]) -> "RoadwayNetwork":
        """Apply a single change: a single-project project or a sub-project."""
        if not isinstance(change, SubProject):
            WranglerLogger.info(
                f"Applying Project to Roadway Network: {change.project}"
            )

        if change.change_type == "roadway_property_change":
            return apply_roadway_property_change(
                self,
                self.get_selection(change.facility),
                change.roadway_property_change["property_changes"],
            )

        elif change.change_type == "roadway_managed_lanes":
            return apply_parallel_managed_lanes(
                self,
                self.get_selection(change.facility),
                change.roadway_managed_lanes["property_changes"],
            )

        elif change.change_type == "roadway_addition":
            return apply_new_roadway(
                self,
                change.roadway_addition,
            )

        elif change.change_type == "roadway_deletion":
            return apply_roadway_deletion(
                self,
                change.roadway_deletion,
            )

        elif change.change_type == "pycode":
            return apply_calculated_roadway(self, change.pycode)
        else:
            WranglerLogger.error(f"Couldn't find project in:\n{change.__dict__}")
            raise (ValueError("Invalid Project Card Category: {change.change_type}"))

    def update_network_geometry_from_node_xy(
        self, updated_nodes: List = None
    ) -> gpd.GeoDataFrame:
        """Adds or updates the geometry of the nodes in the network based on XY coordinates.

        Assumes XY are in self.crs.
        Also updates the geometry of links and shapes that reference these nodes.

        Args:
            updated_nodes: List of node_ids to update. Defaults to all nodes.

        Returns:
           gpd.GeoDataFrame: nodes geodataframe with updated geometry.
        """
        if updated_nodes:
            updated_nodes_df = self.nodes_df.loc[updated_nodes]
        else:
            updated_nodes_df = self.nodes_df

        if len(updated_nodes_df) < 5:
            WranglerLogger.debug(
                f"Original Nodes:\n{updated_nodes_df[['X','Y','geometry']]}"
            )

        updated_nodes_df["geometry"] = updated_nodes_df.apply(
            lambda x: point_from_xy(
                x["X"],
                x["Y"],
                xy_crs=updated_nodes_df.crs,
                point_crs=updated_nodes_df.crs,
            ),
            axis=1,
        )

        if len(updated_nodes_df) < 5:
            WranglerLogger.debug(
                f"Updated Nodes:\n{updated_nodes_df[['X','Y','geometry']]}"
            )

        self.nodes_df.update(
            updated_nodes_df[[updated_nodes_df.params.primary_key, "geometry"]]
        )

        if len(self.nodes_df) < 5:
            WranglerLogger.debug(
                f"Updated self.nodes_df:\n{self.nodes_df[['X','Y','geometry']]}"
            )

        self._update_node_geometry_in_links(updated_nodes_df.index.values.tolist())
        self._update_node_geometry_in_shapes(updated_nodes_df.index.values.tolist())

    def nodes_in_links(
        self,
        links_df: pd.DataFrame,
        nodes_df: pd.DataFrame = None,
    ) -> pd.DataFrame:
        """Filters dataframe for nodes that are in links

        Args:
            links_df: DataFrame of standard network links to search for nodes for.
            nodes_df: DataFrame of standard network nodes. Optional. If not provided will use
                self.nodes_df.

        """
        if nodes_df is None:
            nodes_df = self.nodes_df
        _node_ids = self.node_ids_in_links(links_df)
        nodes_in_links = nodes_df.loc[nodes_df.index.isin(_node_ids)]
        WranglerLogger.debug(
            f"Selected {len(nodes_in_links)} of {len(nodes_df)} nodes."
        )
        return nodes_in_links

    @check_input(LinksSchema, inplace=True)
    def add_links(self, add_links_df: gpd.GeoDataFrame):
        """Validate combined links_df with LinksSchema before adding to self.links_df

        Args:
            add_links_df (gpd.GeoDataFrame): Dataframe of additional links to add.
        """

        self.links_df = LinksSchema(pd.concat([self.links_df, add_links_df]))

    @check_input(NodesSchema, inplace=True)
    def add_nodes(self, add_nodes_df: gpd.GeoDataFrame):
        """Validate combined nodes_df with NodesSchema before adding to self.nodes_df

        Args:
            add_nodes_df (gpd.GeoDataFrame): Dataframe of additional nodes to add.
        """

        self.nodes_df = NodesSchema(pd.concat([self.nodes_df, add_nodes_df]))

    def delete_links(
        self,
        selection_dict: dict = None,
        link_ids: List = [],
        ignore_missing: bool = True,
        clean_nodes: bool = True,
        clean_shapes: bool = False,
    ):
        if selection_dict is not None:
            selection = self.get_selection(
                {"links": selection_dict, "modes": ["any"]},
                ignore_missing=ignore_missing,
            )
            link_ids += selection.selected_links

        _missing = set(link_ids) - set(self.links_df.index)
        if _missing:
            WranglerLogger.warning(
                f"Links in network not there to delete: \n {_missing}"
            )
            if not ignore_missing:
                raise NodeDeletionError("Links to delete are not in network.")

        link_ids = list(set(self.links_df.index).intersection(link_ids))
        WranglerLogger.debug(f"Dropping links: {link_ids}")

        if clean_nodes:
            _links_to_delete = self.links_df.loc[self.links_df.index.isin(link_ids)]
            _nodes_to_delete = self.nodes_in_links(
                _links_to_delete
            ).index.values.tolist()
            WranglerLogger.debug(
                f"Dropping nodes associated with dropped links: \n\
                {_nodes_to_delete}"
            )
            self.delete_nodes(node_ids=_nodes_to_delete)

        self.links_df = self.links_df.drop(selection.selected_links)

        if clean_shapes:
            self.delete_shapes(all_unused=True)

    def delete_nodes(
        self,
        selection_dict: dict = None,
        node_ids: List = [],
        all_unused: bool = False,
        ignore_missing: bool = True,
    ) -> None:
        """
        Deletes nodes from roadway network.

        Gets a list of nodes to delete by either selecting all unused or a combination of
        the node_ids list or a selection dictionary.
        Makes sure any nodes that are used by links aren't deleted.

        args:
            selection_dict:
            node_ids: list of model_node_ids
            all_unused: If True, will select all unused nodes in network. Defaults to False.
            ignore_missing: If False, will raise  NodeDeletionError if nodes specified for deletion
                aren't foudn in the network. Otherwise it will just be a warning. Defaults to True.

        raises:
            NodeDeletionError: If not ignore_missing and selected nodes to delete aren't in network.
        """

        #
        if all_unused:
            node_ids = self.nodes_without_links
        else:
            if selection_dict is not None:
                selection = self.get_selection(
                    {"nodes": selection_dict}, ignore_missing=ignore_missing
                )
                node_ids += selection.selected_nodes

            # Only delete nodes that don't have attached links
            node_ids = list(set(node_ids).intersection(self.nodes_without_links))

        _missing = set(node_ids) - set(self.nodes_df.index)
        if _missing:
            WranglerLogger.warning(
                f"Nodes in network not there to delete: \n {_missing}"
            )
            if not ignore_missing:
                raise NodeDeletionError("Nodes to delete are not in network.")

        node_ids = list(set(self.nodes_df.index).intersection(node_ids))
        WranglerLogger.debug(f"Dropping nodes: {node_ids}")
        self.nodes_df = self.nodes_df.drop(node_ids)

    def delete_shapes(self, shape_ids: List = [], all_unused: bool = False) -> None:
        if all_unused:
            shape_ids += self._shapes_without_links()
            shape_ids = list(set(shape_ids))

        WranglerLogger.debug(f"{len(shape_ids)} shapes to drop\n{shape_ids}")
        self.shapes_df = self.shapes_df.drop(shape_ids)

    def node_ids_in_links(
        self,
        links_df: pd.DataFrame,
    ) -> Collection:
        """Returns a list of nodes that are contained in the links.

        Args:
            links_df: Links which to return node list for
        """
        if len(links_df) < 25:
            WranglerLogger.debug(
                f"Links:\n{links_df[self.links_df.params.fks_to_nodes]}"
            )
        nodes_list = list(
            set(
                pd.concat(
                    [links_df[c] for c in self.links_df.params.fks_to_nodes]
                ).tolist()
            )
        )
        if len(nodes_list) < 25:
            WranglerLogger.debug(f"_node_list:\n{nodes_list}")
        return nodes_list

    def links_with_nodes(
        self, links_df: pd.DataFrame, node_id_list: list
    ) -> gpd.GeoDataFrame:
        """Returns a links geodataframe which start or end at the nodes in the list.

        Args:
            links_df: dataframe of links to search for nodes in
            node_id_list (list): List of nodes to find links for.  Nodes should be identified
                by the foreign key - the one that is referenced in LINK_FOREIGN_KEY.
        """
        # If nodes are equal to all the nodes in the links, return all the links
        _nodes_in_links = self.nodes_in_links(links_df)
        WranglerLogger.debug(
            f"# Nodes: {len(node_id_list)}\nNodes in links:{len(_nodes_in_links)}"
        )
        if len(set(node_id_list) - set(_nodes_in_links)) == 0:
            return links_df

        WranglerLogger.debug(f"Finding links assocated with {len(node_id_list)} nodes.")
        if len(node_id_list) < 25:
            WranglerLogger.debug(f"node_id_list: {node_id_list}")

        _selected_links_df = links_df[
            links_df.isin({c: node_id_list for c in links_df.params.fks_to_nodes}).any(
                axis=1
            )
        ]
        WranglerLogger.debug(
            f"Temp Selected {len(_selected_links_df)} associated with {len(node_id_list)} nodes."
        )
        if len(_selected_links_df) < 10:
            WranglerLogger.debug(
                f"Temp Sel Links:\n{_selected_links_df[_selected_links_df.params.display_cols]}"
            )
        """TODO
        _query_parts = [
            f"{prop} == {str(n)}"
            for prop in links_df.params.fks_to_node
            for n in node_id_list
        ]

        _query = " or ".join(_query_parts)
        _selected_links_df = links_df.query(_query, engine="python")
        """
        WranglerLogger.debug(
            f"Selected {len(_selected_links_df)} associated with {len(node_id_list)} nodes."
        )

        return _selected_links_df

    def links_in_path(self, links_df: pd.DataFrame, node_id_path_list: list):
        """Return selection of links dataframe with nodes along path defined by node_id_path_list.

        Args:
            links_df (pd.DataFrame): Links dataframe to select from
            node_id_path_list (list): List of node primary keys.
        """
        _ab_pairs = [
            node_id_path_list[i : i + 2] for i, _ in enumerate(node_id_path_list)
        ][:-1]
        _cols = self.links_df.params.fks_to_nodes
        _sel_df = pd.DataFrame(_ab_pairs, columns=_cols)

        WranglerLogger.debug(f"Selecting links that match _sel_df:\n{_sel_df}")
        _sel_links_df = pd.merge(
            links_df.reset_index(names="index"), _sel_df, how="inner"
        )
        # WranglerLogger.debug(f"_sel_links_df:\n{_sel_links_df[_sel_links_df.params.display_cols]}")
        _sel_links_df = _sel_links_df.set_index("index")

        return _sel_links_df

    def _update_node_geometry_in_links(
        self,
        updated_node_ids: list[int],
    ) -> None:
        """Updates the geometry for given links for a given list of nodes

        Should be called by any function that changes a node location.

        Args:
            updated_node_ids: list of node PKs with updated geometry
        """
        _from_field = (self.links_df.params.from_node, 0)
        _to_field = (self.links_df.params.to_node, -1)
        _link_pk = self.links_df.params.primary_key

        for node_fk, linestring_idx in _from_field, _to_field:
            # Update Links
            _link_mask = self.links_df[node_fk].isin(updated_node_ids)
            if not _link_mask.any():
                continue

            _links_df = self.links_df.loc[_link_mask, [_link_pk, node_fk, "geometry"]]
            _links_df = _links_df.rename(
                columns={node_fk: self.nodes_df.params.primary_key}
            )
            self.links_df.loc[
                _link_mask, "geometry"
            ] = update_nodes_in_linestring_geometry(
                _links_df,
                self.nodes_df.loc[updated_node_ids],
                linestring_idx,
            )
            _disp_c = self.links_df.params.fks_to_nodes + ["geometry"]
            WranglerLogger.debug(
                f"Upd link geom::\n{self.links_df.loc[_link_mask,_disp_c]}"
            )

    def _update_node_geometry_in_shapes(
        self,
        updated_node_ids: list[int],
    ) -> None:
        """Updates the geometry for given shapes for a given list of nodes.

        Should be called by any function that changes a node location.

        NOTES:
         - This will mutate the geometry of a shape in place for the start and end node
            ...but not the nodes in-between.  Something to consider...

        Args:
            updated_node_ids: list of node PKs with updated geometry
        """
        _from_field = (self.links_df.params.from_node, 0)
        _to_field = (self.links_df.params.to_node, -1)
        _shape_fk = self.links_df.params.fk_to_shape
        _node_pk = self.nodes_df.params.primary_key
        _shape_pk = self.shapes_df.params.primary_key

        for node_fk, linestring_idx in _from_field, _to_field:
            # Identify links
            _link_mask = self.links_df[node_fk].isin(updated_node_ids)
            if not _link_mask.any():
                continue

            # Update Shapes
            _shape_id_df = self.links_df.loc[_link_mask, [node_fk, _shape_fk]]
            if not len(_shape_id_df):
                continue

            # _shape_id_df: model_node_id, shape_id
            _shape_id_df = _shape_id_df.rename(
                columns={node_fk: _node_pk, _shape_fk: _shape_pk}
            )
            _shape_ids = _shape_id_df[_shape_pk].unique()

            # _shapes_df: shape_id, model_node_id, geometry
            _shapes_df = self.shapes_df.loc[_shape_ids, [_shape_pk, "geometry"]].merge(
                _shape_id_df, left_on=_shape_pk, right_on=_shape_pk, how="left"
            )

            # WranglerLogger.debug(f"_shapes_df:\n{_shapes_df}")

            self.shapes_df.loc[
                _shape_ids, "geometry"
            ] = update_nodes_in_linestring_geometry(
                _shapes_df,  # shape_id, model_node_id, geometry
                self.nodes_df.loc[updated_node_ids],  # node_id: geometry
                linestring_idx,
            )

            _disp_c = [self.shapes_df.params.primary_key, "geometry"]
            WranglerLogger.debug(
                f"Upd shape geom:\n{self.shapes_df.loc[_shape_ids,_disp_c]}"
            )

    def has_node(self, unique_node_id) -> bool:
        """Queries if network has node based on nodes_df.params.primary_key.

        Args:
            unique_node_id (_type_): value of nodes_df.params.primary_key
        """

        has_node = (
            self.nodes_df[self.nodes_df.params.primary_key].isin([unique_node_id]).any()
        )

        return has_node

    def has_link(self, link_key_values: tuple) -> bool:
        """Returns true if network has links with link_key_values in the fields associated with
        self.links_df.params.fks_to_nodes.

        Args:
            link_key_values: Tuple of values corresponding with
                RoadwayNetwork.LINK_FOREIGN_KEY_TO_ODE properties. If
                self.links_df.params.fks_to_nodes is ("A","B"), then (1,2) references the
                link of A=1 and B=2.
        """
        _query_parts = [
            f"{k} == {str(v)}"
            for k, v in zip(self.links_df.params.fks_to_nodes, link_key_values)
        ]
        _query = " and ".join(_query_parts)
        _links = self.links_df.query(_query, engine="python")

        return bool(len(_links))

    @property
    def shapes_without_links(self) -> pd.Series:
        """list of shape ids that don't have associated links."""

        _shape_fk_col = self.links_df.params.fk_to_shape
        unused_shape_ids = set(self.shapes_df.index) - set(self.links_df[_shape_fk_col])
        return list(unused_shape_ids)

    @property
    def nodes_without_links(self) -> pd.Series:
        """List of node ids that don't have associated links."""

        _from_col = self.links_df.params.from_node
        _to_col = self.links_df.params.to_node
        _used_node_ids = set(
            self.links_df[_from_col].tolist() + self.links_df[_to_col].tolist()
        )
        unused_node_ids = set(self.nodes_df.index) - _used_node_ids
        return list(unused_node_ids)

    def get_property_by_timespan_and_group(
        self, property, timespan=None, category=None
    ):
        """
        Return a series for the properties with a specific group or timespan

        args
        ------
        property: str
          the variable that you want from network
        timespan: list(str)
          the time period that you are querying for
          i.e. ['16:00', '19:00']
        category: str or list(str)(Optional)
          the group category
          i.e. "sov"

          or

          list of group categories in order of search, i.e.
          ["hov3","hov2"]

        returns
        --------
        pandas series
        """

        def _get_property(
            v,
            timespans=None,
            category=None,
            return_partial_match: bool = False,
            partial_match_minutes: int = 60,
        ):
            """

            .. todo:: return the time period with the largest overlap

            """

            if category and not timespans:
                WranglerLogger.error(
                    "\nShouldn't have a category group without time spans"
                )
                raise ValueError("Shouldn't have a category group without time spans")

            # simple case
            if type(v) in (int, float, str):
                return v

            if not category:
                category = ["default"]
            elif isinstance(category, str):
                category = [category]
            search_cats = [c.lower() for c in category]

            # if no time or group specified, but it is a complex link situation
            if not timespans:
                if "default" in v.keys():
                    return v["default"]
                else:
                    WranglerLogger.debug(f"variable: {v}")
                    msg = f"Variable {v} is more complex in network than query"
                    WranglerLogger.error(msg)
                    raise ValueError(msg)

            if v.get("timeofday"):
                categories = []
                for tg in v["timeofday"]:
                    if (
                        (timespans[0] >= tg["timespan"][0])
                        and (timespans[1] <= tg["timespan"][1])
                        and (timespans[0] <= timespans[1])
                    ):
                        if tg.get("category"):
                            categories += tg["category"]
                            for c in search_cats:
                                print("CAT:", c, tg["category"])
                                if c in tg["category"]:
                                    return tg["value"]
                        else:
                            return tg["value"]

                    if (
                        (timespans[0] >= tg["timespan"][0])
                        and (timespans[1] <= tg["timespan"][1])
                        and (timespans[0] > timespans[1])
                        and (tg["timespan"][0] > tg["timespan"][1])
                    ):
                        if tg.get("category"):
                            categories += tg["category"]
                            for c in search_cats:
                                print("CAT:", c, tg["category"])
                                if c in tg["category"]:
                                    return tg["value"]
                        else:
                            return tg["value"]

                    # if there isn't a fully matched time period, see if there is an overlapping
                    # one right now just return the first overlapping ones
                    # TODO return the time period with the largest overlap

                    if (
                        (timespans[0] >= tg["timespan"][0])
                        and (timespans[0] <= tg["timespan"][1])
                    ) or (
                        (timespans[1] >= tg["timespan"][0])
                        and (timespans[1] <= tg["timespan"][1])
                    ):
                        overlap_minutes = max(
                            0,
                            min(tg["timespan"][1], timespans[1])
                            - max(timespans[0], tg["timespan"][0]),
                        )
                        if not return_partial_match and overlap_minutes > 0:
                            WranglerLogger.debug(
                                f"Couldn't find time period consistent with {timespans}, but \
                                    found a partial match: {tg['timespan']}. Consider allowing \
                                    partial matches using 'return_partial_match' keyword or \
                                    updating query."
                            )
                        elif (
                            overlap_minutes < partial_match_minutes
                            and overlap_minutes > 0
                        ):
                            WranglerLogger.debug(
                                f"Time period: {timespans} overlapped less than the minimum \
                                    number of minutes ({overlap_minutes}<{partial_match_minutes})\
                                    to be considered a match with time period in network:\
                                    {tg['time']}."
                            )
                        elif overlap_minutes > 0:
                            WranglerLogger.debug(
                                f"Returning a partial time period match. Time period: {timespans}\
                                overlapped the minimum number of minutes ({overlap_minutes}>=\
                                {partial_match_minutes}) to be considered a match with time period\
                                 in network: {tg['time']}."
                            )
                            if tg.get("category"):
                                categories += tg["category"]
                                for c in search_cats:
                                    print("CAT:", c, tg["category"])
                                    if c in tg["category"]:
                                        return tg["value"]
                            else:
                                return tg["value"]

                """
                WranglerLogger.debug(
                    "\nCouldn't find time period for {}, returning default".format(
                        str(timespans)
                    )
                )
                """
                if "default" in v.keys():
                    return v["default"]
                else:
                    WranglerLogger.error(
                        "\nCan't find default; must specify a category in {}".format(
                            str(categories)
                        )
                    )
                    raise ValueError(
                        "Can't find default, must specify a category in: {}".format(
                            str(categories)
                        )
                    )

        timespans = parse_timespans_to_secs(timespan)

        return self.links_df[property].apply(
            _get_property, timespans=timespans, category=category
        )

    def _nodes_from_link(
        self, links_df: gpd.GeoDataFrame, link_pos: int, node_key_field: str
    ) -> gpd.GeoDataFrame:
        """Creates a basic list of node entries from links, their geometry, and a position.

        TODO: Does not currently fill in additional values used in nodes.

        Args:
            links_df (gpd.GeoDataFrame): subset of self.links_df or similar which needs nodes
                created
            link_pos (int): Position within geometry collection to use for geometry
            node_key_field (str): field name to use for generating index and node key

        Returns:
            gpd.GeoDataFrame: _description_
        """

        nodes_df = copy.deepcopy(
            links_df[[node_key_field, "geometry"]].drop_duplicates()
        )
        # WranglerLogger.debug(f"ct1: nodes_df:\n{nodes_df}")
        nodes_df = nodes_df.rename(
            columns={node_key_field: nodes_df.params.primary_key}
        )
        # WranglerLogger.debug(f"ct2: nodes_df:\n{nodes_df}")
        nodes_df["geometry"] = nodes_df["geometry"].apply(
            get_point_geometry_from_linestring, pos=link_pos
        )
        nodes_df["X"] = nodes_df.geometry.x
        nodes_df["Y"] = nodes_df.geometry.y
        nodes_df[nodes_df.params.idx_col] = nodes_df[nodes_df.params.primary_key]
        nodes_df.set_index(nodes_df.params.idx_col, inplace=True)
        # WranglerLogger.debug(f"ct3: nodes_df:\n{nodes_df}")
        return nodes_df

    def is_connected(self, mode: str) -> bool:
        """
        Determines if the network graph is "strongly" connected.

        A graph is strongly connected if each vertex is reachable from every other vertex.

        Args:
            mode:  mode of the network, one of `drive`,`transit`,`walk`, `bike`

        Returns: boolean
        """
        is_connected = nx.is_strongly_connected(self.get_modal_graph(mode))

        return is_connected

    @staticmethod
    def add_incident_link_data_to_nodes(
        links_df: DataFrame = None,
        nodes_df: DataFrame = None,
        link_variables: list = [],
    ) -> DataFrame:
        """
        Add data from links going to/from nodes to node.

        Args:
            links_df: if specified, will assess connectivity of this
                links list rather than self.links_df
            nodes_df: if specified, will assess connectivity of this
                nodes list rather than self.nodes_df
            link_variables: list of columns in links dataframe to add to incident nodes

        Returns:
            nodes DataFrame with link data where length is N*number of links going in/out
        """
        WranglerLogger.debug("Adding following link data to nodes: ".format())

        _link_vals_to_nodes = [x for x in link_variables if x in links_df.columns]
        if link_variables not in _link_vals_to_nodes:
            WranglerLogger.warning(
                "Following columns not in links_df and wont be added to nodes: {} ".format(
                    list(set(link_variables) - set(_link_vals_to_nodes))
                )
            )

        _nodes_from_links_A = nodes_df.merge(
            links_df[[links_df.params.from_node] + _link_vals_to_nodes],
            how="outer",
            left_on=nodes_df.params.primary_key,
            right_on=links_df.params.from_node,
        )
        _nodes_from_links_B = nodes_df.merge(
            links_df[[links_df.params.to_node] + _link_vals_to_nodes],
            how="outer",
            left_on=nodes_df.params.primary_key,
            right_on=links_df.params.to_node,
        )
        _nodes_from_links_ab = pd.concat([_nodes_from_links_A, _nodes_from_links_B])

        return _nodes_from_links_ab


class NodeDeletionError(Exception):
    pass


class LinkDeletionError(Exception):
    pass


class NodeAdditionError(Exception):
    pass


class LinkAdditionError(Exception):
    pass

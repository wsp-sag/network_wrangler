"""Roadway Network class and functions for Network Wrangler.

Used to represent a roadway network and perform operations on it.

Usage:

```python
from network_wrangler import load_roadway_from_dir, write_roadway

net = load_roadway_from_dir("my_dir")
net.get_selection({"links": [{"name": ["I 35E"]}]})
net.apply("my_project_card.yml")

write_roadway(net, "my_out_prefix", "my_dir", file_format = "parquet")
```
"""

from __future__ import annotations

import copy
import hashlib

from collections import defaultdict
from typing import List, Union, Literal, TYPE_CHECKING, Optional, Any
from pathlib import Path

import geopandas as gpd
import networkx as nx
import pandas as pd

from pandera.typing import DataFrame
from pydantic import BaseModel, field_validator

from projectcard import ProjectCard, SubProject

from .projects import (
    apply_new_roadway,
    apply_calculated_roadway,
    apply_roadway_deletion,
    apply_roadway_property_change,
)

from ..logger import WranglerLogger
from ..params import ShapesParams, LAT_LON_CRS, DEFAULT_CATEGORY, DEFAULT_TIMESPAN
from ..models.roadway.tables import RoadLinksTable, RoadNodesTable, RoadShapesTable
from ..models.projects.roadway_selection import SelectLinksDict, SelectNodesDict, SelectFacility
from ..models.projects.roadway_property_change import NodeGeometryChangeTable
from ..utils.models import empty_df_from_datamodel
from .selection import (
    RoadwayLinkSelection,
    RoadwayNodeSelection,
    _create_selection_key,
    SelectionError,
)
from .model_roadway import ModelRoadwayNetwork
from .nodes.create import data_to_nodes_df
from .links.create import data_to_links_df
from .links.links import shape_ids_unique_to_link_ids, node_ids_unique_to_link_ids
from .links.filters import filter_links_to_ids, filter_links_to_node_ids
from .links.delete import delete_links_by_ids
from .links.edit import edit_link_geometry_from_nodes
from .nodes.nodes import node_ids_without_links
from .nodes.filters import filter_nodes_to_links
from .nodes.delete import delete_nodes_by_ids
from .nodes.edit import edit_node_geometry
from .shapes.delete import delete_shapes_by_ids
from .shapes.edit import edit_shape_geometry_from_nodes
from .shapes.io import read_shapes
from .shapes.create import df_to_shapes_df

if TYPE_CHECKING:
    from networkx import MultiDiGraph
    from ..models.projects.roadway_selection import SelectFacility
    from ..models._base.types import TimespanString


Selections = Union[RoadwayLinkSelection, RoadwayNodeSelection]


class RoadwayNetwork(BaseModel):
    """Representation of a Roadway Network.

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
        nodes_df (RoadNodesTable): dataframe of of node records.
        links_df (RoadLinksTable): dataframe of link records and associated properties.
        shapes_df (RoadShapestable): data from of detailed shape records  This is lazily
            created iff it is called because shapes files can be expensive to read.
        selections (dict): dictionary of stored roadway selection objects, mapped by
            `RoadwayLinkSelection.sel_key` or `RoadwayNodeSelection.sel_key` in case they are
                made repeatedly.
        crs (str): coordinate reference system in ESPG number format. Defaults to DEFAULT_CRS
            which is set to 4326, WGS 84 Lat/Long
        network_hash: dynamic property of the hashed value of links_df and nodes_df. Used for
            quickly identifying if a network has changed since various expensive operations have
            taken place (i.e. generating a ModelRoadwayNetwork or a network graph)
        model_net (ModelRoadwayNetwork): referenced `ModelRoadwayNetwork` object which will be
            lazily created if None or if the `network_hash` has changed.
    """

    crs: Literal[LAT_LON_CRS] = LAT_LON_CRS
    nodes_df: DataFrame[RoadNodesTable]
    links_df: DataFrame[RoadLinksTable]
    _shapes_df: Optional[DataFrame[RoadShapesTable]] = None

    _links_file: Optional[Path] = None
    _nodes_file: Optional[Path] = None
    _shapes_file: Optional[Path] = None

    _shapes_params: ShapesParams = ShapesParams()
    _model_net: Optional[ModelRoadwayNetwork] = None
    _selections: dict[str, Selections] = {}
    _modal_graphs: dict[str, dict] = defaultdict(lambda: {"graph": None, "hash": None})

    @field_validator("nodes_df", "links_df")
    def coerce_crs(cls, v, info):
        """Coerce crs of nodes_df and links_df to network crs."""
        net_crs = info.data["crs"]
        if v.crs != net_crs:
            WranglerLogger.warning(
                f"CRS of links_df ({v.crs}) doesn't match network crs {net_crs}. \
                    Changing to network crs."
            )
            v.to_crs(net_crs)
        return v

    @property
    def shapes_df(self) -> DataFrame[RoadShapesTable]:
        """Load and return RoadShapesTable.

        If not already loaded, will read from shapes_file and return. If shapes_file is None,
        will return an empty dataframe with the right schema. If shapes_df is already set, will
        return that.
        """
        if (self._shapes_df is None or self._shapes_df.empty) and self._shapes_file is not None:
            self._shapes_df = read_shapes(
                self._shapes_file,
                in_crs=self.crs,
                shapes_params=self._shapes_params,
            )
        # if there is NONE, then at least create an empty dataframe with right schema
        elif self._shapes_df is None:
            self._shapes_df = empty_df_from_datamodel(RoadShapesTable, crs=self.crs)
            self._shapes_df.set_index("shape_id_idx", inplace=True)

        return self._shapes_df

    @shapes_df.setter
    def shapes_df(self, value):
        self._shapes_df = df_to_shapes_df(value, shapes_params=self._shapes_params)

    @property
    def network_hash(self) -> str:
        """Hash of the links and nodes dataframes."""
        _value = str.encode(self.links_df.df_hash() + "-" + self.nodes_df.df_hash())

        _hash = hashlib.sha256(_value).hexdigest()
        return _hash

    @property
    def model_net(self) -> ModelRoadwayNetwork:
        """Return a ModelRoadwayNetwork object for this network."""
        if self._model_net is None or self._model_net._net_hash != self.network_hash:
            self._model_net = ModelRoadwayNetwork(self)
        return self._model_net

    @property
    def summary(self) -> dict:
        """Quick summary dictionary of number of links, nodes."""
        d = {
            "links": len(self.links_df),
            "nodes": len(self.nodes_df),
        }
        return d

    @property
    def link_shapes_df(self) -> gpd.GeoDataFrame:
        """Add shape geometry to links if available.

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

    def get_property_by_timespan_and_group(
        self,
        link_property: str,
        category: Union[str, int] = DEFAULT_CATEGORY,
        timespan: TimespanString = DEFAULT_TIMESPAN,
        strict_timespan_match: bool = False,
        min_overlap_minutes: int = 60,
    ) -> Any:
        """Returns a new dataframe with model_link_id and link property by category and timespan.

        Convenience method for backward compatability.

        Args:
            link_property: link property to query
            category: category to query or a list of categories. Defaults to DEFAULT_CATEGORY.
            timespan: timespan to query in the form of ["HH:MM","HH:MM"].
                Defaults to DEFAULT_TIMESPAN.
            strict_timespan_match: If True, will only return links that match the timespan exactly.
                Defaults to False.
            min_overlap_minutes: If strict_timespan_match is False, will return links that overlap
                with the timespan by at least this many minutes. Defaults to 60.
        """
        from .links.scopes import prop_for_scope

        return prop_for_scope(
            self.links_df,
            link_property,
            timespan=timespan,
            category=category,
            strict_timespan_match=strict_timespan_match,
            min_overlap_minutes=min_overlap_minutes,
        )

    def get_selection(
        self,
        selection_dict: Union[dict, SelectFacility],
        overwrite: bool = False,
    ) -> Union[RoadwayNodeSelection, RoadwayLinkSelection]:
        """Return selection if it already exists, otherwise performs selection.

        Args:
            selection_dict (dict): SelectFacility dictionary.
            overwrite: if True, will overwrite any previously cached searches. Defaults to False.
        """
        key = _create_selection_key(selection_dict)
        if (key in self._selections) and not overwrite:
            WranglerLogger.debug(f"Using cached selection from key: {key}")
            return self._selections[key]

        if isinstance(selection_dict, SelectFacility):
            selection_data = selection_dict
        elif isinstance(selection_dict, SelectLinksDict):
            selection_data = SelectFacility(links=selection_dict)
        elif isinstance(selection_dict, SelectNodesDict):
            selection_data = SelectFacility(nodes=selection_dict)
        elif isinstance(selection_dict, dict):
            selection_data = SelectFacility(**selection_dict)
        else:
            WranglerLogger.error(f"`selection_dict` arg must be a dictionary or SelectFacility model.\
                             Received: {selection_dict} of type {type(selection_dict)}")
            raise SelectionError("selection_dict arg must be a dictionary or SelectFacility model")

        WranglerLogger.debug(f"Getting selection from key: {key}")
        if selection_data.feature_types in ["links", "segment"]:
            return RoadwayLinkSelection(self, selection_dict)
        elif selection_data.feature_types == "nodes":
            return RoadwayNodeSelection(self, selection_dict)
        else:
            WranglerLogger.error("Selection data should be of type 'segment', 'links' or 'nodes'.")
            raise SelectionError("Selection data should be of type 'segment', 'links' or 'nodes'.")

    def modal_graph_hash(self, mode) -> str:
        """Hash of the links in order to detect a network change from when graph created."""
        _value = str.encode(self.links_df.df_hash() + "-" + mode)
        _hash = hashlib.sha256(_value).hexdigest()

        return _hash

    def get_modal_graph(self, mode) -> MultiDiGraph:
        """Return a networkx graph of the network for a specific mode.

        Args:
            mode: mode of the network, one of `drive`,`transit`,`walk`, `bike`
        """
        from .graph import net_to_graph

        if self._modal_graphs[mode]["hash"] != self.modal_graph_hash(mode):
            self._modal_graphs[mode]["graph"] = net_to_graph(self, mode)

        return self._modal_graphs[mode]["graph"]

    def apply(self, project_card: Union[ProjectCard, dict]) -> RoadwayNetwork:
        """Wrapper method to apply a roadway project, returning a new RoadwayNetwork instance.

        Args:
            project_card: either a dictionary of the project card object or ProjectCard instance
        """
        if not (isinstance(project_card, ProjectCard) or isinstance(project_card, SubProject)):
            project_card = ProjectCard(project_card)

        project_card.validate()

        if project_card.sub_projects:
            for sp in project_card.sub_projects:
                WranglerLogger.debug(f"- applying subproject: {sp.change_type}")
                self._apply_change(sp)
            return self
        else:
            return self._apply_change(project_card)

    def _apply_change(self, change: Union[ProjectCard, SubProject]) -> RoadwayNetwork:
        """Apply a single change: a single-project project or a sub-project."""
        if not isinstance(change, SubProject):
            WranglerLogger.info(f"Applying Project to Roadway Network: {change.project}")

        if change.change_type == "roadway_property_change":
            return apply_roadway_property_change(
                self,
                self.get_selection(change.facility),
                change.roadway_property_change["property_changes"],
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
            WranglerLogger.error(f"Couldn't find project in: \n{change.__dict__}")
            raise (ValueError(f"Invalid Project Card Category: {change.change_type}"))

    def links_with_link_ids(self, link_ids: List[int]) -> DataFrame[RoadLinksTable]:
        """Return subset of links_df based on link_ids list."""
        return filter_links_to_ids(self.links_df, link_ids)

    def links_with_nodes(self, node_ids: List[int]) -> DataFrame[RoadLinksTable]:
        """Return subset of links_df based on node_ids list."""
        return filter_links_to_node_ids(self.links_df, node_ids)

    def nodes_in_links(self) -> DataFrame[RoadNodesTable]:
        """Returns subset of self.nodes_df that are in self.links_df."""
        return filter_nodes_to_links(self.links_df, self.nodes_df)

    def add_links(self, add_links_df: Union[pd.DataFrame, DataFrame[RoadLinksTable]]):
        """Validate combined links_df with LinksSchema before adding to self.links_df.

        Args:
            add_links_df: Dataframe of additional links to add.
        """
        if not isinstance(add_links_df, RoadLinksTable):
            add_links_df = data_to_links_df(add_links_df, nodes_df=self.nodes_df)
        self.links_df = RoadLinksTable(pd.concat([self.links_df, add_links_df]))

    def add_nodes(self, add_nodes_df: Union[pd.DataFrame, DataFrame[RoadNodesTable]]):
        """Validate combined nodes_df with NodesSchema before adding to self.nodes_df.

        Args:
            add_nodes_df: Dataframe of additional nodes to add.
        """
        if not isinstance(add_nodes_df, RoadNodesTable):
            add_nodes_df = data_to_nodes_df(add_nodes_df)
        self.nodes_df = RoadNodesTable(pd.concat([self.nodes_df, add_nodes_df]))

    def add_shapes(self, add_shapes_df: Union[pd.DataFrame, DataFrame[RoadShapesTable]]):
        """Validate combined shapes_df with RoadShapesTable efore adding to self.shapes_df.

        Args:
            add_shapes_df: Dataframe of additional shapes to add.
        """
        if not isinstance(add_shapes_df, RoadShapesTable):
            add_shapes_df = df_to_shapes_df(add_shapes_df)
        WranglerLogger.debug(f"add_shapes_df: \n{add_shapes_df}")
        WranglerLogger.debug(f"self.shapes_df: \n{self.shapes_df}")
        together_df = pd.concat([self.shapes_df, add_shapes_df])
        WranglerLogger.debug(f"together_df: \n{together_df}")
        self.shapes_df = RoadShapesTable(pd.concat([self.shapes_df, add_shapes_df]))

    def delete_links(
        self,
        selection_dict: SelectLinksDict,
        clean_nodes: bool = False,
        clean_shapes: bool = False,
    ):
        """Deletes links based on selection dictionary and optionally associated nodes and shapes.

        Args:
            selection_dict (SelectLinks): Dictionary describing link selections as follows:
                `all`: Optional[bool] = False. If true, will select all.
                `name`: Optional[list[str]]
                `ref`: Optional[list[str]]
                `osm_link_id`:Optional[list[str]]
                `model_link_id`: Optional[list[int]]
                `modes`: Optional[list[str]]. Defaults to "any"
                `ignore_missing`: if true, will not error when defaults to True.
                ...plus any other link property to select on top of these.
            clean_nodes (bool, optional): If True, will clean nodes uniquely associated with
                deleted links. Defaults to False.
            clean_shapes (bool, optional): If True, will clean nodes uniquely associated with
                deleted links. Defaults to False.
        """
        selection_dict = SelectLinksDict(**selection_dict).model_dump(
            exclude_none=True, by_alias=True
        )
        selection = self.get_selection({"links": selection_dict})

        if clean_nodes:
            node_ids_to_delete = node_ids_unique_to_link_ids(
                selection.selected_links, selection.selected_links_df, self.nodes_df
            )
            WranglerLogger.debug(
                f"Dropping nodes associated with dropped links: \n{node_ids_to_delete}"
            )
            self.nodes_df = delete_nodes_by_ids(self.nodes_df, del_node_ids=node_ids_to_delete)

        if clean_shapes:
            shape_ids_to_delete = shape_ids_unique_to_link_ids(
                selection.selected_links, selection.selected_links_df, self.shapes_df
            )
            WranglerLogger.debug(
                f"Dropping shapes associated with dropped links: \n{shape_ids_to_delete}"
            )
            self.shapes_df = delete_shapes_by_ids(
                self.shapes_df, del_shape_ids=shape_ids_to_delete
            )

        self.links_df = delete_links_by_ids(
            self.links_df,
            selection.selected_links,
            ignore_missing=selection.ignore_missing,
        )

    def delete_nodes(
        self,
        selection_dict: Union[dict, SelectNodesDict],
        remove_links: bool = False,
    ) -> None:
        """Deletes nodes from roadway network. Wont delete nodes used by links in network.

        Args:
            selection_dict: dictionary of node selection criteria in the form of a SelectNodesDict.
            remove_links: if True, will remove any links that are associated with the nodes.
                If False, will only remove nodes if they are not associated with any links.
                Defaults to False.

        raises:
            NodeDeletionError: If not ignore_missing and selected nodes to delete aren't in network
        """
        if not isinstance(selection_dict, SelectNodesDict):
            selection_dict = SelectNodesDict(**selection_dict)
        selection_dict = selection_dict.model_dump(exclude_none=True, by_alias=True)
        selection: RoadwayNodeSelection = self.get_selection(
            {"nodes": selection_dict},
        )
        if remove_links:
            del_node_ids = selection.selected_nodes
            link_ids = self.links_with_nodes(selection.selected_nodes).model_link_id.to_list()
            WranglerLogger.info(f"Removing {len(link_ids)} links associated with nodes.")
            self.delete_links({"model_link_id": link_ids})
        else:
            unused_node_ids = node_ids_without_links(self.nodes_df, self.links_df)
            del_node_ids = list(set(selection.selected_nodes).intersection(unused_node_ids))

        self.nodes_df = delete_nodes_by_ids(
            self.nodes_df, del_node_ids, ignore_missing=selection.ignore_missing
        )

    def clean_unused_shapes(self):
        """Removes any unused shapes from network that aren't referenced by links_df."""
        from .shapes.shapes import shape_ids_without_links

        del_shape_ids = shape_ids_without_links(self.shapes_df, self.links_df)
        self.shapes_df = self.shapes_df.drop(del_shape_ids)

    def clean_unused_nodes(self):
        """Removes any unused nodes from network that aren't referenced by links_df.

        NOTE: does not check if these nodes are used by transit, so use with caution.
        """
        from .nodes.nodes import node_ids_without_links

        node_ids = node_ids_without_links(self.nodes_df, self.links_df)
        self.nodes_df = self.nodes_df.drop(node_ids)

    def move_nodes(
        self,
        node_geometry_change_table: DataFrame[NodeGeometryChangeTable],
    ):
        """Moves nodes based on updated geometry along with associated links and shape geometry.

        Args:
            node_geometry_change_table: a table with model_node_id, X, Y, and CRS.
        """
        node_geometry_change_table = NodeGeometryChangeTable(node_geometry_change_table)
        node_ids = node_geometry_change_table.model_node_id.to_list()
        WranglerLogger.debug(f"Moving nodes: {node_ids}")
        self.nodes_df = edit_node_geometry(self.nodes_df, node_geometry_change_table)
        self.links_df = edit_link_geometry_from_nodes(self.links_df, self.nodes_df, node_ids)
        self.shapes_df = edit_shape_geometry_from_nodes(
            self.shapes_df, self.links_df, self.nodes_df, node_ids
        )

    def has_node(self, model_node_id: int) -> bool:
        """Queries if network has node based on model_node_id.

        Args:
            model_node_id: model_node_id to check for.
        """
        has_node = self.nodes_df[self.nodes_df.model_node_id].isin([model_node_id]).any()

        return has_node

    def has_link(self, ab: tuple) -> bool:
        """Returns true if network has links with AB values.

        Args:
            ab: Tuple of values corresponding with A and B.
        """
        sel_a, sel_b = ab
        has_link = self.links_df[self.links_df[["A", "B"]]].isin({"A": sel_a, "B": sel_b}).any()
        return has_link

    def is_connected(self, mode: str) -> bool:
        """Determines if the network graph is "strongly" connected.

        A graph is strongly connected if each vertex is reachable from every other vertex.

        Args:
            mode:  mode of the network, one of `drive`,`transit`,`walk`, `bike`
        """
        is_connected = nx.is_strongly_connected(self.get_modal_graph(mode))

        return is_connected

    @staticmethod
    def add_incident_link_data_to_nodes(
        links_df: Optional[DataFrame[RoadLinksTable]] = None,
        nodes_df: Optional[DataFrame[RoadNodesTable]] = None,
        link_variables: list = [],
    ) -> DataFrame[RoadNodesTable]:
        """Add data from links going to/from nodes to node.

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

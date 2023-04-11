import hashlib

from typing import Any, Collection, Mapping

import pandas as pd
from geopandas import GeoDataFrame

from ..logger import WranglerLogger

from .segment import Segment
from .graph import shortest_path,links_nodes_to_ox_graph,SP_WEIGHT_COL,SP_WEIGHT_FACTOR

class SelectionFormatError(Exception):
    pass

class SelectionError(Exception):

class RoadwaySelection():
    """_summary_

    Properties:
        net:
        id:
        type: one of "unique_link_id", "unique_node_id" or "segment_search"

    """

    def __init__(self,net,selection_dict:dict,additional_requirements = {"drive_access": True},ignore = []):
        self.net

        # This should make it compatible with old and new project card types
        self.selection_dict = selection_dict.update(selection_dict.get("links",{}))
        self.sel_key = RoadwaySelection._assign_selection_key(self.selection_dict)
        self.select = None
        self.type = self._assign_selection_type(self.selection_dict)
        self.additional_requirements = additional_requirements
        self.ignore = ignore + ["O","D","A","B"]

        self.selected_links_df = None

        if self.additional_requirements:
            for k,v in additional_requirements.items():
                self.selection_dict[k]=v

        self.segment = None

        if self.type == "segment_search":
            self.segment = Segment(self.net, self.selection_dict)
            
    @property
    def selected_links(self) -> list:
        if not self.found:
            return None
        return self.selected_links_df.index.tolist()
    
    @property
    def found(self) ->bool:
        if self.selected_links_df is not None:
            return True
        return False
    
    @staticmethod
    def _assign_selection_key(selection_dict: dict) -> tuple:
        """
        Selections are stored by a hash of the selection dictionary.

        Args:
            selection_dictonary: Selection Dictionary

        Returns: Hex code for hash
        """

        return hashlib.md5(b"selection_dict").hexdigest()

    def _assign_selection_type(self,selection_dict: dict):
        selection_keys = list(selection_dict.keys()) 
        
        # if selection has a unique id, then its a unique_id type
        if not set(self.net.UNIQUE_MODEL_LINK_IDENTIFIERS).isdisjoint(selection_keys):
            self.select = self._select_unique_link_id
            return "unique_link_id"
        
        elif set(["A","B","name"]).issubset(selection_keys):
            self.select = self._select_roadway_segment
            return "segment_search"
        
        elif set(["O","D","name"]).issubset(selection_keys):
            self.select = self._select_roadway_segment
            return "segment_search"
        
        else:
            WranglerLogger.error(f"Selection type not found for : {selection_dict}")
            WranglerLogger.error(f"Expected one of: {self.net.UNIQUE_MODEL_LINK_IDENTIFIERS} or A, B, name or O, D, name")
            raise SelectionFormatError("Don't believe selection is valid - can't find a unique id or A, B, Name or O,D name")

    

    def _select_unique_link_id(
        self,
    ):
        """_summary_

         Args:
            selection_dict: selection dictionary 
        """

        _sel_links_mask = self.links_df.isin(self.selection_dict).any(axis=1)
        self.selected_links_df = self.net.links_df.loc[_sel_links_mask]

        if not _sel_links_mask.any():
            WranglerLogger.warning("No links found matching criteria.")
            return False
        
        return True
    
    def _dict_to_query(
        selection_dict: Mapping[str, Any],
    )-> str: 
        """Generates the initial query of candidate links for a roadway segment.

        Args:
            selection_dict: selection dictionary 

        Returns:
            _type_: Query value
        """
        WranglerLogger.debug("Building selection query")
        def _kv_to_query_part(k,v,_q_part=''):
            if isinstance(v,list):
                _q_part += "(" + " or ".join([_kv_to_query_part(k,i) for i in v]) + ")"
                return _q_part
            if isinstance(v, str):
                return k + '.str.contains("' + v + '")'
            else:
                return k + "==" + str(v)
            
        query = "(" + " and ".join([_kv_to_query_part(k,v) for k,v in selection_dict.items()]) + ")"
        WranglerLogger.debug(f"Selection query:\n{query}")
        return query
    
    def _select_roadway_segment(
        self,
    ):
        """_summary_

        """
        # identify candidate links which match the initial query
        # assign them as iteration = 0
        # subsequent iterations that didn't match the query will be
        # assigned a heigher weight in the shortest path
        
        WranglerLogger.debug("Selecting segment of connected links")
        _selection_dict = self.selection_dict.copy()

        # First search for initial set of links using "name" and if that doesn't return any links, 
        #   search using "ref"
        if "ref" in _selection_dict:
            _selection_dict["name"] += _selection_dict["ref"]
            del _selection_dict["ref"]

        self.sel_query = self._segment_query(_selection_dict)
        self.subnet_links_df = self.links_df.query(self.sel_query, engine="python")
        if len(self.subnet_links_df) == 0:
            WranglerLogger.warning(f"No links found using selection of name =  {_selection_dict['name']}.") 
            if "ref" not in self.selection_dict:
                raise SegmentSelectionError("No links found with selection.")
            else:
                del _selection_dict["name"]
                _selection_dict["ref"] = self.selection_dict["ref"]

                WranglerLogger.info(f"Trying ref = {self.selection_dict['ref']} instead")
                self.sel_query = self._segment_query(_selection_dict)
                self.subnet_links_df = self.links_df.query(self.sel_query, engine="python")

                if len(self.subnet_links_df) == 0:
                    WranglerLogger.error(f"No links found using selection of ref =  {_selection_dict['ref']}.") 
                    raise SegmentSelectionError("No links found with selection.")
            
        # i is iteration # for an iterative search for connected paths with progressively larger subnet
        self.subnet_links_df["i"] = 0

        self.path_search(
            self.selections[sel_key]["candidate_links"],
            A_pk,
            B_pk,
            weight_factor=RoadwayNetwork.SP_WEIGHT_FACTOR,
        )

        # Conduct a "selection on the selection" if have additional requirements to satisfy
        else:
            resel_query = ProjectCard.build_selection_query(
                selection=selection,
                unique_ids=RoadwayNetwork.UNIQUE_MODEL_LINK_IDENTIFIERS,
                mode=RoadwayNetwork.MODES_TO_NETWORK_LINK_VARIABLES[search_mode],
                ignore=["name"],
            )
            WranglerLogger.debug("Reselecting features:\n{}".format(resel_query))
            self.selections[sel_key]["selected_links"] = self.selections[sel_key][
                "links"
            ].query(resel_query, engine="python")

        if len(self.selections[sel_key]["selected_links"]) > 0:
            self.selections[sel_key]["selection_found"] = True
        else:
            raise SelectionError(f"No links found for selection: {selection}")

        self.selections[sel_key]["selection_found"] = True
        return self.selections[sel_key]["selected_links"].index.tolist()

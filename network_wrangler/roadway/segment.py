from ..logger import WranglerLogger

class SegmentFormatError(Exception):
    pass

class Segment:

    def __init__(self,net,segment_def_dict):

        self.net = net
        self.segment_def_dict = segment_def_dict
        self.segment_links_df = None
        self.segment_nodes_df = None

        self.od_nodes = self._calculate_od_node_fks(self.selection_dict)

        # for segment_search
        self.sel_query = None
        self.subnet_links_df = None
        self.graph = None

        # segment members
        self.segment_route_nodes = []
        self.segment_links = []

    def _calculate_od_node_fks(
        self, segment_def_dict: dict
    ) -> tuple:
        """
        Returns the foreign key id (whatever is used in the u and v
        variables in the links file) for the AB nodes as a tuple.

        Args:
            segment_def_dictionary : segment definition dictionary with O and D or A and B specified as a key-value pair
                e.g. A: {osm_node_id = 'asdf2390'} B: {model_node_id = 12345}

        Returns: tuple of (origin node pk, destination node pk)
        """
        if set(["A","B"]).issubset(segment_def_dict):
            _o_dict = selection_dict["A"]
            _d_dict = selection_dict["B"]
        elif set(["O","D"]).issubset(segment_def_dict):
            _o_dict = selection_dict["O"]
            _d_dict = selection_dict["D"]
        else:
            raise SegmentFormatError()

        if len(_o_dict) > 1 or len(_d_dict) >1:
            WranglerLogger.debug(f"_o_dict: {_o_dict}\n_d_dict: {_d_dict}")
            raise SegmentFormatError("O and D of selection should have only one value each.")

        o_node_prop, o_val = next(iter(_o_dict.items()))
        d_node_prop, d_val = next(iter(_d_dict.items()))

        if o_node_prop != self.net.UNIQUE_NODE_KEY:
            _o_pk_list = self.net.nodes_df[self.net.nodes_df[o_node_prop] == o_val].index.tolist()
            if len(_o_pk_list) != 1:
                WranglerLogger.error(f"Node selectio for segment invalid. Found {len(_o_pk_list)} \
                    in nodes_df with {o_node_prop} = {o_val}. Should only find one!")
            o_pk = _o_pk_list[0]
        else:
            o_pk = o_val

        if d_node_prop != self.net.UNIQUE_NODE_KEY:
            _d_pk_list = self.net.nodes_df[self.net.nodes_df[o_node_prop] == o_val].index.tolist()
            if len(_d_pk_list) != 1:
                WranglerLogger.error(f"Node selection for segment invalid. Found {len(_d_pk_list)} \
                    in nodes_df with {d_node_prop} = {d_val}. Should only find one!")
            d_pk = _d_pk_list[0]
        else:
            d_pk = d_val

        return (o_pk, d_pk)
    
def connected_path_search(
        self,
        subnet_links_df: GeoDataFrame,
        O_pk,
        D_ok,
        weight_column: str = "i",
        weight_factor: float = 1.0,
    ):
        """

        Args:
            candidate_links: selection of links geodataframe with links likely to be part of path
            O_pk: origin node foreigh key ID
            D_pk: destination node foreigh key ID
            weight_column: column to use for weight of shortest path. Defaults to "i" (iteration)
            weight_factor: optional weight to multiply the weight column by when finding
                the shortest path

        Returns

        """

        

        # -----------------------------------
        # Set search breadth to zero + set max
        # -----------------------------------
        i = 0
        max_i = RoadwayNetwork.SEARCH_BREADTH
        # -----------------------------------
        # Add links to the graph until
        #   (i) the A and B nodes are in the
        #       foreign key list
        #          - OR -
        #   (ii) reach maximum search breadth
        # -----------------------------------
        node_list_foreign_keys = RoadwayNetwork.nodes_in_links(candidate_links_df)
        WranglerLogger.debug("Initial set of nodes: {}".format(node_list_foreign_keys))
        while (
            O_id not in node_list_foreign_keys or D_id not in node_list_foreign_keys
        ) and i <= max_i:
            WranglerLogger.debug(
                "Adding breadth - i: {}, Max i: {}] - {} and {} not found in node list.".format(
                    i, max_i, O_id, D_id
                )
            )
            i += 1
            candidate_links_df, node_list_foreign_keys = _add_breadth(
                candidate_links_df, self.nodes_df, self.links_df, i=i
            )
        # -----------------------------------
        #  Once have A and B in graph,
        #  Try calculating shortest path
        # -----------------------------------
        WranglerLogger.debug("calculating shortest path from graph")
        (
            sp_found,
            graph,
            candidate_links_df,
            shortest_path_route,
            shortest_path_links,
        ) = self.shortest_path(candidate_links_df, O_id, D_id)
        if sp_found:
            return graph, candidate_links_df, shortest_path_route, shortest_path_links

        if not sp_found:
            WranglerLogger.debug(
                "No shortest path found with breadth of {i}, trying greater breadth until SP \
                    found or max breadth {max_i} reached."
            )
        while not sp_found and i <= RoadwayNetwork.MAX_SEARCH_BREADTH:
            WranglerLogger.debug(
                "Adding breadth, with shortest path iteration. i: {} Max i: {}".format(
                    i, max_i
                )
            )
            i += 1
            candidate_links_df, node_list_foreign_keys = _add_breadth(
                candidate_links_df, self.nodes_df, self.links_df, i=i
            )
            (
                sp_found,
                graph,
                candidate_links_df,
                route,
                shortest_path_links,
            ) = self.shortest_path(candidate_links_df, O_id, D_id)

        if sp_found:
            return graph, candidate_links_df, route, shortest_path_links

        if not sp_found:
            msg = "Couldn't find path from {} to {} after adding {} links in breadth".format(
                O_id, D_id, i
            )
            WranglerLogger.error(msg)
            raise NoPathFound(msg)
    
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
                raise SelectionError("No links found with selection.")
            else:
                del _selection_dict["name"]
                _selection_dict["ref"] = self.selection_dict["ref"]

                WranglerLogger.info(f"Trying ref = {self.selection_dict['ref']} instead")
                self.sel_query = self._segment_query(_selection_dict)
                self.subnet_links_df = self.links_df.query(self.sel_query, engine="python")

                if len(self.subnet_links_df) == 0:
                    WranglerLogger.error(f"No links found using selection of ref =  {_selection_dict['ref']}.") 
                    raise SelectionError("No links found with selection.")
            
        # i is iteration # for an iterative search for connected paths with progressively larger subnet
        self.subnet_links_df["i"] = 0

        self.path_search(
            self.selections[sel_key]["candidate_links"],
            A_id,
            B_id,
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

    def _expand_subnet(
        _candidate_links_df: gpd.GeoDataFrame,
        _nodes_df: gpd.GeoDataFrame,
        _links_df: gpd.GeoDataFrame,
        i: int = None,
    ):
        """
        Add outbound and inbound reference IDs to candidate links
        from existing nodes

        Args:
            _candidate_links_df : df with the links from the previous iteration
            _nodes_df : df of all nodes in the full network
            _links_df : df of all links in the full network
            i : iteration of adding breadth

        Returns:
            candidate_links : GeoDataFrame
                updated df with one more degree of added breadth

            node_list_foreign_keys : list of foreign key ids for nodes in the updated
                candidate links to test if the A and B nodes are in there.
        """
        WranglerLogger.debug("-Adding Breadth-")

        if not i:
            WranglerLogger.warning("i not specified in _add_breadth, using 1")
            i = 1

        _candidate_nodes_df = _nodes_df.loc[
            RoadwayNetwork.nodes_in_links(_candidate_links_df)
        ]
        WranglerLogger.debug("Candidate Nodes: {}".format(len(_candidate_nodes_df)))

        # Identify links to add based on outbound and inbound links from nodes
        _links_shstRefId_to_add = list(
            set(
                sum(_candidate_nodes_df["outboundReferenceIds"].tolist(), [])
                + sum(_candidate_nodes_df["inboundReferenceIds"].tolist(), [])
            )
            - set(_candidate_links_df["shstReferenceId"].tolist())
            - set([""])
        )
        _links_to_add_df = _links_df[
            _links_df.shstReferenceId.isin(_links_shstRefId_to_add)
        ]

        WranglerLogger.debug("Adding {} links.".format(_links_to_add_df.shape[0]))

        # Add information about what iteration the link was added in
        _links_df[_links_df.model_link_id.isin(_links_shstRefId_to_add)]["i"] = i

        # Append links and update node list
        _candidate_links_df = pd.concat([_candidate_links_df, _links_to_add_df])
        _node_list_foreign_keys = RoadwayNetwork.nodes_in_links(_candidate_links_df)

        return _candidate_links_df, _node_list_foreign_keys

def find_subnet_shortest_path(
    net,
    subnet_links_df: GeoDataFrame,
    O_id,
    D_id, 
    sp_weight_col: str = SP_WEIGHT_COL,
    sp_weight_factor: float = SP_WEIGHT_FACTOR,
    link_foreign_key_to_node = ("A","A")
):
    
    WranglerLogger.debug(
        f"Calculating shortest path from {O_id} to {D_id} using {sp_weight_col} as \
            weight with a factor of {sp_weight_factor}"
    )

    subnet_nodes_df = net.nodes_df.loc[net.nodes_in_links(subnet_links_df)]

    # Create Graph
    G = links_nodes_to_ox_graph(
        subnet_nodes_df, 
        subnet_links_df,
        link_foreign_key_to_node = link_foreign_key_to_node,
        sp_weight_col = sp_weight_col,
        sp_weight_factor = sp_weight_factor)

    sp_route = shortest_path(G,O_id,D_id,sp_weight_col)

    if not sp_route:
        WranglerLogger.debug("No SP from {} to {} Found.".format(O_id, D_id))
        return False, G, subnet_links_df, None, None

    sp_links = subnet_links_df[
            subnet_links_df[link_foreign_key_to_node[0]].isin(sp_route) & subnet_links_df[link_foreign_key_to_node[1]].isin(sp_route)
        ]

    return True, G, subnet_links_df, sp_route, sp_links
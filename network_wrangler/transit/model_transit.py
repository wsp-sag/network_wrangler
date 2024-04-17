import copy

import pandas as pd


class ModelTransit:
    def __init__(
        self,
        transit_net: "TransitNetwork",
        roadway_net: "RoadwayNetwork",
        shift_transit_to_managed_lanes: bool = True,
    ):
        self.transit_net = transit_net
        self.roadway_net = roadway_net
        self._roadway_net_hash = None
        self._transit_feed_hash = None
        self._transit_shifted_to_ML = shift_transit_to_managed_lanes

        self._m_feed = self.create_model_transit_feed(transit_net, roadway_net)

    @property
    def model_roadway_net(self):
        return self.roadway_net.model_net

    @property
    def consistent_nets(self) -> bool:
        """Indicate if roadway and transit networks have changed since self.m_feed updated."""
        if (self.roadway_net.network_hash == self._roadway_net_hash) and (
            self.transit_net.feed_hash == self._transit_feed_hash
        ):
            return True
        return False

    @property
    def m_feed(self):
        """TransitNetwork.feed with updates for consistency with associated ModelRoadwayNetwork."""
        if self.consistent_nets:
            return self._m_feed

        # If netoworks have changed, updated model transit and update reference hash
        self._roadway_net_hash = copy.deepcopy(self.roadway_net.network_hash)
        self._transit_feed_hash = copy.deepcopy(self.transit_net.feed_hash)

        if not self._transit_shifted_to_ML:
            self._m_feed = self.transit_net.feed.copy()
            return self._m_feed

    def _shift_transit_to_managed_lanes(
        self,
        trip_ids: pd.Series,
        node_ids: list,
    ) -> "TransitNetwork":
        """_summary_

        FIXME
        Args:
            selftrip_ids (pd.Series): _description_
            node_ids (list): _description_

        Returns:
            TransitNetwork: _description_
        """
        # Traversed nodes without a stop should be negative integers
        net = copy.deepcopy(self)
        all_stops = net.feed.stops[TransitNetwork.STOPS_FOREIGN_KEY].tolist()
        node_ids = [int(x) if str(x) in all_stops else int(x) * -1 for x in node_ids]

        net.apply(
            net.get_selection({"trip_id": trip_ids}),
            properties={
                "existing": node_ids,
                "set": RoadwayNetwork.get_managed_lane_node_ids(node_ids),
            },
        )
        return net

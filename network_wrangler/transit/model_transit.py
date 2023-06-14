class ModelTransit:
    def __init__(self, transit_net: "TransitNetwork", roadway_net: "RoadwayNetwork"):
        self.transit_net = transit_net
        self.roadway_net = roadway_net

    @property
    def model_roadway_net(self):
        return self.roadway_net.model_net

    # def write()


# def shift_transit_to_managed_lanes(transit_net,roadway_net) -> ModelTransit:

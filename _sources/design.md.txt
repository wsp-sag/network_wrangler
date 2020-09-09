# Design


## Atomic Parts

NetworkWrangler deals with four primary atomic parts:

**1. [`Scenario`](_generated/network_wrangler.Scenario.html)** objects describe a Roadway Network, Transit Network, and collection of Projects. Scenarios manage the addition and construction of projects on the network via projct cards. Scenarios can be based on or tiered from other scenarios.

**2. [`RoadwayNetwork`](_generated/network_wrangler.RoadwayNetwork.html)** objects stores information about roadway nodes, directed links between nodes, and the shapes of links (note that the same shape can be shared between two or more links). Network Wrangler reads/writes roadway network objects from/to three files: `links.json`, `shapes.geojson`, and `nodes.geojson`. Their data is stored as GeoDataFrames in the object.

**3. [`TransitNetwork`](_generated/network_wrangler.TransitNetwork.html)** objects contain information about stops, routes, trips, shapes, stoptimes, and frequencies. Network Wrangler reads/writes transit network information from/to gtfs csv files and stores them as DataFrames within a
`Partridge` `feed` object.  Transit networks can be associated with Roadway networks.

**4.[`ProjectCard`](_generated/network_wrangler.ProjectCard.html)** objects store infromation (including  metadata) about changes to the network.  Network Wtanglr reads project cards from .yml files validates them, and manages them within a `Scenario` object.

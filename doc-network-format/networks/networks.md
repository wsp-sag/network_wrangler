# Networks

## Roadway Network Format

RoadwayNetworks must be defined in the following format, which is an extension of the [SharedStreets](http://sharedstreets.io) format which leverages [Open Street Map](http://openstreetmap.org) which uses a tag-based format (e.g. json, xml).  This may be changed in the future to align with the tabular-based [General Modeling Network Specification](https://github.com/zephyr-data-specs/GMNS).

A network is defined by a set of nodes and links which connect them.  Shapes may be optionally specified for each link in a separate file.

### Nodes

A  valid `geojson` file with `point` features and the folowing `properties`.

| **Property** | **Type** |  **Description** |
| --------- | -------- | ---------------- |
| model_node_id | Any |  PrimaryKey. Most modeling assignment software requires this be an integer. |
| osm_node_id | str | Reference to open street map node id. Used for querying. Not guaranteed to be unique. |
| shstReferenceId | str | Shared Streets ID reference. Used for querying |

### Links

A  valid `json` file with the following `properties`. Additional properites may be defined and are assumed to have the safe definition of OpenStreetMap if they have overlapping property names.

| **Property** | **Type** |  **Description** |
| --------- | -------- | ---------------- |
| model_link_id |  Any |  Required. PrimaryKey. |
| shape_id | Any | Optional foreign key to `shapes`. |
| A | int | Required. Foreign key to Nodes. From node. |
| B | int | Required. Foreign key to Nodes. To node. |
| name | str | Required. Name of link. |
| distance | float | Required. Length in miles. |
| lanes | int | Required. Number of GP lanes. |
| ML_lanes | int | Number of managed lanes. |
| drive_access | bool | Required. Allows driving access. |
| bus_only | bool | Required. Bus Only facility |
| rail_only | bool | Required. Bus Only facility |
| bike_access | bool | Required. Bikes may use facility |
| walk_access | bool | Required. Pedestrians may use facility |
| osm_link_id | str | Reference to open street map node id. Used for querying. Not guaranteed to be unique. |
| shstReferenceId | str | Shared Streets ID reference. Used for querying |

### Shapes

A  valid `geojson` file with `LineString` features and the folowing `properties`. Additional properties may be defined and if overlapping with [SharedStreets](http://sharedstreets.io) are assumed to be defined in the same manner.

| **Property** | **Type** |  **Description** |
| --------- | -------- | ---------------- |
| shape_id |  Any |  Required. PrimaryKey. |

## Transit Network Format

Transit Networks must use the the [GTFS](https://www.gtfs.org) Schedule format with the following additional constraints:

1. At this time, only frequency-based schedules are supported with basic project cards. Changes to schedule-based schedules can be made using python code in a `.wrangler` file.

2. Each stop must be associated a node in the RoadwayNetwork by either its `stop_id`, or by adding a field to `stops.txt` with an additional field which references the RoadwayNetwork.  The default is `model_node_id`.

## Project Cards

Project Cards, which define changes to the roadway and transit networks must use the [ProjectCard](https://github.com/networkwrangler/projectcard) standard.
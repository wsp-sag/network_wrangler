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

A  valid `json` file with the following `properties`. Additional properites may be defined and are assumed to have the same definition of OpenStreetMap if they have overlapping property names.

| **Property** | **Type** |  **Description** |
| --------- | -------- | ---------------- |
| model_link_id |  Any |  Required. PrimaryKey. |
| shape_id | Any | Optional foreign key to `shapes`. |
| A | int | Required. Foreign key to Nodes. From node. |
| B | int | Required. Foreign key to Nodes. To node. |
| name | str | Required. Name of link. |
| roadway | str | Required. Uses [OpenStreetMap.highway](https://wiki.openstreetmap.org/wiki/Key:highway) tag definition. |
| distance | Positive float | Required. Length in miles. |
| managed | [ 0,1] | Indicator for managed lane presence.|s
| lanes | Positive int | Required. Number of GP lanes. |
| ML_lanes | Positive int | Number of managed lanes.|
| price | Number | Required. Price of GP lanes. |
| ML_price| Number  | Price of managed lanes. |
| drive_access | bool | Required. Allows driving access. |
| bus_only | bool | Required. Bus Only facility |
| rail_only | bool | Required. Bus Only facility |
| bike_access | bool | Required. Bikes may use facility |
| walk_access | bool | Required. Pedestrians may use facility |
| osm_link_id | str | Reference to open street map node id. Used for querying. Not guaranteed to be unique. |
| shstReferenceId | str | Shared Streets ID reference. Used for querying |

The following above properties can be time-dependent or time- and category-dependent:

- lanes
- ML_lanes
- price
- ML_price

!!! tip

    There is no limit on other, user-defined properties being listed as time-dependent or time- and category-dependent.

!!! example "User-defined variable by time of day"

    Define a variable `access` to represent which categories can access the network and vary it by time of day.

    ```python
    #access
    {
        'default': ('any'),
        'timeofday': [
            {
                'time':('06:00':'09:00'),
                'value': ('no-trucks')
            },
            {
                'time':('16:00':'19:00'),
                'value': ('hov2','hov3','trucks')
            }
        ]
    }
    ```

#### Defining time-dependent properties

- Time-dependent properties are defined as a list of dictionaries with time-periods and values.
- Time periods must be defined as a tuple of HH:MM or HH:MM:SS using a 24-hour clock: `('06:00':'09:00')`.
- Time periods must not intersect.
- When defining a property for a specific time period, a `default` value must be provided and will apply when a time period is not in effect.

!!! example  "Time-dependent property"

    $3 peak-period pricing

    ```python
    # price
    {
        'default': 0,
        'timeofday': [
            {
                'time':('06:00':'09:00'),
                'value': 3
            },
            {
                'time':('16:00':'19:00'),
                'value': 3,
            }
        ]
    }
    ```

#### Defining category-dependent properties

- Not currently implemented.

#### Defining time- and category-dependent properties

- Properties co-dependent on time- and category are defined as a list of dictionaries with value, category and time defined.
- Similar to the above, a `default` value must be provided and will be understood to apply when a category is not matched.

!!! example "time- and category-dependent property"

    A pricing strategy which only applies in peak period for trucks and sovs:

    ```python
    # price
    {
        'default': 0,
        'timeofday': [
            {
                'time':('06:00':'09:00'),
                'category': ('sov','truck'),
                'value': 3
            },
            {
                'time':('16:00':'19:00'),
                'category': ('sov','truck'),
                'value': 3,
            }
        ]
    }
    ```

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

## Model Roadway Network Export Format

In order to separatly calculate the delay when the networks are assigned in static roadway network assignment the roadway network must be exported with separate managed lane links.

To acheive this, RoadwayNetwork objects have the option to be exported in the ModelRoadwayNetwork format, which separates a link into two: set of managed lanes and a set of general purpose lanes which are connected by a set of dummy connector links.  

The parallel managed lanes are defined by nodes which are offset from the parent by the `MANAGED_LANES_NODE_ID_SCALAR` and a primary link id which is offset from teh parent by `MANAGED_LANES_LINK_ID_SCALAR`.

Remaining properties which are not defined by a preceeding `ML` are copied from the parent link.

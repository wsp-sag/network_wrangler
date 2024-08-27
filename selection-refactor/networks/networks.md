# Networks

## Roadway Network Format

RoadwayNetworks must be defined in the following format, which leverages [Open Street Map](http://openstreetmap.org) which uses a tag-based format (e.g. json, xml).  This may be changed in the future to align with the tabular-based [General Modeling Network Specification](https://github.com/zephyr-data-specs/GMNS).

A network is defined by a set of nodes and links which connect them.  Shapes may be optionally specified for each link in a separate file.

### Roadway Validation

RoadwayNetworks can be validated using the following tools:

=== "CLI"

    ```bash
    python validate_roadway.py <network_directory> <network_suffix> [-s] [--output_dir <output_dir>]
    ```
    Where:

    - `network_directory`: The roadway network file directory.
    - `network_suffix`: The suffices of roadway network file name.
    - `-s`, `--strict`: Validate the roadway network strictly without parsing and filling in data.
    -  `--output_dir`: The output directory for the validation report.

=== "Python API"

    ```python
    from network_wrangler.roadway.validate import validate_roadway_in_dir
    validate_roadway_in_dir(
        directory=<network_directory>,
        suffix=<network_suffix>,
        strict=<strict_bool>>,
        output_dir=<output_dir>
    )
    ```
    Where:

    - `network_directory`: The roadway network file directory.
    - `network_suffix`: The suffices of roadway network file name.
    - `strict`: Validate the roadway network strictly without parsing and filling in data.
    -  `output_dir`: The output directory for the validation report.

### Examples

Network Wrangler is packaged with two examples located in the `/examples` directory:

- St Paul, MN
- Small which is a several block exerpt of St Paul and is infinitely easier to trouble-shoot quickly.

### Nodes

A  valid `geojson` file with `point` features and the folowing `properties`.

!!! note "alternative file serializations"

    Networks can also be stored – more efficiently – in parquet files with a similar structure.
    Other workable file serializations include shapefiles, csvs, and anything that can be read
    by pandas or geopandas.

| **Property** | **Type** |  **Description** |
| --------- | -------- | ---------------- |
| model_node_id | Any |  PrimaryKey. Most modeling assignment software requires this be an integer. |
| osm_node_id | str | Reference to open street map node id. Used for querying. Not guaranteed to be unique. |

::: network_wrangler.models.roadway.tables.RoadNodesTable

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
| distance | Positive float | Required, or will be calculated as a crow-fly distance from A to B node. Length in miles. |
| managed | [ 0,1] | Indicator for managed lane presence.  Does not need to be edited in project cards as any project card applied that adds a value for a property with the prefix "ML_" will automatically set this. |
| lanes | Positive int | Required. Number of GP lanes. |
| ML_access_point | bool | Point where managed lane may be accessed from general purpose lanes. |
| ML_egress_point | bool | Point where managed lane may be exited to general purpose lanes. |
| ML_lanes | Positive int | Number of managed lanes.|
| sc_lanes | Positive int | Required. Number of GP lanes scoped by time of day and/or category. |
| sc_ML_lanes | Positive int | Number of managed lanes scoped by time of day and/or category.|
| price | Number | Required. Price of GP lanes . |
| ML_price| Number  | Price of managed lanes. |
| sc_price | Number | Required. Price of GP lanes scoped by time of day and/or category. |
| sc_ML_price | Number  | Price of managed lanes scoped by time of day and/or category. |
| drive_access | bool | Required. Allows driving access. |
| bus_only | bool | Required. Bus Only facility |
| rail_only | bool | Required. Bus Only facility |
| bike_access | bool | Required. Bikes may use facility |
| walk_access | bool | Required. Pedestrians may use facility |
| truck_access | bool | Required. Trucks may use this facility |
| osm_link_id | str | Reference to open street map node id. Used for querying. Not guaranteed to be unique. |
| shstReferenceId | str | Shared Streets ID reference. Not used currently in network wrangler. |

::: network_wrangler.models.roadway.tables.RoadLinksTable

#### Prefix for Managed Lanes

(Almost) any property, including an ad-hoc one, can be made to apply to a parallel managed lane by applying the prefix `ML_`.  

Example: `ML_lanes`

A handful of properties should **not** be assigned an `ML_` prefix by the user because they are assigned one within networkwrangler:

- `name`
- `A`
- `B`
- `model_link_id`

#### Scoped Properties

The following above properties can be time-dependent, category-dependent, or both by adding `sc_`.
The "plain" property without the prefix becomes the default when no scoped property applies.

- lanes: default # of lanes for general purpose lanes
- ML_lanes: default # of lanes for a managed lane
- sc_lanes: time- and/or category-dependent # of lanes for general purpose lanes
- sc_ML_lanes: time- and/or category-dependent # of lanes for a managed lane
- price: default price for general purpose lanes
- ML_price: default price for a managed lane
- sc_price: time- and/or category-dependent price for general purpose lanes
- sc_ML_price: time- and/or category-dependent price for a managed lane

!!! note "previous format for scoped properties"

    Some previous tooling was developed around a previous method for serializing scoped properties.  In order to retain compatability with this format:

    - `load_roadway_from_dir()`, `read_links()`, and associated functions will "sniff" the network for the old format and apply the converter function `translate_links_df_v0_to_v1()`
    - `write_links()` has an boolean attribute to `convert_complex_properties_to_single_field` which can also be invoked from `write_roadway()` as `convert_complex_link_properties_to_single_field`.

#### Defining time-dependent properties

- Time-dependent properties are defined as a list of dictionaries with timespans and values.
- Timespans must be defined as a list of HH:MM or HH:MM:SS using a 24-hour clock: `('06:00':'09:00')`.
- Timespans must not intersect.

!!! example  "Time-dependent property"

    $3 peak-period pricing

    ```python
    # default price
    'price' = 0
    'sc_price':
    [
        {
            'time':['06:00':'09:00'],
            'value': 3
        },
        {
            'timespan':['16:00':'19:00'],
            'value': 3,
        }
    ]
    ```

#### Defining time- and category-dependent properties

- Properties co-dependent on time- and category are defined as a list of dictionaries with value, category and time defined.

!!! example "time- and category-dependent property"

    A pricing strategy which only applies in peak period for trucks and sovs:

    ```python
    # default price
    "price": 0
    # price scoped by time of day
    "sc_price":
    [
        {
            'timespan':['06:00':'09:00'],
            'category': ('sov','truck'),
            'value': 3
        },
        {
            'timespan':['16:00':'19:00'],
            'category': ('sov','truck'),
            'value': 3,
        }
    ]
    ```

!!! tip

    There is no limit on other, user-defined properties being listed as time-dependent or time- and category-dependent.

!!! example "User-defined variable by time of day"

    Define a variable `access` to represent which categories can access the network and vary it by time of day.

    ```python
    #access
    {
        # default value for access
        'access': ('any'),
        # scoped value for access
        'sc_access': [
            {
                'timespan':['06:00':'09:00'],
                'value': ('no-trucks')
            },
            {
                'timespan':['16:00':'19:00'],
                'value': ('hov2','hov3','trucks')
            }
        ]
    }
    ```

### Shapes

A  valid `geojson` file with `LineString` features and the folowing `properties`. Additional properties may be defined and if overlapping with [SharedStreets](http://sharedstreets.io) are assumed to be defined in the same manner.

| **Property** | **Type** |  **Description** |
| --------- | -------- | ---------------- |
| shape_id |  Any |  Required. PrimaryKey. |

::: network_wrangler.models.roadway.tables.RoadShapesTable

## Transit Network Format

Transit Networks must use the the [GTFS](https://www.gtfs.org) Schedule format with the following additional constraints:

1. At this time, only frequency-based schedules are supported.
2. Each `stop_id` must be a node in the RoadwayNetwork.
3. `shapes.txt` is *required* (it is optional in GTFS) and must have the added field `model_node_id` associating a specific location with a node on the `RoadwayNetwork`.

### Transit Validation

TransitNetworks can be validated using the following tools:

=== "CLI"

    ```bash
    python validate_transit.py <network_directory> <network_suffix> [-s] [--output_dir <output_dir>] [--road_dir <road_dir>] [--road_suffix <road_suffix>]
    ```
    Where:

    - `network_directory`: The roadway network file directory.
    - `network_suffix`: The suffices of roadway network file name.
    - `--output_dir`: The output directory for the validation report.
    - `--road_dir`: The directory roadway network. if want to validate the transit network to it.
    - `--road_suffix`: The suffix for roadway network. Defaults to 'geojson'.

=== "Python API"

    ```python
    from network_wrangler.transit.validate import validate_transit_in_dir
    validate_transit_in_dir(
        directory=<network_directory>,
        suffix=<network_suffix>,
        road_dir=<road_dir>,
        road_suffix=<road_suffix>,
    )
    ```
    Where:

    - `network_directory`: The roadway network file directory.
    - `network_suffix`: The suffices of roadway network file name.
    - `road_dir`: The directory roadway network. if want to validate the transit network to it.
    - `road_suffix`: The suffix for roadway network. Defaults to 'geojson'.

## Project Cards

Project Cards, which define changes to the roadway and transit networks must use the [ProjectCard](https://github.com/networkwrangler/projectcard) standard.

## Model Roadway Network Export Format

In order to separately calculate the delay when the networks are assigned in static roadway network assignment the roadway network must be exported with separate managed lane links.

To acheive this, RoadwayNetwork objects have the option to be exported in the ModelRoadwayNetwork format, which separates a link into two: set of managed lanes and a set of general purpose lanes which are connected by a set of dummy connector links.  

### Managed Lane Links

All properties preceded by `ML_` will be copied, without that prefix, to the managed lane links.

Geometry of these managed lanes will be defined as a shape offset by the parameter ML_OFFSET_METERS

::: network_wrangler.params.ML_OFFSET_METERS

Properties defined in the parameter `COPY_FROM_GP_TO_ML` are also copied from the parent link.

::: network_wrangler.params.COPY_FROM_GP_TO_ML

New `model_node_id` s and `model_link_ids` are generated based on the following ranges

::: network_wrangler.params.MANAGED_LANES_LINK_ID_RANGE
::: network_wrangler.params.MANAGED_LANES_NODE_ID_RANGE

`name` is created as "managed lane of `name of GP link`"

Relationship to the general purpose lanes is retained using the fields `GP_A`, `GP_B`, `GP_model_link_id`.

Managed-lane link-ids are generated as multiples of 10.

### Dummy Connector Links

Dummy connector links are generated between the general purpose lane links and managed lane links at points defined by the variable `ML_access_point` and `ML_egress_point`.  If a managed lane is created without explictly setting these values, network wrangler will assume that the managed lanes can be accessed at any node.

The parameter  `COPY_TO_ACCESS_EGRESS` defines what additional attributes are copied from the general purpose lane to the access and egress links.

::: network_wrangler.params.COPY_TO_ACCESS_EGRESS

`name` is created as "<access/egress> dummy link"

`model_link_id` is created as follows, noting that `model_link_id` s for managed lanes will be multiples of 10:

- 1 + managed lane's `model_link_id` for access links
- 2 + managed lane's `model_link_id` for access links

## Network Management

Several functions assist in managing networks themselves including converting serialization formats and clipping to various geographic bounds.

### Viewing

Because the roadway network is already in GeoDataFrames, they can be easily viewed in Jupyter Notebooks using commands such as:

```python
my_small_net = SMALL_ROAD.links_df).explore(tiles="CartoDB positron")
SMALL_ROAD.nodes_df.explore(m=my_small_net, color="grey")
my_small_net
```

For larger networks, you might want to sample objects if you are just trying to get the general picture in order to save memory.

```python
stpaul_net = STPAUL_ROAD.links_df.sample(300).explore(tiles="CartoDB positron")
STPAUL_ROAD.nodes_df.sample(300).explore(m=stpaul_net, color="grey")
stpaul_net
```

For transit, you have access to GeoDataFrames for both shapes and stops:

```python
STPAUL_TRANSIT.shapes_gdf.explore(m=stpaul_net, color="limegreen")
STPAUL_TRANSIT.stops_gdf.explore(m=stpaul_net, color="limegreen")
stpaul_net
```

### Clipping

There are two options for getting networks that are subsets of the larger one:

1. clipping networks already loaded from files. Useful when you are already manipulating the objects.
2. filtering the network when it is read in.

In both instances, you have the option to filter based on one of three methods:

1. `boundary_file` which is a geojson or shapefile that you want to filter to.
2. `boundary_gdf` passing a geodataframe with a single polygon record that you want to filter to.
3. `boundary_geocode` which queries open streetmap for a jurisdiction matching the provided name, e.g. "St Paul, MN, USA".

Transit additionally has the option to be filtered to a roadway network.

#### Clipping loaded networks

If your network is already loaded from disk into a RoadwayNetwork or TransitNetwork, you can clip it using `clip_roadway()` or `clip_transit()`:

```python
from network_wrangler.roadway import clip_roadway
clipped_road_eco = clip_roadway(STPAUL_ROAD, boundary_file=TEST_DATA / "ecolab.geojson")
clipped_road_eco.links_df.explore(m= color="hotpink")
```

```python
from network_wrangler.transit import clip_transit
clipped_transit_eco = clip_transit(STPAUL_TRANSIT, roadway_net=clipped_road_eco)
```

#### Filtering on being read from disk

To filter roadway networks on being read-in, you can use the same parameters (`boundary_gdf`, `boundary_geocode` or `boundary_file`, available in `load_roadway_from_dir`, and all associated methods.  

```python
downtown_net = load_roadway_from_dir(STPAUL_DIR, boundary_geocode="Downtown, St Paul, MN, USA")
```

This feature is not yet implemented in transit.

### Converting Serialization

To convert file serialization without reading it into objects, you can use the method `convert_roadway_file_serialization()`:

```python
from roadway.io import convert_roadway_file_serialization

convert_roadway_file_serialization(
    my_dir, # the path to the input directory.
    "geojson", # the suffix of the input files. Defaults to "geojson".
    my_new_dir, # the path were the output will be saved.
    "parquet", # the format of the output files. Defaults to "parquet".
    "new", # the name prefix of the roadway files that will be generated. Defaults to "".
    overwrite = True, # if True, will overwrite the files if they already exist. Defaults to True.
)
```

!!! Note "clip too"

    Note you can also pass one of `boundary_geocode`, `boundary_gdf` or `boundary_file` to clip while you are converting the file serialization.

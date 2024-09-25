# How To

## Build a Scenario from a Scenario Configuration File

You can build a scenario and write out the output from a scenario configuration file using the code below.  This is very useful when you are running a specific scenario with minor variations over again because you can enter your config file into version control.  In addition to the completed roadway and transit files, the output will provide a record of how the scenario was run.

```python
    from scenario import build_scenario_from_config
    my_scenario = build_scenario_from_config(my_scenario_config)
```

Where `my_scenario_config` can be a:

- Path to a scenario config file in yaml/toml/json (recommended),
- Dictionary which is in the same structure of a scenario config file, or
- A `ScenarioConfig()`  instance.

??? example "Example scenario config file"

    ```yaml
    name: "my_scenario"
    wrangler_config: "path/to/wrangler_config.yaml"
    base_scenario:
        roadway:
            dir: "path/to/roadway_network"
            suffix: "geojson"
            read_in_shapes: True
        transit:
            dir: "path/to/transit_network"
            suffix: "txt"
        applied_projects:
            - "project1"
            - "project2"
        conflicts:
            - "project3"
            - "project4"
    projects:
        project_card_filepath:
            - "path/to/projectA.yaml"
            - "path/to/projectB.yaml"
        filter_tags:
            - "tag1"
    output_scenario:
        overwrite: True
        path: "path/to/my/output"
        roadway:
            out_dir: "./roadway"
            prefix: "my_scenario"
            file_format: "geojson"
            true_shape: False
        transit:
            out_dir: "path/to/output/transit"
            prefix: "my_scenario"
            file_format: "txt"
        project_cards:
            out_dir: "path/to/output/project_cards"
    ```

!!! tip "Notes on relative paths in scenario configs"

    - Relative paths are recognized by a preceeding ".".
    - Relative paths within `output_scenario` for `roadway`, `transit`, and `project_cards` are interpreted to be relative to `output_scenario.path`.
    - All other relative paths are interpreted to be relative to directory of the scenario config file. (Or if scenario config is provided as a dictionary, relative paths will be interpreted as relative to the current working directory.)

::: network_wrangler.configs.scenario

## Change Wrangler Configuration

Users can change a handful of parameters which control the way Wrangler runs.  These parameters can be saved as a wrangler config file which can be read in repeatedly to make sure the same parameters are used each time.

At runtime, you can specify configurable parameters at the scenario level which will then also be assigned and accessible to the roadway and transit networks.

```python
create_scenario(...config = myconfig)
```

Or if you are not using Scenario functionality, you can specify the config when you read in a RoadwayNetwork.

```python
load_roadway_from_dir(**roadway, config=myconfig)
load_transit(**transit, config=myconfig)
```

`my_config` can be a:

- Path to a config file in yaml/toml/json (recommended),
- List of paths to config files (in case you want to split up various sub-configurations)
- Dictionary which is in the same structure of a config file, or
- A `WranglerConfig()`  instance.

If not provided, Wrangler will use reasonable defaults.

??? example "Example wrangler file, showing the default values."

    ```yaml

    IDS:
        TRANSIT_SHAPE_ID_METHOD: scalar
        TRANSIT_SHAPE_ID_SCALAR: 1000000
        ROAD_SHAPE_ID_METHOD: scalar
        ROAD_SHAPE_ID_SCALAR: 1000
        ML_LINK_ID_METHOD: "range"
        ML_LINK_ID_RANGE: (950000, 999999)
        ML_LINK_ID_SCALAR: 15000
        ML_NODE_ID_METHOD: "range"
        ML_NODE_ID_RANGE: (950000, 999999)
        ML_NODE_ID_SCALAR: 15000
    MODEL_ROADWAY:
        ML_OFFSET_METERS: int = -10
        ADDITIONAL_COPY_FROM_GP_TO_ML: []
        ADDITIONAL_COPY_TO_ACCESS_EGRESS: []
    CPU:
        EST_PD_READ_SPEED:
            csv: 0.03
            parquet: 0.005
            geojson: 0.03
            json: 0.15
            txt: 0.04
    ```

::: network_wrangler.configs.wrangler

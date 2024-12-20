# How To

## Build a Scenario using API

::: network_wrangler.scenario
    options:
        members: []
        heading_level: 3
    handlers:
      python:
        options:
          show_root_toc_entry: false

!!! tip "additional examples"

    You can see additional scenario creating capabilities in the example jupyter notebook `Scenario Building Example.ipynb`.

## Build a Scenario from a Scenario Configuration File

::: network_wrangler.configs.scenario
    options:
        members: []
        heading_level: 3
    handlers:
      python:
        options:
          show_root_toc_entry: false

## Change Wrangler Configuration

::: network_wrangler.configs.wrangler
    options:
        heading_level: 3
    handlers:
      python:
        options:
          show_root_toc_entry: false

## Review changes beetween networks

!!! example "Review Added Managed Lanes"

    ```python
    from network_wrangler import load_roadway_from_dir
    from projectcard import read_card
    from pathlib import Path

    EXAMPLE_DIR = Path.cwd().parent / "examples"
    STPAUL = EXAMPLE_DIR / "stpaul"
    STPAUL_ROAD = load_roadway_from_dir(STPAUL)

    card_path = STPAUL / "project_cards" / "road.prop_change.managed_lanes.yml"
    card = read_card(card_path)
    stpaul_build = STPAUL_ROAD.apply(card)

    ml_map = STPAUL_ROAD.links_df[STPAUL_ROAD.links_df.managed > 0].explore(
        color="blue",
        tiles="CartoDB positron",
        name="Managed Lanes",
        style_kwds={"opacity": 0.6, "weight": 20}
    )

    added_managed_lanes = stpaul_build.links_df[(stpaul_build.links_df.managed > 0) & (STPAUL_ROAD.links_df.managed == 0)]

    added_managed_lanes.explore(
        m=ml_map,
        color="red",
        name="Added Managed Lanes",
        style_kwds={"opacity": 0.6, "weight": 20}
    )
    ```

!!! tip "additional examples"
    You can see additional scenario review capabilities in the example jupyter notebook `Visual Checks.ipynb`.

## Review selected facilities

!!! example "Review selected links"

    ```python
    from network_wrangler import load_roadway_from_dir
    from pathlib import Path

    EXAMPLE_DIR = Path.cwd().parent / "examples"
    STPAUL = EXAMPLE_DIR / "stpaul"

    STPAUL_ROAD = load_roadway_from_dir(STPAUL)
    sel_dict = {
      "links": {
          "modes": ["walk"],
          "name": ["Valley Street"],
      },
      "from": {"model_node_id": 174762},
      "to": {"model_node_id": 43041},
    }
    STPAUL_ROAD.get_selection(sel_dict).selected_links_df.explore(
      color="red", style_kwds={"opacity": 0.6, "weight": 20}
    )
    ```

!!! tip "additional examples"

    You can see additional interactive exploration of how selections work and how to review them in the Jupyter notebook `Roadway Network Search.ipynb`.

## Create your own example data from Open Street Map

::: network_wrangler.bin.build_basic_osm_roadnet
    options:
        show_bases: false
        show_root_toc_entry: false
        heading_level: 3
        show_source: false
        members: false

!!! tip "additional examples"

    You can review the process in this script step-wise and interactively create your own networks from OSM with variation in the underlying assumptions in the Jupyter notebook `Create Network from OSM.ipynb`.

## Review separated model network managed lanes

!!! example "Review model network"

    ```python
    m_net = stpaul_build.model_net
    model_net_map = m_net.gp_links_df.explore(
        tiles="CartoDB positron",
        color="blue",
        style_kwds={"opacity": 0.6, "weight": 10}
    )
    m_net.ml_links_df.explore(m=model_net_map, color="red", style_kwds={"opacity": 0.6, "weight": 10})
    m_net.dummy_links_df.explore(m=model_net_map, color="green", style_kwds={"opacity": 0.6, "weight": 10})
    ```

!!! tip "additional examples"

    You can learn more about visualization of networks in the Jupyter notebook `Network Viewer.ipynb`.

{!
  include-markdown("https://raw.githubusercontent.com/network-wrangler/projectcard/refs/heads/main/docs/how-to.md")
!}

## How to make co-dependent roadway and transit networks routing changes

When deleting roadways breaks existing transit routes, users must fix the route shapes as part of the roadway deletion project card. This involves a sequence of steps: first, adding new roadways to fix the broken shapes, then updating the affected transit routes, and finally, proceeding with the roadway deletion.

!!! example "make co-dependent roadway and transit networks routing changes"

    ```python
    project: add and delete roadway for transit
    tags: []
    dependencies: {}
    changes:
    - roadway_addition:
        links:
        - A: 53282
          B: 354063
          model_link_id: 900000
          lanes: 1
          name: Smith Avenue South
          drive_access: 1
          walk_access: 1
          bike_access: 1
          bus_only: 0
          rail_only: 0
          roadway: primary
        - A: 354063
          B: 53282
          model_link_id: 900001
          lanes: 1
          name: Smith Avenue South
          drive_access: 1
          walk_access: 1
          bike_access: 1
          bus_only: 0
          rail_only: 0
          roadway: primary
        nodes:
        - model_node_id: 354063
          X: -93.1016
          Y: 44.9222
    - transit_routing_change:
        service:
          trip_properties:
            route_id:
            - 417-111
            direction_id: 0
        routing:
          existing:
          - -57637
          - -53282
          - -57636
          set:
          - -57637
          - -53282
          - -354063
          - -57636
    - roadway_deletion:
        links:
          model_link_id:
            - 394014
            - 394829
          modes:
            - "any"
          ignore_missing: False
    ```

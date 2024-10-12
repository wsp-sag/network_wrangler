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

{!
  include-markdown("https://raw.githubusercontent.com/network-wrangler/projectcard/refs/heads/main/docs/how-to.md")

!}

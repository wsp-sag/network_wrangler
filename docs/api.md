# API Documentation

## Common Usage

## Base Objects

::: network_wrangler.scenario
    options:
        heading_level: 3
::: network_wrangler.roadway.network
    options:
        heading_level: 3
::: network_wrangler.transit.network
    options:
        heading_level: 3

## Configs

::: network_wrangler.configs.wrangler
    options:
        heading_level: 3

::: network_wrangler.configs.scenario
    options:
        heading_level: 3

## Projects

Projects are how you manipulate the networks. Each project type is defined in a module in the `projects` folder and accepts a RoadwayNetwork and or TransitNetwork as an input and returns the same objects (manipulated) as an output.  

## Roadway

The roadway module contains submodules which define and extend the links, nodes, and shapes dataframe objects which within a RoadwayNetwork object as well as other classes and methods which support and extend the RoadwayNetwork class.

### Roadway Network Objects

Submodules which define and extend the links, nodes, and shapes dataframe objects which within a RoadwayNetwork object.  Includes classes which define:

- dataframe schemas to be used for dataframe validation using `pandera`
- methods which extend the dataframes

#### Roadway Links

:: network_wrangler.roadway.links.io
    options:
        heading_level: 5
:: network_wrangler.roadway.links.create
    options:
        heading_level: 5
:: network_wrangler.roadway.links.delete
    options:
        heading_level: 5
:: network_wrangler.roadway.links.edit
    options:
        heading_level: 5
:: network_wrangler.roadway.links.filters
    options:
        heading_level: 5
:: network_wrangler.roadway.links.geo
    options:
        heading_level: 5
:: network_wrangler.roadway.links.scopes
    options:
        heading_level: 5
:: network_wrangler.roadway.links.summary
    options:
        heading_level: 5
:: network_wrangler.roadway.links.validate
    options:
        heading_level: 5
:: network_wrangler.roadway.links.df_accessors
    options:
        heading_level: 5

#### Roadway Nodes

:: network_wrangler.roadway.nodes.io
    options:
        heading_level: 5
:: network_wrangler.roadway.nodes.create
    options:
        heading_level: 5
:: network_wrangler.roadway.nodes.delete
    options:
        heading_level: 5
:: network_wrangler.roadway.nodes.edit
    options:
        heading_level: 5
:: network_wrangler.roadway.nodes.filters
    options:
        heading_level: 5
:: network_wrangler.roadway.nodes
    options:
        heading_level: 5

#### Roadway Shapes

:: network_wrangler.roadway.shapes.io
    options:
        heading_level: 5
:: network_wrangler.roadway.shapes.create
    options:
        heading_level: 5
:: network_wrangler.roadway.shapes.edit
    options:
        heading_level: 5
:: network_wrangler.roadway.shapes.delete
    options:
        heading_level: 5
:: network_wrangler.roadway.shapes.filters
    options:
        heading_level: 5
:: network_wrangler.roadway.shapes.shapes
    options:
        heading_level: 5

### Roadway Projects

:: network_wrangler.roadway.projects.add
    options:
        heading_level: 4
:: network_wrangler.roadway.projects.calculate
    options:
        heading_level: 4
:: network_wrangler.roadway.projects.delete
    options:
        heading_level: 4
:: network_wrangler.roadway.projects.edit_property
    options:
        heading_level: 4

### Roadway Supporting Modules

:: network_wrangler.roadway.io
    options:
        heading_level: 4
:: network_wrangler.roadway.clip
    options:
        heading_level: 4
:: network_wrangler.roadway.model_roadway
    options:
        heading_level: 4
:: network_wrangler.roadway.utils
    options:
        heading_level: 4
:: network_wrangler.roadway.validate
    options:
        heading_level: 4
:: network_wrangler.roadway.segment
    options:
        heading_level: 4
:: network_wrangler.roadway.subnet
    options:
        heading_level: 4
:: network_wrangler.roadway.graph
    options:
        heading_level: 4

## Transit

### Feed

::: network_wrangler.transit.feed.feed
    options:
        heading_level: 4
::: network_wrangler.transit.feed.frequencies
    options:
        heading_level: 4
::: network_wrangler.transit.feed.routes
    options:
        heading_level: 4
::: network_wrangler.transit.feed.shapes
    options:
        heading_level: 4
::: network_wrangler.transit.feed.stop_times
    options:
        heading_level: 4
::: network_wrangler.transit.feed.stops
    options:
        heading_level: 4
::: network_wrangler.transit.feed.trips
    options:
        heading_level: 4
::: network_wrangler.transit.feed.transit_links
    options:
        heading_level: 4
::: network_wrangler.transit.feed.transit_segments
    options:
        heading_level: 4

### Transit Projects

::: network_wrangler.transit.projects.add_route
    options:
        heading_level: 4
::: network_wrangler.transit.projects.calculate
    options:
        heading_level: 4
::: network_wrangler.transit.projects.delete_service
    options:
        heading_level: 4
::: network_wrangler.transit.projects.edit_property
    options:
        heading_level: 4
::: network_wrangler.transit.projects.edit_routing
    options:
        heading_level: 4

### Transit Helper Modules

::: network_wrangler.transit.clip
    options:
        heading_level: 4
::: network_wrangler.transit.geo
    options:
        heading_level: 4
::: network_wrangler.transit.io
    options:
        heading_level: 4
::: network_wrangler.transit.model_transit
    options:
        heading_level: 4
::: network_wrangler.transit.selection
    options:
        heading_level: 4
::: network_wrangler.transit.validate
    options:
        heading_level: 4

## Utils and Functions

::: network_wrangler.utils.utils
    options:
        heading_level: 3
::: network_wrangler.utils.io_table
    options:
        heading_level: 3
::: network_wrangler.utils.io_dict
    options:
        heading_level: 3
::: network_wrangler.utils.models
    options:
        heading_level: 3
::: network_wrangler.utils.net
    options:
        heading_level: 3
::: network_wrangler.utils.time
    options:
        heading_level: 3
::: network_wrangler.utils.data
    options:
        heading_level: 3
::: network_wrangler.utils.geo
    options:
        heading_level: 3
::: network_wrangler.utils.df_accessors
    options:
        heading_level: 3
::: network_wrangler.logger
    options:
        heading_level: 3
::: network_wrangler.configs.utils
    options:
        heading_level: 3
::: network_wrangler.time
    options:
        heading_level: 3
::: network_wrangler.viz
    options:
        heading_level: 3
::: network_wrangler.errors
    options:
        heading_level: 3

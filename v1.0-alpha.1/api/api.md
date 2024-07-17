# API Documentation

## Common Usage

## Base Objects

::: network_wrangler.scenario
::: network_wrangler.roadway.network
::: network_wrangler.transit.network

## Parameters

::: network_wrangler.params

## Projects

Projects are how you manipulate the networks. Each project type is defined in a module in the `projects` folder and accepts a RoadwayNetwork and or TransitNetwork as an input and returns the same objects (manipulated) as an output.  

## Roadway

The roadway module contains submodules which define and extend the links, nodes, and shapes dataframe objects which within a RoadwayNetwork object as well as other classes and methods which support and extend the RoadwayNetwork class.

### Network Objects

Submodules which define and extend the links, nodes, and shapes dataframe objects which within a RoadwayNetwork object.  Includes classes which define:

- dataframe schemas to be used for dataframe validation using `pandera`
- methods which extend the dataframes

#### Links

:: network_wrangler.roadway.links.io
:: network_wrangler.roadway.links.create
:: network_wrangler.roadway.links.delete
:: network_wrangler.roadway.links.edit
:: network_wrangler.roadway.links.filters
:: network_wrangler.roadway.links.geo
:: network_wrangler.roadway.links.scopes
:: network_wrangler.roadway.links.summary
:: network_wrangler.roadway.links.validate
:: network_wrangler.roadway.links.df_accessors

#### Nodes

:: network_wrangler.roadway.nodes.io
:: network_wrangler.roadway.nodes.create
:: network_wrangler.roadway.nodes.delete
:: network_wrangler.roadway.nodes.edit
:: network_wrangler.roadway.nodes.filters
:: network_wrangler.roadway.nodes
#### Shapes

:: network_wrangler.roadway.shapes.io
:: network_wrangler.roadway.shapes.create
:: network_wrangler.roadway.shapes.edit
:: network_wrangler.roadway.shapes.delete
:: network_wrangler.roadway.shapes.filters
:: network_wrangler.roadway.shapes.shapes

### Supporting Classes, Methods + Parameters

:: network_wrangler.roadway.segment
:: network_wrangler.roadway.subnet
:: network_wrangler.roadway.graph

## Utils and Functions

::: network_wrangler.utils.utils
::: network_wrangler.utils.io
::: network_wrangler.utils.models
::: network_wrangler.utils.net
::: network_wrangler.utils.time
::: network_wrangler.utils.data
::: network_wrangler.utils.geo
::: network_wrangler.utils.df_accessors
::: network_wrangler.logger

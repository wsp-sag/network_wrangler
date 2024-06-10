---
hide:
 - navigation
---
# API Documentation

## Base Classes

::: network_wrangler.Scenario
::: network_wrangler.RoadwayNetwork
::: network_wrangler.TransitNetwork

## Projects

Projects are how you manipulate the networks. Each project type is defined in a module in the `projects` folder and accepts a RoadwayNetwork and or TransitNetwork as an input and returns the same objects (manipulated) as an output.  

:: network_wrangler.projects

## Roadway

The roadway module contains submodules which define and extend the links, nodes, and shapes dataframe objects which within a RoadwayNetwork object as well as other classes and methods which support and extend the RoadwayNetwork class.

### Network Objects

Submodules which define and extend the links, nodes, and shapes dataframe objects which within a RoadwayNetwork object.  Includes classes which define:

- dataframe schemas to be used for dataframe validation using `pandera`
- parameters for defining primary and foreign keys between links, nodes and shapes
- parameters helpful lookups to keep with a dataframe
- methods which extend the dataframes

#### Links

:: network_wrangler.roadway.links.LinksSchema
:: network_wrangler.roadway.links.LinksParams
:: network_wrangler.roadway.links.MODES_TO_NETWORK_LINK_VARIABLES
:: network_wrangler.roadway.links.ModeLinkAccessor

#### Nodes

:: network_wrangler.roadway.links.NodesSchema
:: network_wrangler.roadway.links.NodesParams

#### Shapes

:: network_wrangler.roadway.links.ShapesSchema
:: network_wrangler.roadway.links.ShapesParams

### Supporting Classes, Methods + Parameters

:: network_wrangler.roadway.selection.RoadwayLinkSelection
:: network_wrangler.roadway.selection.RoadwayNodeSelection
:: network_wrangler.roadway.Segment
:: network_wrangler.roadway.DEFAULT_MAX_SEARCH_BREADTH
:: network_wrangler.roadway.DEFAULT_SEARCH_BREADTH
:: network_wrangler.roadway.DEFAULT_SP_WEIGHT_FACTOR
:: network_wrangler.roadway.Subnet
:: network_wrangler.roadway.graph.shortest_path
:: network_wrangler.roadway.graph.links_nodes_to_ox_graph
:: network_wrangler.roadway.graph.net_to_graph
:: network_wrangler.roadway.graph.assess_connectivity

### Visualization

Requires installation of supporting packages in `requirements.viz.txt`

:: network_wrangler.roadway.viz.selection_map
:: network_wrangler.roadway.viz.network_connection_plot

## Utils and Functions

::: network_wrangler.utils.utils
::: network_wrangler.utils.geo
::: network_wrangler.logger

## On-Disk Schemas

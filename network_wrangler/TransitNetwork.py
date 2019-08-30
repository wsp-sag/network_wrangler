#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys

import networkx as nx
import pandas as pd
import partridge as ptg
from partridge.config import geo_config
from partridge.config import default_config
from partridge.gtfs import Feed

from .Logger import WranglerLogger


class TransitNetwork(object):
    """
    Representation of a Transit Network.

    Usage:
      import network_wrangler as wr
      stpaul = r'/home/jovyan/work/example/stpaul'
      tc=wr.TransitNetwork.read(path=stpaul)

    """

    def __init__(self, feed: Feed = None, config: nx.DiGraph = None):
        """
        Constructor
        """
        self.feed: Feed = feed
        self.config: nx.DiGraph = config

        self.selections: dict = {}

    @staticmethod
    def validate_feed(feed: Feed, config: nx.DiGraph) -> Bool:
        """
        Since Partridge lazily loads the df, load each file to make sure it
        actually works.

        Partridge uses a DiGraph from the networkx library to represent the
        relationships between GTFS files. Each file is a 'node', and the
        relationship between files are 'edges'.
        """
        try:
            for node in config.nodes.keys():
                feed.get(node)
            return True
        except AttributeError:
            return False

    @staticmethod
    def read(feed_path: str, fast: bool = False) -> Tuple[nx.DiGraph, Feed]:
        """
        Read GTFS feed from folder and return a config and Partridge Feed object
        """
        config = geo_config()
        config.nodes["shapes.txt"]["required_columns"] = config.nodes["shapes.txt"][
            "required_columns"
        ] + ("A", "B", "LINK_ID")

        try:
            feed = ptg.load_feed(feed_path, config=config)
            TransitNetwork.validate_feed(feed, config)

        except KeyError:
            config = default_config()
            config.nodes["shapes.txt"]["required_columns"] = (
                "shape_id",
                "A",
                "B",
                "LINK_ID",
            )

            WranglerLogger.warning(
                "Reducing data requirements for shapes.txt to:",
                config.nodes["shapes.txt"]["required_columns"],
            )
            feed = ptg.load_feed(feed_path, config=config)
            TransitNetwork.validate_feed(feed, config)

        transit_network = TransitNetwork(feed=feed, config=config)

        return transit_network

    def write(self, outpath: str = ".") -> None:
        """
        Writes a network in the transit network standard

        args:
        outpath: the path were the output files will be saved
        """

        for node in self.config.nodes.keys():
            df = self.feed.get(node)
            if not df.empty:
                path = os.path.join(outpath, node)
                df.to_csv(path, index=False)

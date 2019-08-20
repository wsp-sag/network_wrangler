#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import os, sys

import networkx as nx
import pandas as pd
import partridge as ptg
from partridge.config import geo_config, default_config
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

    def __init__(self, feed_path: str = None):
        """
        Constructor
        """

        self.config, self.feed = TransitNetwork.read_feed(feed_path)

    @staticmethod
    def validate_feed(feed: Feed, config: nx.DiGraph) -> Bool:
        """
        Since Partridge lazily loads the df, load each file to make sure it actually works.
        """
        try:
            for node in config.nodes.keys():
                feed.get(node)
            return True
        except AttributeError:
            return False

    @staticmethod
    def read_feed(feed_path: str = None) -> Tuple[nx.DiGraph, Feed]:
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

        ## todo should be read in as a schema
        WranglerLogger.info(
            "Read %s agencies from %s"
            % (feed.agency.size, os.path.join(feed_path, "agency.txt"))
        )
        WranglerLogger.info(
            "Read %s frequencies from %s"
            % (feed.frequencies.size, os.path.join(feed_path, "frequencies.txt"))
        )
        WranglerLogger.info(
            "Read %s routes from %s"
            % (feed.routes.size, os.path.join(feed_path, "routes.txt"))
        )
        WranglerLogger.info(
            "Read %s shapes from %s"
            % (feed.shapes.size, os.path.join(feed_path, "shapes.txt"))
        )
        WranglerLogger.info(
            "Read %s stops from %s"
            % (feed.stops.size, os.path.join(feed_path, "stops.txt"))
        )
        WranglerLogger.info(
            "Read %s transfers from %s"
            % (feed.transfers.size, os.path.join(feed_path, "transfers.txt"))
        )
        WranglerLogger.info(
            "Read %s trips from %s"
            % (feed.trips.size, os.path.join(feed_path, "transfers.txt"))
        )

        return config, feed

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

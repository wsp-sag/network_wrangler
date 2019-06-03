#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import os, sys
import partridge as ptg

from .Logger import WranglerLogger
from partridge.config import geo_config
from partridge.gtfs import Feed

class TransitNetwork(object):
    '''
    Representation of a Transit Network.

    Usage:
      import network_wrangler as wr
      stpaul = r'/home/jovyan/work/example/stpaul'
      tc=wr.TransitNetwork.read(path=stpaul)

    '''

    def __init__(self, feed: Feed):
        '''
        Constructor
        '''

        if isinstance(feed, Feed):
            self.agency = feed.agency
            self.frequencies = feed.frequencies
            self.routes = feed.routes
            self.shapes = feed.shapes
            self.stops = feed.stops
            self.transfers = feed.transfers
            self.trips = feed.trips
        else:
            WranglerLogger.error("Incompatible feed type. Must provide a partridge Feed object.")
            sys.exit("Incompatible feed type. Must provide a partridge Feed object.")

    @staticmethod
    def read(path: str = '.', agency_file: str = 'agency.txt',
             frequencies_file: str = 'frequencies.txt', routes_file: str = 'routes.txt',
             shapes_file: str = 'shapes.txt', stops_files: str = 'stops.txt',
             transfers_file: str = 'transfers.txt',
             trips_file: str = 'trips.txt') -> TransitNetwork:
        '''
        Reads a network from the transit network standard

        args:
        agency_file: the agencies file
        frequencies_file: the frequencies file
        routes_file: the routes file
        shapes_file: the shapes file
        stops_files: the stops file
        transfers_file: the transfers file
        trips_file: the trips file
        '''

        config = geo_config()
        #using load_feed so we can feed a config to it
        #need to make sure we have data configured per https://github.com/remix/partridge/blob/master/partridge/config.py#L251
        #or need to update the config 
        feed = ptg.load_feed(path, view = None, config = config)


        #TODO: the above seems to be reading trips, routes, frequencies and agencies. Shapes, stops and transfers are not read.
        WranglerLogger.info('Read %s agencies from %s' % (feed.agency.size, agency_file))
        WranglerLogger.info('Read %s frequencies from %s' % (feed.frequencies.size, frequencies_file))
        WranglerLogger.info('Read %s routes from %s' % (feed.routes.size, routes_file))
        #WranglerLogger.info('Read %s shapes from %s' % (feed.shapes.size, shapes_file))
        #WranglerLogger.info('Read %s stops from %s' % (feed.stops.size, stops_files))
        #WranglerLogger.info('Read %s transfers from %s' % (feed.transfers.size, transfers_file))
        WranglerLogger.info('Read %s trips from %s' % (feed.trips.size, trips_file))

        transit_network = TransitNetwork(feed)
        return transit_network



    def write(self, path: str = '.', filename: str = None) -> None:
        '''
        Writes a network in the transit network standard

        args:
        path: the path were the output files will be saved
        filename: the name prefix of the transit files that are generated
        '''

        if not os.path.exists(path):
            WranglerLogger.debug("\nPath [%s] doesn't exist; creating." % path)
            os.mkdir(path)


        file = open(os.path.join(path, filename + "_agency.txt"), 'w');
        for agency in self.agency:
            file.write(str(agency)+"\n")
        file.close()

        file = open(os.path.join(path, filename + "_frequencies.txt"), 'w');
        for frequency in self.frequencies:
            file.write(str(frequency)+"\n")
        file.close()

        file = open(os.path.join(path, filename + "_routes.txt"), 'w');
        for route in self.routes:
            file.write(str(route)+"\n")
        file.close()

        file = open(os.path.join(path, filename + "_shapes.txt"), 'w');
        for shape in self.shapes:
            file.write(str(shape)+"\n")
        file.close()

        file = open(os.path.join(path, filename + "_stops.txt"), 'w');
        for stop in self.stops:
            file.write(str(stop)+"\n")
        file.close()

        file = open(os.path.join(path, filename + "_transfers.txt"), 'w');
        for transfer in self.transfers:
            file.write(str(transfer)+"\n")
        file.close()

        file = open(os.path.join(path, filename + "_trips.txt"), 'w');
        for trip in self.trips:
            file.write(str(trip)+"\n")
        file.close()

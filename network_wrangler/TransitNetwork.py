#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import os, sys
import partridge as ptg

from Logger import WranglerLogger
from partridge.gtfs import Feed

class TransitNetwork(object):
    '''
    Representation of a Transit Network.
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
    
    
    def read(self, agency_file: str, frequencies_file: str, routes_file: str, 
             shapes_file: str, stops_files: str, transfers_file: str, 
             trips_file: str, path: str = '.') -> TransitNetwork:
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
        path: the location of all the network standard files
        '''
        
        
        #TODO: not sure which column I should be passing for the second dictionary.
        #I thought it might be the ID column but their examples are not consistent with that
        #and their documentation is horrible.... Any suggestions?
        view = {agency_file: {'service_id': None},
                frequencies_file: {'service_id': None},
                routes_file: {'service_id': None},
                shapes_file: {'service_id': None},
                stops_files: {'service_id': None},
                transfers_file: {'service_id': None},
                trips_file: {'service_id': None},
                }
        
        
        if not os.path.exists(path):
            WranglerLogger.debug("\nPath [%s] doesn't exist; creating." % path)
            os.mkdir(path)
            
        #TODO: do we want load_feed or load_geo_feed?
        feed = ptg.load_feed(path, view)
        
        WranglerLogger.info('Read %s agencies from %s' % (feed.agency.size, agency_file))
        WranglerLogger.info('Read %s frequencies from %s' % (feed.frequencies.size, frequencies_file))
        WranglerLogger.info('Read %s routes from %s' % (feed.routes.size, routes_file))
        WranglerLogger.info('Read %s shapes from %s' % (feed.shapes.size, shapes_file))
        WranglerLogger.info('Read %s stops from %s' % (feed.stops.size, stops_files))
        WranglerLogger.info('Read %s transfers from %s' % (feed.transfers.size, transfers_file))
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
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import partridge as ptg

from Logger import WranglerLogger
from partridge.gtfs import Feed

class TransitNetwork(object):
    '''
    Representation of a Transit Network.
    '''

    def __init__(self, params):
        '''
        Constructor
        '''
        self.agency = None
        self.frequencies = None
        self.routes = None
        self.shapes = None
        self.stops = None
        self.transfers = None
        self.trips = None
    
    
    def read(self, network):
        '''
        Reads a network from the transit network standard
        
        '''
        
        #gtfs_feed = ptg.feed(network, view={'trips.txt': {'service_id': service_ids},})
    
    def write(self, path='.', filename = None, fileFormat:"lin"):
        '''
        Writes a network in the transit network standard and/or Cube .lin files
        
        args:
        path (string): the path were the output will be saved
        filename (string): the name prefix of the transit file that is generated
        fileFormat (string): the network format that is written out, use 'lin' for cube format and 'txt' for network standard
        '''
        
        if not os.path.exists(path):
            WranglerLogger.debug("\nPath [%s] doesn't exist; creating." % path)
            os.mkdir(path)
            
        else:
            
            if fileFormat == "lin":
                tansit_file = os.path.join(path, filename + "." + fileFormat)
                
                if os.path.exists(tansit_file):
                    print ("File [%s] exists already.  Overwrite contents? (y/n/s) " % tansit_file)
                    response = input("")
                    WranglerLogger.debug("response = [%s]" % response)
                    
                    if response == "s" or response == "S":
                        WranglerLogger.debug("Skipping!")
                        return
                    
                    if response != "Y" and response != "y":
                        exit(0)
                        
                WranglerLogger.info("Writing into %s\\%s" % (path, filename))
                
                file = open(tansit_file, 'w');
                #TODO: write the file. What am i writing to the cube file again???
                file.write(";;<<Trnbuild>>;;\n")
                file.close()
                
            if fileFormat == "txt":
                file = open(os.path.join(path, filename + "_agency." + fileFormat), 'w');
                for agency in self.agency:
                    file.write(str(agency)+"\n")
                file.close()
                
                file = open(os.path.join(path, filename + "_frequencies." + fileFormat), 'w');
                for frequency in self.frequencies:
                    file.write(str(frequency)+"\n")
                file.close()
                    
                file = open(os.path.join(path, filename + "_routes." + fileFormat), 'w');
                for route in self.routes:
                    file.write(str(route)+"\n")
                file.close()
                
                file = open(os.path.join(path, filename + "_shapes." + fileFormat), 'w');
                for shape in self.shapes:
                    file.write(str(shape)+"\n")
                file.close()
                
                file = open(os.path.join(path, filename + "_stops." + fileFormat), 'w');
                for stop in self.stops:
                    file.write(str(stop)+"\n")
                file.close()
                
                file = open(os.path.join(path, filename + "_transfers." + fileFormat), 'w');
                for transfer in self.transfers:
                    file.write(str(transfer)+"\n")
                file.close()
                
                file = open(os.path.join(path, filename + "_trips." + fileFormat), 'w');
                for trip in self.trips:
                    file.write(str(trip)+"\n")
                file.close()
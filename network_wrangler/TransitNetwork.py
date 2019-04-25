#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

#from Logger import WranglerLogger

class TransitNetwork(object):
    '''
    Representation of a Transit Network.
    '''


    def __init__(self, params):
        '''
        Constructor
        '''
    
    
    def read(self, network):
        '''
        Reads the St Paul network from the transit network standard
        
        '''
    
    def write(self, path='.', filename = None):
        '''
        Writes the St Paul network in the transit network standard and/or Cube .lin files
        
        args:
        path (string): the path were the output will be saved
        filename (string): the name of the transit file that is generated
        '''
        
        if not os.path.exists(path):
            #WranglerLogger.debug("\nPath [%s] doesn't exist; creating." % path)
            os.mkdir(path)
            
        else:
            tansit_file = os.path.join(path, filename + ".lin")
            
            if os.path.exists(tansit_file):
                print ("File [%s] exists already.  Overwrite contents? (y/n/s) " % tansit_file)
                response = input("")
                #WranglerLogger.debug("response = [%s]" % response)
                
                if response == "s" or response == "S":
                    #WranglerLogger.debug("Skipping!")
                    return

                if response != "Y" and response != "y":
                    exit(0)
                    
        #WranglerLogger.info("Writing into %s\\%s" % (path, filename))
                
        file = open(os.path.join(path, filename + ".lin"), 'w');
        file.write(";;<<Trnbuild>>;;\n")
        file.close()
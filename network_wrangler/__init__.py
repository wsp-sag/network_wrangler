##TODO expose classes and functions by importing them hereself.
# i.e. from .highway_network import Highway_network  

__version__ = '0.0.0'

import sys

from Logger import WranglerLogger, setupLogging
from ProjectCard import ProjectCard

__all__ = ['WranglerLogger','setupLogging']

if __name__ == '__main__':   
    setupLogging(logFileName = 'network_wrangler.log')    
    #ProjectCard().read("../example/stpaul/project_cards/1_simple_roadway_attribute_change.yml")
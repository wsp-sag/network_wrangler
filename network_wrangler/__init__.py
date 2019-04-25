##TODO expose classes and functions by importing them hereself.
# i.e. from .highway_network import Highway_network  

__version__ = '0.0.0'

import sys
import logging
from ProjectCard import ProjectCard

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler('network_wrangler.log')
    
    file_handler.setLevel(logging.DEBUG)
    console_handler.setLevel(logging.WARNING)
    
    
    FORMAT = logging.Formatter('%(asctime)-15s %(message)s')
    console_handler.setFormatter(FORMAT)
    file_handler.setFormatter(FORMAT)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    logger.debug('debug')
    logger.info('info')
    logger.warning('warning')
    logger.error('error')
    logger.critical('critical')
       
    #ProjectCard().read("../example/stpaul/project_cards/1_simple_roadway_attribute_change.yml")
    
    

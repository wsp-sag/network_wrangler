#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Utility functions for logging

import sys
import logging

__all__ = ["WranglerLogger", "setupLogging"]

WranglerLogger = logging.getLogger("WranglerLogger")


def setupLogging(level=None, logFileName=None, logToConsole=False):
    """
        Sets up the Wrangler Logger
        
    args:
        level (int): the level of logging that will be recorded 
        logFileName (string): the name of the log file that will get created
        logToConsole(boolean): if True, logging will go to the console
        
        """

    if level is None:
        WranglerLogger.setLevel(logging.DEBUG)
    else:
        WranglerLogger.setLevel(level)

    FORMAT = logging.Formatter(
        "%(asctime)-15s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S,"
    )

    if logFileName:
        file_handler = logging.FileHandler(logFileName)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(FORMAT)
        WranglerLogger.addHandler(file_handler)

    if logToConsole:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.WARNING)
        console_handler.setFormatter(FORMAT)
        WranglerLogger.addHandler(console_handler)

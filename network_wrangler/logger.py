#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Utility functions for logging

import sys
import logging

__all__ = ["WranglerLogger", "setupLogging"]

WranglerLogger = logging.getLogger("WranglerLogger")


def setupLogging(
    level: int = None, log_filename: str = None, log_to_console: bool = False
):
    """
    Sets up the WranglerLogger w.r.t. the debug file location and if logging to console.

    args:
        level: the level of logging that will be recorded
        log_filename: the location of the log file that will get created to add the DEBUG log
        log_to_console: if True, logging will go to the console at INFO level
        """

    if level is None:
        WranglerLogger.setLevel(logging.DEBUG)
    else:
        WranglerLogger.setLevel(level)

    FORMAT = logging.Formatter(
        "%(asctime)-15s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S,"
    )

    if log_filename:
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(FORMAT)
        WranglerLogger.addHandler(file_handler)

    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(FORMAT)
        WranglerLogger.addHandler(console_handler)

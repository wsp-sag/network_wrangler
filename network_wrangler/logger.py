#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Utility functions for logging

import sys, os
import logging
from datetime import datetime

__all__ = ["WranglerLogger", "setupLogging"]

WranglerLogger = logging.getLogger("WranglerLogger")


def setupLogging(
    info_log_filename: str = None,
    debug_log_filename: str = None,
    log_to_console: bool = True
):
    """
    Sets up the WranglerLogger w.r.t. the debug file location and if logging to console.

    args:
        info_log_filename: the location of the log file that will get created to add the INFO log.
                           The INFO Log is terse, just gives the bare minimum of details.
        debug_log_filename: the location of the log file that will get created to add the DEBUG log.
                            The DEBUG log is very noisy, for debugging.
        log_to_console: if True, logging will go to the console at DEBUG level
        """
    # clear handlers if any exist already
    WranglerLogger.handlers = []

    # default is 'debug' so that debug inof won't be ignored is individual handler
    WranglerLogger.setLevel(logging.DEBUG)

    FORMAT = logging.Formatter(
        "%(asctime)-15s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S,"
    )

    # if info_log_name not provided, default to "network_wrangler.info.log" in current working directory
    if not info_log_filename:
        info_log_filename = os.path.join(
                os.getcwd(),
                "network_wrangler_{}.info.log".format(datetime.now().strftime("%Y_%m_%d__%H_%M_%S")),
                )
    info_log_handler = logging.StreamHandler(open(info_log_filename, "w"))
    info_log_handler.setLevel(logging.INFO)
    info_log_handler.setFormatter(FORMAT)
    WranglerLogger.addHandler(info_log_handler)

    # create debug file only when debug_log_filename is provided
    if debug_log_filename:
        debug_log_handler = logging.StreamHandler(open(debug_log_filename, "w"))
        debug_log_handler.setLevel(logging.DEBUG)
        debug_log_handler.setFormatter(FORMAT)
        WranglerLogger.addHandler(debug_log_handler)

    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(FORMAT)
        WranglerLogger.addHandler(console_handler)

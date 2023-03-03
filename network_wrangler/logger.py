#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Utility functions for logging
from datetime import datetime
import os
import sys
import logging

__all__ = ["WranglerLogger", "setup_logging"]

WranglerLogger = logging.getLogger("WranglerLogger")

def setup_logging(
    info_log_filename: str = None, 
    debug_log_filename: str = "wrangler_{}.debug.log".format(datetime.now().strftime("%Y_%m_%d__%H_%M_%S")),
    log_to_console: bool = False
):
    """
    Sets up the WranglerLogger w.r.t. the debug file location and if logging to console.

    args:
        info_log_filename: the location of the log file that will get created to add the INFO log.
            The INFO Log is terse, just gives the bare minimum of details.
            Defaults to file in cwd() `wrangler_[datetime].log`. To turn off logging to a file,
            use log_filename = None.
        debug_log_filename: the location of the log file that will get created to add the DEBUG log.
            The DEBUG log is very noisy, for debugging. Defaults to file in cwd() 
            `wrangler_[datetime].log`. To turn off logging to a file, use log_filename = None.
        log_to_console: if True, logging will go to the console at DEBUG level. Defaults to False.
    """

    #add function variable so that we know if logging has been called
    setup_logging.called = True

    # Clear handles if any exist already
    WranglerLogger.handlers = []

    WranglerLogger.setLevel(logging.DEBUG)

    FORMAT = logging.Formatter(
        "%(asctime)-15s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S,"
    )
    if not info_log_filename:
        info_log_filename = os.path.join(
                os.getcwd(),
                "network_wrangler_{}.info.log".format(datetime.now().strftime("%Y_%m_%d__%H_%M_%S")),
                )

    info_file_handler = logging.StreamHandler(open(info_log_filename,'w'))
    info_file_handler.setLevel(logging.INFO)
    info_file_handler.setFormatter(FORMAT)
    WranglerLogger.addHandler(info_file_handler)

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

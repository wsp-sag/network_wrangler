#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Utility functions for logging

import sys
import logging

__all__ = ["WranglerLogger", "setupLogging"]

WranglerLogger = logging.getLogger("WranglerLogger")


def setupLogging(
    level: int = None,
    info_log_filename: str = None,
    debug_log_filename: str = None,
    log_to_console: bool = True
):
    """
    Sets up the WranglerLogger w.r.t. the debug file location and if logging to console.

    args:
        level: the level of logging that will be recorded
        info_log_filename: the location of the log file that will get created to add the INFO log.
                           The INFO Log is terse, just gives the bare minimum of details.
        debug_log_filename: the location of the log file that will get created to add the DEBUG log.
                            The DEBUG log is very noisy, for debugging.
        log_to_console: if True, logging will go to the console at DEBUG level
        """
    # clear handlers if any exist already
    WranglerLogger.handlers = []

    if level is None:
        WranglerLogger.setLevel(logging.DEBUG)
    else:
        WranglerLogger.setLevel(level)

    FORMAT = logging.Formatter(
        "%(asctime)-15s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S,"
    )

    if info_log_filename:
        info_log_handler = logging.StreamHandler(open(info_log_filename, "w"))
        info_log_handler.setLevel(logging.INFO)
        info_log_handler.setFormatter(FORMAT)
        WranglerLogger.addHandler(info_log_handler)

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

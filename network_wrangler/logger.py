"""Logging utilities for Network Wrangler."""

# Utility functions for logging
from datetime import datetime
import os
import sys
import logging


WranglerLogger = logging.getLogger("WranglerLogger")


def setup_logging(
    info_log_filename: str = None,
    debug_log_filename: str = "wrangler_{}.debug.log".format(
        datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
    ),
    std_out_level: str = "info",
):
    """Sets up the WranglerLogger w.r.t. the debug file location and if logging to console.

    Called by the test_logging fixture in conftest.py and can be called by the user to setup
    logging for their session. If called multiple times, the logger will be reset.

    Args:
        info_log_filename: the location of the log file that will get created to add the INFO log.
            The INFO Log is terse, just gives the bare minimum of details.
            Defaults to file in cwd() `wrangler_[datetime].log`. To turn off logging to a file,
            use log_filename = None.
        debug_log_filename: the location of the log file that will get created to add the DEBUG log
            The DEBUG log is very noisy, for debugging. Defaults to file in cwd()
            `wrangler_[datetime].log`. To turn off logging to a file, use log_filename = None.
        std_out_level: the level of logging to the console. One of "info", "warning", "debug".
            Defaults to "info" but will be set to ERROR if nothing provided matches.
    """
    # add function variable so that we know if logging has been called
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

    info_file_handler = logging.StreamHandler(open(info_log_filename, "w"))
    info_file_handler.setLevel(logging.INFO)
    info_file_handler.setFormatter(FORMAT)
    WranglerLogger.addHandler(info_file_handler)

    # create debug file only when debug_log_filename is provided
    if debug_log_filename:
        debug_log_handler = logging.StreamHandler(open(debug_log_filename, "w"))
        debug_log_handler.setLevel(logging.DEBUG)
        debug_log_handler.setFormatter(FORMAT)
        WranglerLogger.addHandler(debug_log_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(FORMAT)
    WranglerLogger.addHandler(console_handler)
    if std_out_level == "debug":
        console_handler.setLevel(logging.DEBUG)
    elif std_out_level == "info":
        console_handler.setLevel(logging.DEBUG)
    elif std_out_level == "warning":
        console_handler.setLevel(logging.WARNING)
    else:
        console_handler.setLevel(logging.ERROR)

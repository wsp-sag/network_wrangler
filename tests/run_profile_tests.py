#! /usr/bin/env python3
"""Runs tests with profiling enabled.

Can visualize the results using snakeviz:

    ```shell
    snakeviz tests/profile_results/profile.prof
    ```
"""

import subprocess


def run_tests_with_profiling():
    command = [
        "pytest",
        "-m", "profile",
        "--profile",
    ]
    subprocess.run(command)


run_tests_with_profiling()

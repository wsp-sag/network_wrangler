"""Tests the documentation can be built without errors."""

import subprocess

from network_wrangler.logger import WranglerLogger


def test_mkdocs_build(request):
    """Tests that the MkDocs documentation can be built without errors."""
    WranglerLogger.info(f"--Starting: {request.node.name}")
    subprocess.run(["mkdocs", "build"], capture_output=True, text=True, check=True)
    WranglerLogger.info(f"--Finished: {request.node.name}")

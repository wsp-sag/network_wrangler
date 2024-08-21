"""Test that the package can be setup and imported."""

from network_wrangler import WranglerLogger


def test_setup():
    """Create virtual environment and test that network wranlger can be installed and imported."""
    import subprocess
    import shutil

    WranglerLogger.debug("Creating virtual environment...")
    subprocess.run(["python", "-m", "venv", "wranglertest"], check=True)
    WranglerLogger.debug("Created virtual environment.\nInstalling Wrangler...")
    install_process = subprocess.run(["wranglertest/bin/pip", "install", "-e", "."], check=True)
    WranglerLogger.debug(f"Installed Wrangler.\n{install_process.stdout}")
    pip_list_process = subprocess.run(
        ["wranglertest/bin/pip", "list"], capture_output=True, text=True
    )
    WranglerLogger.debug(f"Venv contents:\n{pip_list_process.stdout}")
    WranglerLogger.debug("Testing import...")
    subprocess.run(["wranglertest/bin/python", "-c", "import network_wrangler"], check=True)
    WranglerLogger.debug("Wrangler can import.\nTesting importing dependencies...")
    subprocess.run(["wranglertest/bin/python", "-c", "import osmnx"], check=True)
    WranglerLogger.debug("Dependencies can import.\nRemoving virtual env...")
    shutil.rmtree("wranglertest")

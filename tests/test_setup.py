"""Test that the package can be setup and imported."""


def test_setup():
    """Create virtual environment and test that network wranlger can be installed and imported."""
    import subprocess
    import shutil

    subprocess.run(["python", "-m", "venv", "wranglertest"], check=True)
    subprocess.run(["wranglertest/bin/pip", "install", "network_wrangler"], check=True)
    # Test that network wrangler can be imported
    import network_wrangler

    # Test that dependencies were installed
    import osmnx

    shutil.rmtree("wranglertest")

"""Models for when you want to use vanilla (non wrangler) GTFS."""

from .types import *


class MockPaModel:
    """Mock model for when Pandera is not installed."""

    def __init__(self, **kwargs):
        """Mock modle initiation."""
        for key, value in kwargs.items():
            setattr(self, key, value)


try:
    from .tables import (
        FrequenciesTable,
        RoutesTable,
        ShapesTable,
        StopsTable,
        StopTimesTable,
        WranglerShapesTable,
        WranglerStopsTable,
        WranglerStopTimesTable,
        WranglerTripsTable,
    )
except ImportError:
    # Mock the data models
    import logging

    log = logging.getLogger(__name__)
    log.warning("Pandera is not installed, using mock models.")
    globals().update(
        {
            "StopsTable": MockPaModel,
            "RoutesTable": MockPaModel,
            "TripsTable": MockPaModel,
            "StopTimesTable": MockPaModel,
            "ShapesTable": MockPaModel,
            "FrequenciesTable": MockPaModel,
            "WranglerStopTimesTable": MockPaModel,
            "WranglerStopsTable": MockPaModel,
            "WranglerShapesTable": MockPaModel,
        }
    )

from types import *
from records import *


class MockPaModel:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


try:
    from gtfs import GTFSModel
    from tables import (
        StopsTable,
        RoutesTable,
        TripsTable,
        StopTimesTable,
        ShapesTable,
        FrequenciesTable,
    )
except ImportError:
    # Mock the data models
    import logging

    log = logging.getLogger(__name__)
    log.warning("Pandera is not installed, using mock models.")
    globals().update(
        {
            "GTFSModel": MockPaModel,
            "StopsTable": MockPaModel,
            "RoutesTable": MockPaModel,
            "TripsTable": MockPaModel,
            "StopTimesTable": MockPaModel,
            "ShapesTable": MockPaModel,
            "FrequenciesTable": MockPaModel,
        }
    )

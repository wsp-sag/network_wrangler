"""Filters and queries of a gtfs frequencies table."""

from __future__ import annotations
from pandera.typing import DataFrame

from ...logger import WranglerLogger
from ...models.gtfs.tables import FrequenciesTable, TripsTable


def frequencies_for_trips(
    frequencies: DataFrame[FrequenciesTable], trips: DataFrame[TripsTable]
) -> DataFrame[FrequenciesTable]:
    """Filter frequenceis dataframe to records associated with trips table."""
    _sel_trips = trips.trip_id.unique().tolist()
    filtered_frequencies = frequencies[frequencies.trip_id.isin(_sel_trips)]
    WranglerLogger.debug(
        f"Filtered frequencies to {len(filtered_frequencies)}/{len(frequencies)} \
                         records that referenced one of {len(trips)} trips."
    )
    return filtered_frequencies

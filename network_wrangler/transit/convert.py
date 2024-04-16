from pandera.errors import SchemaErrors

from ..logger import WranglerLogger

from ..models.gtfs.tables import (
    StopTimesTable,
    WranglerStopTimesTable,
    WranglerStopsTable,
)


def gtfs_to_wrangler_stop_times(
    stop_times: StopTimesTable, stops: WranglerStopsTable = None, **kwargs
) -> WranglerStopTimesTable:
    WranglerLogger.debug("Converting GTFS stop_times to Wrangler stop_times")
    try:
        stop_times = StopTimesTable.validate(stop_times)
    except SchemaErrors as e:
        WranglerLogger.error(
            "Failed validating StopTimes to generic gtfs StopTimes table."
        )
        raise e
    if stops is None:
        raise ValueError("Must provide stops to convert stop_times")

    _merge_fields = ["stop_id"]

    if "trip_id" in stops:
        _merge_fields.append("trip_id")

    _stops_fields = _merge_fields + ["model_node_id"]

    if "model_node_id" in stop_times:
        if stop_times["model_node_id"].isna().any():
            stop_times = stop_times.drop(columns=["model_node_id"])
        else:
            return stop_times

    _m_df = stop_times.merge(
        stops[_stops_fields],
        on=_merge_fields,
        how="left",
    )
    # make sure each stop in stop_times exists in stops
    _missing_stops = _m_df.loc[_m_df["model_node_id"].isna(), "stop_id"]
    if not _missing_stops.empty:
        WranglerLogger.error(
            f"Missing stops in stops.txt that are found in \
                             stop_times.txt: {_missing_stops.to_list()}"
        )
        raise ValueError("Stops and stop_times tables incompatable.")
    # WranglerLogger.debug(f"Merged stop times df with stops:\n{_m_df}")
    stop_times["model_node_id"] = _m_df["model_node_id"]
    # WranglerLogger.debug(f"StopTimes with updated model_node_id:\n{stop_times}")

    return stop_times

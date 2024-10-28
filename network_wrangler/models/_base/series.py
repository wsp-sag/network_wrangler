import pandera as pa

"""
Time strings in HH:MM or HH:MM:SS format up to 48 hours.
"""
TimeStrSeriesSchema = pa.SeriesSchema(
    pa.String,
    pa.Check.str_matches(r"^(?:[0-9]|[0-3][0-9]|4[0-7]):[0-5]\d(?::[0-5]\d)?$|^24:00(?::00)?$"),
    coerce=True,
    name=None,  # Name is set to None to ignore the Series name
)

import pandera as pa

# Define a general schema for any Series with time strings in HH:MM or HH:MM:SS format
TimeStrSeriesSchema = pa.SeriesSchema(
    pa.String,
    pa.Check.str_matches(r"^(?:[0-9]|[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?$|^24:00(?::00)?$"),
    coerce=True,
    name=None  # Name is set to None to ignore the Series name
)

from enum import Enum

import pandas as pd
from pandera.extensions import register_check_method

from pydantic import ValidationError

from ...logger import WranglerLogger


@register_check_method
def uniqueness(df, *, cols: list[str]):
    """
    Custom check method to check for uniqueness of values in a DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame to check for uniqueness.
        cols (list[str]): The list of column names to check for uniqueness.

    Returns:
        bool: True if the values in the specified columns are unique, False otherwise.
    """
    dupes = df[cols].duplicated()
    if dupes.sum():
        WranglerLogger.error(
            f"Non-Unique values found in column/column-set: \
                              {cols}:\n{df.loc[dupes, cols]}"
        )
    return dupes.sum() == 0


@register_check_method
def is_enum(series: pd.Series, *, enum_class: Enum) -> bool:
    valid = series.isin([e.value for e in enum_class])
    if series[~valid].any():
        WranglerLogger.error(f"Invalid values for {enum_class}: {series[~valid]}")
    return valid.all()

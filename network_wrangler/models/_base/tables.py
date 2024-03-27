from pandera.extensions import register_check_method

@register_check_method()
def uniqueness(df, *, cols: list[str]):
    """
    Custom check method to check for uniqueness of values in a DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame to check for uniqueness.
        cols (list[str]): The list of column names to check for uniqueness.

    Returns:
        bool: True if the values in the specified columns are unique, False otherwise.
    """
    return df[cols].duplicated().sum() == 0

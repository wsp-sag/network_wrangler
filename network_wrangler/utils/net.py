"""Functions to help with network manipulations in dataframes."""

from pandas import DataFrame


def point_seq_to_links(
    point_seq_df: DataFrame,
    id_field: str,
    seq_field: str,
    node_id_field: str,
    from_field: str = "A",
    to_field: str = "B",
) -> DataFrame:
    """Translates a df with tidy data representing a sequence of points into links.

    Args:
        point_seq_df (pd.DataFrame): Dataframe with source breadcrumbs
        id_field (str): Trace ID
        seq_field (str): Order of breadcrumbs within ID_field
        node_id_field (str): field denoting the node ID
        from_field (str, optional): Field to export from_field to. Defaults to "A".
        to_field (str, optional): Field to export to_field to. Defaults to "B".

    Returns:
        pd.DataFrame: Link records with id_field, from_field, to_field
    """
    point_seq_df = point_seq_df.sort_values(by=[id_field, seq_field])

    links = point_seq_df.add_suffix(f"_{from_field}").join(
        point_seq_df.shift(-1).add_suffix(f"_{to_field}")
    )

    links = links[links[f"{id_field}_{to_field}"] == links[f"{id_field}_{from_field}"]]

    links = links.drop(columns=[f"{id_field}_{to_field}"])
    links = links.rename(
        columns={
            f"{id_field}_{from_field}": id_field,
            f"{node_id_field}_{from_field}": from_field,
            f"{node_id_field}_{to_field}": to_field,
        }
    )

    links = links.dropna(subset=[from_field, to_field])
    # Since join with a shift() has some NAs, we need to recast the columns to int
    _int_cols = [to_field, f"{seq_field}_{to_field}"]
    links[_int_cols] = links[_int_cols].astype(int)
    return links

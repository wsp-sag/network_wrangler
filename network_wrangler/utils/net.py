from ..logger import WranglerLogger


def point_seq_to_links(
    point_seq_df: "pd.DataFrame",
    id_field: str,
    seq_field: str,
    node_id_field: str,
    from_field: str = "A",
    to_field: str = "B",
) -> "pd.DataFrame":
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

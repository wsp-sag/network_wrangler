"""Utilities for generating ID values."""

import re

import pandas as pd

from network_wrangler.logger import WranglerLogger
from network_wrangler.utils.utils import split_string_prefix_suffix_from_num


class IdCreationError(Exception):
    """Error raised when an ID cannot be created."""


def generate_new_id_from_existing(
    input_id: str,
    existing_ids: pd.Series,
    id_scalar: int,
    iter_val: int = 10,
    max_iter: int = 1000,
) -> str:
    """Generate a new ID that isn't in existing_ids.

    Input id is generally an id that have been copied and needs to be made unique.

    TODO: check a registry rather than existing IDs

    Args:
        input_id: id to use to generate new id.
        existing_ids: series that has existing IDs that should be unique
        id_scalar: scalar value to initially use to create the new id.
        iter_val: iteration value to use in the generation process.
        max_iter: maximum number of iterations allowed in the generation process.
    """
    str_prefix, input_id, str_suffix = split_string_prefix_suffix_from_num(input_id)

    for i in range(1, max_iter + 1):
        new_id = f"{str_prefix}{int(input_id) + id_scalar + (iter_val * i)}{str_suffix}"
        if new_id not in existing_ids.values:
            return new_id
    msg = f"Cannot generate new id within max iters of {max_iter}."
    WranglerLogger.error(msg)
    raise IdCreationError(msg)


def generate_list_of_new_ids_from_existing(
    input_ids: list[str],
    existing_ids: pd.Series,
    id_scalar: int,
    iter_val: int = 10,
    max_iter: int = 1000,
) -> list[str]:
    """Generates a list of new IDs based on the input IDs, existing IDs, and other parameters.

    Input ids are generally ids that have been copied and need to be made unique.

    Args:
        input_ids (list[str]): The input IDs for which new IDs need to be generated.
        existing_ids (pd.Series): The existing IDs that should be avoided when generating new IDs.
        id_scalar (int): The scalar value used to generate new IDs.
        iter_val (int, optional): The iteration value used in the generation process.
            Defaults to 10.
        max_iter (int, optional): The maximum number of iterations allowed in the generation
            process. Defaults to 1000.

    Returns:
        list[str]: A list of new IDs generated based on the input IDs and other parameters.
    """
    # keep new_ids as list to preserve order
    new_ids = []
    existing_ids = set(existing_ids)
    for i in input_ids:
        new_id = generate_new_id_from_existing(
            i,
            pd.Series(list(existing_ids)),
            id_scalar,
            iter_val=iter_val,
            max_iter=max_iter,
        )
        new_ids.append(new_id)
        existing_ids.add(new_id)
    return new_ids


def _get_max_int_id_within_string_ids(id_s: pd.Series, prefix: str, suffix: str) -> int:
    pattern = re.compile(rf"{re.escape(prefix)}(\d+){re.escape(suffix)}")
    extracted_ids = id_s.dropna().apply(
        lambda x: int(match.group(1)) if (match := pattern.search(x)) else None
    )
    extracted_ids = extracted_ids.dropna()
    if extracted_ids.empty:
        return 0
    return extracted_ids.max()


def create_str_int_combo_ids(
    n_ids: int, taken_ids_s: pd.Series, str_prefix: str = "", str_suffix: str = ""
) -> list:
    """Create a list of string IDs that are not in taken_ids_s.

    Args:
        n_ids (int): Number of IDs to create.
        taken_ids_s (pd.Series): Series of IDs that are already taken.
        str_prefix (str, optional): Prefix to add to the new ID. Defaults to "".
        str_suffix (str, optional): Suffix to add to the new ID. Defaults to "".
    """
    if not isinstance(taken_ids_s.iloc[0], str):
        msg = "taken_ids_s must be a series of strings."
        WranglerLogger.error(msg)
        raise IdCreationError(msg)

    start_id = _get_max_int_id_within_string_ids(taken_ids_s, str_prefix, str_suffix) + 1
    return [f"{str_prefix}{i}{str_suffix}" for i in range(start_id, start_id + n_ids)]


def fill_str_int_combo_ids(
    id_s: pd.Series, taken_ids_s: pd.Series, str_prefix: str = "", str_suffix: str = ""
) -> pd.Series:
    """Fill NaN values in id_s for string type surrounding a number.

    Args:
        id_s (pd.Series): Series of IDs to fill.
        taken_ids_s (pd.Series): Series of IDs that are already taken.
        str_prefix (str, optional): Prefix to add to the new ID. Defaults to "".
        str_suffix (str, optional): Suffix to add to the new ID. Defaults to "".
    """
    n_ids = id_s.isna().sum()
    new_ids = create_str_int_combo_ids(n_ids, taken_ids_s, str_prefix, str_suffix)
    id_s.loc[id_s.isna()] = new_ids
    return id_s


def fill_int_ids(id_s: pd.Series, taken_ids_s: pd.Series) -> pd.Series:
    """Fill NaN values in id_s with values that are not in taken_ids_s for int type.

    Args:
        id_s (pd.Series): Series of IDs to fill.
        taken_ids_s (pd.Series): Series of IDs that are already taken.
    """
    if not isinstance(taken_ids_s.iloc[0], int):
        msg = f"taken_ids_s must be a series of integers, found: {taken_ids_s.iloc[0]}"
        WranglerLogger.error(msg)
        raise IdCreationError(msg)
    n_ids = id_s.isna().sum()
    start_id = max(set(taken_ids_s.dropna())) + 1

    new_ids = pd.Series(range(start_id, start_id + n_ids), index=id_s.index)
    id_s.loc[id_s.isna()] = new_ids
    return id_s

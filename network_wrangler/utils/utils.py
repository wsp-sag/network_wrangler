"""General utility functions used throughout package."""

import hashlib
import re

from typing import Union

import pandas as pd

from ..logger import WranglerLogger


def topological_sort(adjacency_list, visited_list):
    """Topological sorting for Acyclic Directed Graph.

    Parameters:
    - adjacency_list (dict): A dictionary representing the adjacency list of the graph.
    - visited_list (list): A list representing the visited status of each vertex in the graph.

    Returns:
    - output_stack (list): A list containing the vertices in topological order.

    This function performs a topological sort on an acyclic directed graph. It takes an adjacency
    list and a visited list as input. The adjacency list represents the connections between
    vertices in the graph, and the visited list keeps track of the visited status of each vertex.

    The function uses a recursive helper function to perform the topological sort. It starts by
    iterating over each vertex in the visited list. For each unvisited vertex, it calls the helper
    function, which recursively visits all the neighbors of the vertex and adds them to the output
    stack in reverse order. Finally, it returns the output stack, which contains the vertices in
    topological order.
    """
    output_stack = []

    def _topology_sort_util(vertex):
        if not visited_list[vertex]:
            visited_list[vertex] = True
            for neighbor in adjacency_list[vertex]:
                _topology_sort_util(neighbor)
            output_stack.insert(0, vertex)

    for vertex in visited_list:
        _topology_sort_util(vertex)

    return output_stack


def make_slug(text: str, delimiter: str = "_") -> str:
    """Makes a slug from text."""
    text = re.sub("[,.;@#?!&$']+", "", text.lower())
    return re.sub("[\ ]+", delimiter, text)  # noqa: W605


def delete_keys_from_dict(dictionary: dict, keys: list) -> dict:
    """Removes list of keys from potentially nested dictionary.

    SOURCE: https://stackoverflow.com/questions/3405715/
    User: @mseifert

    Args:
        dictionary: dictionary to remove keys from
        keys: list of keys to remove

    """
    keys_set = set(keys)  # Just an optimization for the "if key in keys" lookup.

    modified_dict = {}
    for key, value in dictionary.items():
        if key not in keys_set:
            if isinstance(value, dict):
                modified_dict[key] = delete_keys_from_dict(value, keys_set)
            else:
                modified_dict[key] = (
                    value  # or copy.deepcopy(value) if a copy is desired for non-dicts.
                )
    return modified_dict


def get_overlapping_range(ranges: list[Union[tuple[int], range]]) -> Union[None, range]:
    """Returns the overlapping range for a list of ranges or tuples defining ranges.

    Args:
        ranges (list[Union[tuple[int], range]]): A list of ranges or tuples defining ranges.

    Returns:
        Union[None, range]: The overlapping range if found, otherwise None.

    Example:
        >>> ranges = [(1, 5), (3, 7), (6, 10)]
        >>> get_overlapping_range(ranges)
        range(3, 5)

    """
    _ranges = [r if isinstance(r, range) else range(r[0], r[1]) for r in ranges]

    _overlap_start = max(r.start for r in _ranges)
    _overlap_end = min(r.stop for r in _ranges)

    if _overlap_start < _overlap_end:
        return range(_overlap_start, _overlap_end)
    else:
        return None


def findkeys(node, kv):
    """Returns values of all keys in various objects.

    Adapted from arainchi on Stack Overflow:
    https://stackoverflow.com/questions/9807634/find-all-occurrences-of-a-key-in-nested-dictionaries-and-lists
    """
    if isinstance(node, list):
        for i in node:
            for x in findkeys(i, kv):
                yield x
    elif isinstance(node, dict):
        if kv in node:
            yield node[kv]
        for j in node.values():
            for x in findkeys(j, kv):
                yield x


def split_string_prefix_suffix_from_num(input_string: str):
    """Split a string prefix and suffix from *last* number.

    Args:
        input_string (str): The input string to be processed.

    Returns:
        tuple: A tuple containing the prefix (including preceding numbers),
               the last numeric part as an integer, and the suffix.

    Notes:
        This function uses regular expressions to split a string into three parts:
        the prefix, the last numeric part, and the suffix. The prefix includes any
        preceding numbers, the last numeric part is converted to an integer, and
        the suffix includes any non-digit characters after the last numeric part.

    Examples:
        >>> split_string_prefix_suffix_from_num("abc123def456")
        ('abc', 123, 'def456')

        >>> split_string_prefix_suffix_from_num("hello")
        ('hello', 0, '')

        >>> split_string_prefix_suffix_from_num("123")
        ('', 123, '')

    """
    input_string = str(input_string)
    pattern = re.compile(r"(.*?)(\d+)(\D*)$")
    match = pattern.match(input_string)

    if match:
        # Extract the groups: prefix (including preceding numbers), last numeric part, suffix
        prefix, numeric_part, suffix = match.groups()
        # Convert the numeric part to an integer
        num_variable = int(numeric_part)
        return prefix, num_variable, suffix
    else:
        return input_string, 0, ""


def generate_new_id(
    input_id: str,
    existing_ids: pd.Series,
    id_scalar: int,
    iter_val: int = 10,
    max_iter: int = 1000,
) -> str:
    """Generate a new ID that isn't in existing_ids.

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
        elif i == max_iter:
            WranglerLogger.error(f"Cannot generate new id within max iters of {max_iter}.")
            raise ValueError("Cannot create unique new id.")


def generate_list_of_new_ids(
    input_ids: list[str],
    existing_ids: pd.Series,
    id_scalar: int,
    iter_val: int = 10,
    max_iter: int = 1000,
) -> list[str]:
    """Generates a list of new IDs based on the input IDs, existing IDs, and other parameters.

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
        new_id = generate_new_id(
            i,
            pd.Series(list(existing_ids)),
            id_scalar,
            iter_val=iter_val,
            max_iter=max_iter,
        )
        new_ids.append(new_id)
        existing_ids.add(new_id)
    return new_ids


def dict_to_hexkey(d: dict) -> str:
    """Converts a dictionary to a hexdigest of the sha1 hash of the dictionary.

    Args:
        d (dict): dictionary to convert to string

    Returns:
        str: hexdigest of the sha1 hash of dictionary
    """
    return hashlib.sha1(str(d).encode()).hexdigest()


def combine_unique_unhashable_list(list1: list, list2: list):
    """Combines lists preserving order of first and removing duplicates.

    Args:
        list1 (list): The first list.
        list2 (list): The second list.

    Returns:
        list: A new list containing the elements from list1 followed by the
        unique elements from list2.

    Example:
        >>> list1 = [1, 2, 3]
        >>> list2 = [2, 3, 4, 5]
        >>> combine_unique_unhashable_list(list1, list2)
        [1, 2, 3, 4, 5]
    """
    return [item for item in list1 if item not in list2] + list2


def normalize_to_lists(mixed_list: list[Union[str, list]]) -> list[list]:
    """Turn a mixed list of scalars and lists into a list of lists."""
    normalized_list = []
    for item in mixed_list:
        if isinstance(item, str):
            normalized_list.append([item])
        else:
            normalized_list.append(item)
    return normalized_list


def list_elements_subset_of_single_element(mixed_list: list[Union[str, list[str]]]) -> bool:
    """Find the first list in the mixed_list."""
    potential_supersets = []
    for item in mixed_list:
        if isinstance(item, list) and len(item) > 0:
            potential_supersets.append(set(item))

    # If no list is found, return False
    if not potential_supersets:
        return False

    normalized_list = normalize_to_lists(mixed_list)

    valid_supersets = []
    for ss in potential_supersets:
        if all(ss.issuperset(i) for i in normalized_list):
            valid_supersets.append(ss)

    return len(valid_supersets) == 1


def check_one_or_one_superset_present(
    mixed_list: list[Union[str, list[str]]], all_fields_present: list[str]
) -> bool:
    """Checks that exactly one of the fields in mixed_list is in fields_present or one superset."""
    normalized_list = normalize_to_lists(mixed_list)

    list_items_present = [i for i in normalized_list if set(i).issubset(all_fields_present)]

    if len(list_items_present) == 1:
        return True

    return list_elements_subset_of_single_element(list_items_present)

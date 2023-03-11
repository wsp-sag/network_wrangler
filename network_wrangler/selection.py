#!/usr/bin/env python
# -*- coding: utf-8 -*-


from typing import Any, Collection, Mapping

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError
from .logger import WranglerLogger


def build_selection_query(
    selection: Mapping[str, Any],
    type: str = "links",
    unique_ids: Collection[str] = [],
    mode: Collection[str] = ["drive_access"],
    ignore: Collection[str] = [],
):
    """Generates the query for selecting links within links_df.

    Args:
        selection: Selection dictionary from project card.
        type: one of "links" or "nodes"
        unique_ids: Properties which are unique in network and can be used
            for selecting individual links or nodes without other properties.
        mode: Limits selection to certain modes.
            Defaults to ["drive_access"].
        ignore: _description_. Defaults to [].

    Returns:
        _type_: _description_
    """
    sel_query = "("
    count = 0

    selection_keys = [k for li in selection[type] for k, v in li.items()]

    unique_ids_sel = list(set(unique_ids) & set(selection_keys))

    for li in selection[type]:
        for key, value in li.items():

            if key in ignore:
                continue

            if unique_ids_sel and key not in unique_ids:
                continue

            count = count + 1

            if isinstance(value, list):
                sel_query = sel_query + "("
                v = 1
                for i in value:  # building an OR query with each element in list
                    if isinstance(i, str):
                        sel_query = sel_query + key + '.str.contains("' + i + '")'
                    else:
                        sel_query = sel_query + key + "==" + str(i)
                    if v != len(value):
                        sel_query = sel_query + " or "
                        v = v + 1
                sel_query = sel_query + ")"
            elif isinstance(value, str):
                sel_query = sel_query + key + "==" + '"' + str(value) + '"'
            else:
                sel_query = sel_query + key + "==" + str(value)

            if not unique_ids_sel and count != (len(selection[type]) - len(ignore)):
                sel_query = sel_query + " and "

            if unique_ids_sel and count != len(unique_ids_sel):
                sel_query = sel_query + " and "

    if not unique_ids_sel:
        if count > 0:
            sel_query = sel_query + " and "

        # add mode query
        mode_sel = "(" + " or ".join(m + "==1" for m in mode) + ")"
        sel_query = sel_query + mode_sel

    sel_query = sel_query + ")"

    return sel_query

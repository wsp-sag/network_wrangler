"""Deletes links from RoadLinksTable."""

from pandera.typing import DataFrame

from ...logger import WranglerLogger
from ...models.roadway.tables import RoadLinksTable, RoadLinksAttrs
from ...utils.models import validate_call_pyd


class LinkDeletionError(Exception):
    """Raised when there is an issue with deleting links."""

    pass


@validate_call_pyd
def delete_links_by_ids(
    links_df: DataFrame[RoadLinksTable],
    del_link_ids: list[int],
    ignore_missing: bool = False,
) -> DataFrame[RoadLinksTable]:
    """Delete links from a links table.

    Args:
        links_df: DataFrame[RoadLinksTable] to delete links from.
        del_link_ids: list of link ids to delete.
        ignore_missing: if True, will not raise an error if a link id to delete is not in
            the network. Defaults to False.
    """
    WranglerLogger.debug(f"Deleting links with ids: \n{del_link_ids}")
    # TODO write wrapper on validate call so don't have to do this
    links_df.attrs.update(RoadLinksAttrs)
    _missing = set(del_link_ids) - set(links_df.index)
    if _missing:
        WranglerLogger.warning(f"Links in network not there to delete: \n{_missing}")
        if not ignore_missing:
            raise LinkDeletionError("Links to delete are not in the network.")
    return links_df.drop(labels=del_link_ids, errors="ignore")

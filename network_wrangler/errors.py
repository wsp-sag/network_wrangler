"""All network wrangler errors."""


class FeedReadError(Exception):
    """Raised when there is an error reading a transit feed."""


class FeedValidationError(Exception):
    """Raised when there is an issue with the validation of the GTFS data."""


class InvalidScopedLinkValue(Exception):
    """Raised when there is an issue with a scoped link value."""


class LinkAddError(Exception):
    """Raised when there is an issue with adding links."""


class LinkChangeError(Exception):
    """Raised when there is an error in changing a link property."""


class LinkCreationError(Exception):
    """Raised when there is an issue with creating links."""


class LinkDeletionError(Exception):
    """Raised when there is an issue with deleting links."""


class LinkNotFoundError(Exception):
    """Raised when a link is not found in the links table."""


class ManagedLaneAccessEgressError(Exception):
    """Raised when there is an issue with access/egress points to managed lanes."""


class MissingNodesError(Exception):
    """Raised when referenced nodes are missing from the network."""


class NewRoadwayError(Exception):
    """Raised when there is an issue with applying a new roadway."""


class NodeAddError(Exception):
    """Raised when there is an issue with adding nodes."""


class NodeChangeError(Exception):
    """Raised when there is an issue with applying a node change."""


class NodeDeletionError(Exception):
    """Raised when there is an issue with deleting nodes."""


class NodesInLinksMissingError(Exception):
    """Raised when there is an issue with validating links and nodes."""


class NodeNotFoundError(Exception):
    """Raised when a node is not found in the nodes table."""


class NotLinksError(Exception):
    """Raised when a dataframe is not a RoadLinksTable."""


class NotNodesError(Exception):
    """Raised when a dataframe is not a RoadNodesTable."""


class ProjectCardError(Exception):
    """Raised when a project card is not valid."""


class RoadwayDeletionError(Exception):
    """Raised when there is an issue with applying a roadway deletion."""


class RoadwayPropertyChangeError(Exception):
    """Raised when there is an issue with applying a roadway property change."""


class ScenarioConflictError(Exception):
    """Raised when a conflict is detected."""


class ScenarioCorequisiteError(Exception):
    """Raised when a co-requisite is not satisfied."""


class ScenarioPrerequisiteError(Exception):
    """Raised when a pre-requisite is not satisfied."""


class ScopeConflictError(Exception):
    """Raised when there is a scope conflict in a list of ScopedPropertySetItems."""


class ScopeLinkValueError(Exception):
    """Raised when there is an issue with ScopedLinkValueList."""


class SegmentFormatError(Exception):
    """Error in segment format."""


class SegmentSelectionError(Exception):
    """Error in segment selection."""


class SelectionError(Exception):
    """Raised when there is an issue with a selection."""


class DataframeSelectionError(Exception):
    """Raised when there is an issue with a selection from a dataframe."""


class ShapeAddError(Exception):
    """Raised when there is an issue with adding shapes."""


class ShapeDeletionError(Exception):
    """Raised when there is an issue with deleting shapes from a network."""


class SubnetExpansionError(Exception):
    """Raised when a subnet can't be expanded to include a node or set of nodes."""


class SubnetCreationError(Exception):
    """Raised when a subnet can't be created."""


class TimeFormatError(Exception):
    """Time format error exception."""


class TimespanFormatError(Exception):
    """Timespan format error exception."""


class TransitPropertyChangeError(Exception):
    """Error raised when applying transit property changes."""


class TransitRouteAddError(Exception):
    """Error raised when applying add transit route."""


class TransitRoutingChangeError(Exception):
    """Raised when there is an error in the transit routing change."""


class TransitSelectionError(Exception):
    """Base error for transit selection errors."""


class TransitSelectionEmptyError(Exception):
    """Error for when no transit trips are selected."""


class TransitSelectionNetworkConsistencyError(TransitSelectionError):
    """Error for when transit selection dictionary is not consistent with transit network."""


class TransitRoadwayConsistencyError(Exception):
    """Error raised when transit network is inconsistent with roadway network."""


class TransitValidationError(Exception):
    """Error raised when transit network doesn't have expected values."""

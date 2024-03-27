import logging

from typing import Annotated, Any, ClassVar, Dict, List, Union

from pydantic import BaseModel, ConfigDict, model_validator, Field, ValidationError

log = logging.getLogger(__name__)

AllOf = Annotated[
    List[str],
    Field(
        description=[
            "List fields where all are required for the data model to be valid."
        ]
    ),
]


OneOf = Annotated[
    List[Union[str, AllOf]],
    Field(
        description=[
            "List fields where at least one is required for the data model to be valid."
        ]
    ),
]


AnyOf = Annotated[
    List[Union[str, AllOf]],
    Field(
        description=[
            "List fields, AllOf where any are required for the data model to be valid."
        ]
    ),
]


class RecordModel(BaseModel):
    """A pydantic model for records which adds validation for require_any_of and require_one_of.

    Attributes:
        model_config (ConfigDict): Configuration dictionary for the model.
        require_any_of (ClassVar[AnyOf]): Class variable specifying fields that require at least one of them to be present.
        require_one_of (ClassVar[OneOf]): Class variable specifying fields that require exactly one of them to be present.
    """

    model_config = ConfigDict(
        protected_namespaces=(),
        use_enum_values=True,  # for dumping a model to a dict to have value rather than Enum obj
    )
    require_any_of: ClassVar[AnyOf] = []
    require_one_of: ClassVar[OneOf] = []
    _examples: ClassVar = []

    @staticmethod
    def _check_field_exists(field: Union[str, List[str]], values: Dict[str, Any]):
        if isinstance(field, list):
            return all(f in values for f in field)
        return field in values

    @model_validator(mode="before")
    def check_one_of(cls, values):
        """Return True if exactly one of the fields in require_one_of is in values.

        If one of the fields in require_one_of is a list, then return True for that list item if
            ALL of the fields in the list are in values.
        """
        if not cls.require_one_of:
            return values

        if (
            sum(
                [cls._check_field_exists(field, values) for field in cls.require_one_of]
            )
            == 1
        ):
            return values

        raise ValidationError(f"{cls} should have exactly one of {cls.require_one_of}")

    @model_validator(mode="before")
    def check_any_of(cls, values):
        """Return True if at least one of the fields in any_of_fields is in values.

        If one of the fields in require_any_of is a list, then return True if ALL of the fields
            in the list are in values.

        ["a","b",["c","d","e"]] and return true if it has at least one match of a, b, or (c,d, AND e)
        """
        if not cls.require_any_of:
            return values
        if any(
            [cls._check_field_exists(field, values) for field in cls.require_any_of]
        ):
            return values
        raise ValidationError(f"{cls} should have at least one of {cls.require_any_of}")

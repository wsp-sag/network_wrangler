from typing import Any, ClassVar, Dict, List, Union

from pydantic import BaseModel, ConfigDict, model_validator
from .types import AnyOf, OneOf


class OneOfError(ValueError):
    pass


class AnyOfError(ValueError):
    pass


class RecordModel(BaseModel):
    """A pydantic model for records which adds validation for require_any_of and require_one_of.

    Attributes:
        model_config (ConfigDict): Configuration dictionary for the model.
        require_any_of (ClassVar[AnyOf]): Class variable specifying fields that require at least
            one of them to be present.
        require_one_of (ClassVar[OneOf]): Class variable specifying fields that require exactly
            one of them to be present.
    """

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        exclude_none=True,
        protected_namespaces=(),
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

        raise OneOfError(f"{cls} should have exactly one of {cls.require_one_of}")

    @model_validator(mode="before")
    def check_any_of(cls, values):
        """
        Validates that at least one of the specified fields in `require_any_of` exists in `values`.

        This method supports complex validation rules allowing for nested lists indicating
        all fields within that list must be present to satisfy a single "any of" condition.

        For example, given ["a", "b", ["c", "d", "e"]], this validates true if either "a" or "b" is present,
        or all of "c", "d", and "e" are present.

        Args:
            values (dict): The dictionary of field values to validate.

        Returns:
            dict: The original `values` if validation passes.

        Raises:
            ValueError: If none of the `require_any_of` conditions are met.
        """
        if not cls.require_any_of:
            return values

        # Corrected to iterate over require_any_of
        if any(cls._check_field_exists(field, values) for field in cls.require_any_of):
            return values

        raise AnyOfError(
            f"{cls.__name__} requires at least one of the following fields: \
                        {cls.require_any_of}. None were provided."
        )

import logging
from typing import Any, ClassVar, Dict, List, Union
from pydantic import BaseModel, ConfigDict, model_validator

from .types import AnyOf, OneOf
from ...utils import _fk_in_pk

log = logging.getLogger(__name__)


class RecordModel(BaseModel):
    """A pydantic model for records which adds validation for require_any_of and require_one_of.

    Attributes:
        model_config (ConfigDict): Configuration dictionary for the model.
        require_any_of (ClassVar[AnyOf]): Class variable specifying fields that require at least 
            one of them to be present.
        require_one_of (ClassVar[OneOf]): Class variable specifying fields that require exactly 
            one of them to be present.
    """

    model_config = ConfigDict(protected_namespaces=())
    require_any_of: ClassVar[AnyOf] = []
    require_one_of: ClassVar[OneOf] = []
    _examples: ClassVar = []

    @staticmethod
    def _check_field_exists(field: Union[str, List[str]], values: Dict[str, Any]):
        if isinstance(field, list):
            return all(f in values for f in field)
        return field in values

    @model_validator(pre=True)
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

        raise ValueError(f"{cls} should have exactly one of {cls.require_one_of}")

    @model_validator(pre=True)
    def check_any_of(cls, values):
        """Return True if at least one of the fields in any_of_fields is in values.

        If one of the fields in require_any_of is a list, then return True if ALL of the fields
            in the list are in values.

        ["a","b",["c","d","e"]] and return true if it has at least one match of a, b, or (c,d, AND e)
        """
        if not cls.require_any_of:
            return values
        if any(
            [cls._check_field_exists(field, values) for field in cls.require_one_of]
        ):
            return values
        raise ValueError(f"{cls} should have at least one of {cls.any_of}")


class ForeignKeyFieldMissing(Exception):
    pass


class ForeignKeyValueError(Exception):
    pass


class DBModel(BaseModel):
    """A pydantic model for interrelated tables.

    Attributes:
        model_config (ConfigDict): Configuration dictionary for the model.
    """

    @property
    def fk_tables(self):
        """Return the tables that have foreign keys in the model."""
        for table in self.model_fields.values():
            if "_fks" in table:
                yield table

    @property
    def fk_fields(self):
        """Return the fk field constraints."""
        for table in self.fk_tables:
            for field in table._fks:
                yield table, field, *table._fks[field]

    @model_validator()
    def check_fks(cls, values):
        """Return if the foreign key fields in values are valid.

        Note: will just give a warning if the specified foreign key table does not exist.
        """
        fks_valid = True
        for table, field, fk_table, fk_field in cls.fk_fields:
            if fk_table not in values:
                log.warning(
                    f"Table {fk_table} for specified FK {table} not in {cls}. \
                            Proceeding without fk validation."
                )
                continue
            if fk_field not in values[fk_table]:
                raise ForeignKeyFieldMissing(
                    f"Specified FK {fk_field} not in {fk_table}"
                )
            _valid, _missing = _fk_in_pk(values.table[field], values.fk_table[fk_field])
            fks_valid = fks_valid and _valid
            if _missing:
                log.error(
                    f"FK referenced in {table}.{field} missing in {fk_table}.{fk_field} :\n{_missing}"
                )
        if not fks_valid:
            raise ForeignKeyValueError(f"Foreign keys missing. See log for details.")
        return values

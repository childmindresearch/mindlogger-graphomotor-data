"""Model for BIDS structure."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd


@dataclass
class BidsConfig:
    """Configuration for BIDS structure generation."""

    subject_formatter: Callable[[BidsEntity], str]
    task_name_formatter: Callable[[BidsEntity], str]
    session_formatter: Callable[[BidsEntity], str]
    entity_formatter: Callable[[BidsEntity], str]
    merge_participants_tsv: bool = True
    merge_dataset_description: bool = True
    # If include_sessions is True, then the BIDS model will include session folders.
    include_sessions: bool = False
    default_session_name: Optional[str] = None
    # TODO: Implement increment_session_id option for auto-incrementing numerical
    # session IDs. Requires another lambda to order sessions.
    # increment_session_id: bool = False

    def relative_entity_dir(self, entity: BidsEntity) -> Path:
        """Return relative path to entity."""
        rel_path = Path(self.subject_formatter(entity))
        if self.include_sessions:
            rel_path /= self.session_formatter(entity)
        rel_path /= entity.datatype
        return rel_path

    def relative_entity_path(self, entity: BidsEntity) -> Path:
        """Return relative path to entity."""
        return self.relative_entity_dir(entity) / self.entity_formatter(entity)

    def relative_entity_metadata_path(self, entity: BidsEntity) -> Path:
        """Return relative path to entity metadata."""
        return self.relative_entity_path(entity).with_suffix(".json")

    @classmethod
    def default(cls) -> BidsConfig:
        """Return default BIDS configuration."""
        return BidsConfig(
            subject_formatter=cls.default_subject_formatter(),
            task_name_formatter=cls.default_task_name_formatter(),
            session_formatter=cls.default_session_formatter(),
            entity_formatter=cls.default_entity_formatter(),
        )

    @classmethod
    def default_subject_formatter(cls) -> Callable[[BidsEntity], str]:
        """Return default subject formatter."""
        return lambda entity: f"sub-{entity.subject_id}"

    @classmethod
    def default_task_name_formatter(cls) -> Callable[[BidsEntity], str]:
        """Return default task name formatter."""
        return lambda entity: f"task-{entity.task_name}"

    @classmethod
    def default_session_formatter(cls) -> Callable[[BidsEntity], str]:
        """Return default session formatter."""
        return lambda entity: f"ses-{entity.session_id}"

    @classmethod
    def default_entity_formatter(cls) -> Callable[[BidsEntity], str]:
        """Return default entity formatter.

        Assumes all entities have session_ids.
        """

        def entity_formatter(entity: BidsEntity) -> str:
            name_parts = [f"sub-{entity.subject_id}"]
            # TODO: Include session_id in entity name.
            # if entity.session_id is not None:
            #     name_parts.append(f"ses-{entity.session_id}")
            # else:
            #     # Raise ValueError because if any entities have session IDs, all must.
            #     raise ValueError("Session ID is required.")
            name_parts.append(f"task-{entity.task_name}")
            if entity.run_id is not None:
                name_parts.append(f"run-{entity.run_id}")
            name_parts.append(entity.suffix)
            return "_".join(name_parts)

        return entity_formatter


@dataclass(frozen=True)
class BidsModel:
    """Model of BIDS structure for generation, conversion and merging."""

    entities: List[BidsEntity]
    dataset_description: Dict[str, str]

    def _bids_subject_label(self, entity: BidsEntity) -> str:
        """Return BIDS subject label."""
        return f"sub-{entity.subject_id}"

    def _bids_task_label(self, entity: BidsEntity) -> str:
        """Return BIDS task label."""
        return f"task-{entity.task_name}"

    def _bids_session_label(self, entity: BidsEntity) -> Optional[str]:
        """Return BIDS session label."""
        return f"ses-{entity.session_id}" if entity.session_id else None

    def _bids_run_label(self, entity: BidsEntity) -> Optional[str]:
        """Return BIDS run label."""
        return f"run-{entity.run_id}" if entity.run_id else None

    @cached_property
    def has_sessions(self) -> bool:
        """Return True if BIDS model has sessions."""
        return any([entity.session_id for entity in self.entities])

    @cached_property
    def subject_ids(self) -> List[str]:
        """Return list of subject IDs."""
        return list(set([entity.subject_id for entity in self.entities]))


@dataclass(frozen=True)
class BidsEntity:
    """Model of BIDS entity, a representation of data within the BIDS structure.

    Entities can be files or tabular data.
    Only one of file or tabular_data should be set.
    """

    subject_id: str
    datatype: str
    task_name: str
    suffix: str
    session_id: Optional[str] = None
    metadata: Optional[Dict[str, str]] = None
    file_path: Optional[Path] = None
    tabular_data: Optional[pd.DataFrame] = None
    run_id: Optional[str] = None

    def is_file_resource(self) -> bool:
        """Return True if entity is a file resource."""
        return self.file_path is not None

    def is_tabular_data(self) -> bool:
        """Return True if entity is tabular data."""
        return self.tabular_data is not None


class BidsBuilder:
    """Builder for BIDS Model."""

    def __init__(
        self, entities: List[BidsEntity] = [], dataset_description: Dict[str, str] = {}
    ) -> None:
        """Initialize BIDS builder."""
        self._entities = entities
        self._dataset_description = dataset_description

    def build(self) -> BidsModel:
        """Build BIDS model."""
        return BidsModel(self._entities, self._dataset_description)

    def add_dataset_description(
        self, name: str, bids_version: str, fields: Dict[str, Any]
    ) -> BidsBuilder:
        """Add dataset description.

        https://bids-specification.readthedocs.io/en/stable/modality-agnostic-files.html#dataset_descriptionjson
        """
        self._dataset_description.update(
            {
                "Name": name,
                "BIDSVersion": bids_version,
            }
        )
        self._dataset_description.update(fields)
        return self

    def add(
        self,
        subject_id: str,
        datatype: str,
        task_name: str,
        suffix: str,
        resource: Path | pd.DataFrame,
        run_id: Optional[str] = None,
        session_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> BidsBuilder:
        """Add data to BIDS structure."""
        if isinstance(resource, Path):
            self._entities.append(
                BidsEntity(
                    subject_id=subject_id,
                    datatype=datatype,
                    task_name=task_name,
                    suffix=suffix,
                    session_id=session_id,
                    file_path=resource,
                    metadata=metadata,
                    run_id=run_id,
                )
            )
        elif isinstance(resource, pd.DataFrame):
            self._entities.append(
                BidsEntity(
                    subject_id=subject_id,
                    datatype=datatype,
                    task_name=task_name,
                    suffix=suffix,
                    session_id=session_id,
                    tabular_data=resource,
                    metadata=metadata,
                    run_id=run_id,
                )
            )
        else:
            raise ValueError("Resource must be a file path or tabular data.")
        return self

"""Writer for BIDS Model."""

from __future__ import annotations

import json
import logging
import shutil
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import Callable, Dict, Optional, Type

import pandas as pd

from .bids_model import BidsBuilder, BidsConfig, BidsEntity, BidsModel

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


class MergeStrategy(Enum):
    """Merge strategy for BIDS structure.

    NO_MERGE: Do not merge, exit with error on conflict.
    OVERWRITE: Overwrite existing files on conflict.
    KEEP: Keep existing files on conflict.
    RENAME_FILE: Rename files on conflict using run-label increments.
    NEW_SESSION: Write new session folder for all data additions.
    """

    UNKNOWN_MERGE = 0
    NO_MERGE = 1
    OVERWRITE = 2
    KEEP = 3
    RENAME_ENTITIES = 5
    NEW_SESSION = 6

    def __str__(self) -> str:
        """Return string representation of MergeStrategy."""
        return self.name

    def __repr__(self) -> str:
        """Return string representation of MergeStrategy."""
        return f"{self.__class__}.{str(self)}"

    @staticmethod
    def argparse(s: str) -> MergeStrategy:
        """Parse MergeStrategy from argparse string."""
        return MergeStrategy[s.upper()]


class BidsWriter:
    """Writer for BIDS Model."""

    def __init__(
        self,
        bids_root: Path,
        entity_merge_strategy: MergeStrategy,
        bids: Optional[BidsModel] = None,
        config: BidsConfig = BidsConfig.default(),
    ) -> None:
        """Initialize BIDS writer."""
        self._bids_root = bids_root
        self._entity_merge_strategy = entity_merge_strategy
        self._bids = bids
        self._builder: Optional[BidsBuilder] = None
        self._config = config

    def __enter__(self) -> BidsWriter:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        """Exit context manager."""
        return self.write()

    def builder(self) -> BidsBuilder:
        """Return BIDS builder."""
        if self._builder is None:
            self._builder = BidsBuilder()
        return self._builder

    def _merge_json(self, path: Path, data: Dict[str, str]) -> None:
        """Merge JSON file with BIDS structure."""
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                existing_data = json.load(f)
            existing_data.update(data)
            data = existing_data
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def _merge_tsv(self, path: Path, data: pd.DataFrame) -> None:
        """Merge TSV file with BIDS structure."""
        if path.exists():
            existing_data = pd.read_csv(path, sep="\t")
            data = pd.concat([existing_data, data])
        data.to_csv(path, sep="\t", index=False)

    def _ensure_directory_path(self, path: Path, is_dir: bool = False) -> None:
        """Ensure directory path exists."""
        if is_dir:
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)
        else:
            if not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)

    def _merge_entity(
        self, entity: BidsEntity, write_op: Callable[[Path], None]
    ) -> None:
        """Write file respecting MergeStrategy."""
        expected_filepath = self._bids_root / self._config.relative_entity_path(entity)
        if (
            self._entity_merge_strategy == MergeStrategy.NO_MERGE
            and expected_filepath.exists()
        ):
            raise ValueError(f"File {expected_filepath} already exists.")
        elif self._entity_merge_strategy == MergeStrategy.OVERWRITE:
            self._ensure_directory_path(expected_filepath)
            write_op(expected_filepath)
            if entity.metadata:
                self._merge_json(
                    self._bids_root
                    / self._config.relative_entity_metadata_path(entity),
                    entity.metadata,
                )
            return
        elif self._entity_merge_strategy == MergeStrategy.KEEP:
            return
        elif self._entity_merge_strategy == MergeStrategy.RENAME_ENTITIES:
            raise NotImplementedError("RENAME_ENTITIES merge strategy not implemented.")
        elif self._entity_merge_strategy == MergeStrategy.NEW_SESSION:
            raise NotImplementedError("New session merge strategy not implemented.")
        else:
            raise ValueError(f"Unknown merge strategy {self._entity_merge_strategy}.")

    def write(self) -> bool:
        """Write BIDS structure to disk."""
        if self._bids is None and self._builder is None:
            raise ValueError("No BIDS model to write.")

        if self._bids is None and self._builder is not None:
            self._bids = self._builder.build()

        # Unwrap Optional value for type-checking.
        if self._bids is None:
            raise ValueError("No BIDS model or BIDS Builder to write.")

        # Write BIDS structure
        # Confirm root
        LOG.info(f"Writing BIDS structure to {self._bids_root}")
        self._bids_root.mkdir(parents=True, exist_ok=True)
        if len(list(self._bids_root.iterdir())) > 0:
            if self._entity_merge_strategy == MergeStrategy.UNKNOWN_MERGE:
                raise ValueError("BIDS root is not empty, merge strategy required.")
            if self._entity_merge_strategy == MergeStrategy.NO_MERGE:
                raise ValueError("BIDS root is not empty, cannot merge.")

        # Write dataset_description.json
        LOG.info("Writing dataset_description.json")
        data_description_path = self._bids_root / "dataset_description.json"
        if not data_description_path.exists() or self._config.merge_dataset_description:
            self._merge_json(data_description_path, self._bids.dataset_description)

        # Write participants.tsv
        LOG.info("Writing participants.tsv")
        participants_path = self._bids_root / "participants.tsv"
        if not participants_path.exists() or self._config.merge_participants_tsv:
            self._merge_tsv(
                participants_path, pd.DataFrame({"participant": self._bids.subject_ids})
            )

        # Write subject folders
        for entity in self._bids.entities:
            LOG.info(f"Writing entity {entity.subject_id}")
            if entity.file_path is not None:
                fp = entity.file_path
                self._merge_entity(entity, lambda path: shutil.copy2(fp, path))
            elif entity.tabular_data is not None:
                tb = entity.tabular_data
                self._merge_entity(
                    entity,
                    lambda path: tb.to_csv(path, sep="\t", index=False),
                )
            else:
                raise ValueError(f"Unknown entity type for {entity}")
        return True

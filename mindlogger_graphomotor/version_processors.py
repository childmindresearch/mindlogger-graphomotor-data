"""Processors for different versions of MindLogger Export."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Protocol

import pandas as pd
from bidsi import BidsBuilder
from packaging.version import Version

LOG = logging.getLogger(__name__)


class DataVersionProcessor(Protocol):
    """Protocol for data version processing."""

    def check_version(self, version: Version) -> bool:
        """Check if the processor can handle the given version."""
        pass

    def process_activities(
        self, study_id: str, activities: pd.DataFrame, builder: BidsBuilder
    ) -> BidsBuilder:
        """Process a set of activities, returning a BidsBuilder."""
        pass

    def process_report_row(
        self, row: pd.Series, resource: pd.DataFrame | Path, builder: BidsBuilder
    ) -> BidsBuilder:
        """Process a row of data, adding resources to builder."""
        # Skip study_id rows
        if row.item == "study_id":
            return builder
        builder.add(
            subject_id=row.study_id,
            datatype="beh",
            task_name=row.item,
            resource=resource,
            metadata={
                "mindlogger_id": row.id,
                "source_id": row.source_id,
                "target_id": row.target_id,
                "legacy_user_id": row.legacy_user_id,
                "activity_flow_submission_id": row.activity_flow_submission_id,
                "activity_start_time": row.activity_start_time.isoformat(),
                "activity_end_time": row.activity_start_time.isoformat(),
                "activity_scheduled_time": row.activity_scheduled_time,
                "flag": row.flag,
                "secret_user_id": row.secret_user_id,
                "user_id": row.userId,
                "activity_id": row.activity_id,
                "activity_name": row.activity_name,
                "activity_flow_id": row.activity_flow_id,
                "activity_flow_name": row.activity_flow_name,
                "item_id": row.item_id,
                "item": row.item,
                "response": row.response,
                "prompt": row.prompt,
                "options": row.options,
                "version": row.version,
                "rawScore": row.rawScore,
                "reviewing_id": row.reviewing_id,
                "event_id": row.event_id,
                "timezone_offset": row.timezone_offset,
            },
        )
        return builder


# TODO: Change name, confirm check_version correctly isolates data.
class NewDataProcessor(DataVersionProcessor):
    """Processor for new MindLogger data."""

    MIN_VERSION = Version("14.6.149")

    def check_version(self, version: Version) -> bool:
        """Check if the processor can handle the given version."""
        return version > self.MIN_VERSION

    def process_activities(
        self, study_id: str, activities: pd.DataFrame, builder: BidsBuilder
    ) -> BidsBuilder:
        """Process a set of activities, returning a BidsBuilder."""
        builder.add(
            subject_id=study_id,
            datatype="beh",
            task_name="activities",
            resource=activities,
        )
        return builder

    def process_report_row(
        self, row: pd.Series, resource: pd.DataFrame | Path, builder: BidsBuilder
    ) -> BidsBuilder:
        """Process a row of data, adding resources to builder."""
        # Skip study_id rows
        if row.item == "study_id":
            return builder
        builder.add(
            subject_id=row.study_id,
            datatype="beh",
            task_name=row.item,
            resource=resource,
            metadata={
                "mindlogger_id": row.id,
                "activity_flow_submission_id": row.activity_flow_submission_id,
                "source_id": row.source_id,
                "target_id": row.target_id,
                "legacy_user_id": row.legacy_user_id,
                "activity_start_time": row.activity_start_time.isoformat(),
                "activity_end_time": row.activity_start_time.isoformat(),
                "activity_scheduled_time": row.activity_scheduled_time,
                "flag": row.flag,
                "secret_user_id": row.secret_user_id,
                "user_id": row.userId,
                "activity_id": row.activity_id,
                "activity_name": row.activity_name,
                "activity_flow_id": row.activity_flow_id,
                "activity_flow_name": row.activity_flow_name,
                "item_id": row.item_id,
                "item": row.item,
                "response": row.response,
                "prompt": row.prompt,
                "options": row.options,
                "version": row.version,
                "rawScore": row.rawScore,
                "reviewing_id": row.reviewing_id,
                "event_id": row.event_id,
                "timezone_offset": row.timezone_offset,
            },
        )
        return builder


class DefaultDataProcessor(DataVersionProcessor):
    """Default processor for MindLogger data."""

    def check_version(self, version: Version) -> bool:
        """Always return True for default processor."""
        return True

    def process_activities(
        self, study_id: str, activities: pd.DataFrame, builder: BidsBuilder
    ) -> BidsBuilder:
        """Process a set of activities, returning a BidsBuilder."""
        builder.add(
            subject_id=study_id,
            datatype="beh",
            task_name="activities",
            resource=activities,
        )
        return builder

    def process_report_row(
        self, row: pd.Series, resource: pd.DataFrame | Path, builder: BidsBuilder
    ) -> BidsBuilder:
        """Process a row of data, adding resources to builder."""
        # Skip study_id rows
        if row.item == "study_id":
            return builder
        builder.add(
            subject_id=row.study_id,
            datatype="beh",
            task_name=row.item,
            resource=resource,
            metadata={
                "mindlogger_id": row.id,
                "activity_start_time": row.activity_start_time.isoformat(),
                "activity_end_time": row.activity_start_time.isoformat(),
                "activity_scheduled_time": row.activity_scheduled_time,
                "flag": row.flag,
                "secret_user_id": row.secret_user_id,
                "user_id": row.userId,
                "activity_id": row.activity_id,
                "activity_name": row.activity_name,
                "activity_flow_id": row.activity_flow_id,
                "activity_flow_name": row.activity_flow_name,
                "item_id": row.item_id,
                "item": row.item,
                "response": row.response,
                "prompt": row.prompt,
                "options": row.options,
                "version": row.version,
                "rawScore": row.rawScore,
                "reviewing_id": row.reviewing_id,
                "event_id": row.event_id,
                "timezone_offset": row.timezone_offset,
            },
        )
        return builder

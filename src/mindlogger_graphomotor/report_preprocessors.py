"""Processors for different versions of MindLogger Export."""

from __future__ import annotations

import logging
from typing import Protocol

import pandas as pd

LOG = logging.getLogger(__name__)


class ReportPreprocessor(Protocol):
    """Protocol for data preprocessing."""

    def __call__(
        self, report: pd.DataFrame, activity: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Preprocess the report."""
        pass


class StudyIdPreprocessor(ReportPreprocessor):
    """Preprocessor for adding study_id to report."""

    def __call__(
        self, report: pd.DataFrame, activity: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Preprocess the report."""
        # Construct column for study_id
        report["study_id"] = report["response"].where(report["item"] == "study_id")
        # Set study_id for cursive_q to the row above, backfill other values
        report["study_id"] = (
            report["study_id"].fillna(report["study_id"].shift(1)).bfill()
        )
        # Drop study_id rows
        report = report.drop(index=report[report["item"] == "study_id"].index)

        # Construct column for study_id in activity
        activity["study_id"] = activity["response"].where(
            activity["item"] == "study_id"
        )
        # Set fill stop value for cursive_q, backfill other values
        activity.loc[activity["item"] == "cursive_q", "study_id"] = "STOP"
        activity["study_id"] = activity["study_id"].bfill()
        # Remove fill stop value and forward fill
        # activity.loc[activity["item"] == "cursive_q", "study_id"] = pd.NA
        activity.loc[activity["study_id"] == "STOP", "study_id"] = pd.NA
        activity["study_id"] = activity["study_id"].ffill()
        return report, activity


class DateTimePreprocessor(ReportPreprocessor):
    """Convert timestamps to datetime."""

    def __call__(
        self, report: pd.DataFrame, activity: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Convert timestamps in report and activity to datetime."""
        # Convert timestamps to datetime
        report["activity_end_time"] = pd.to_datetime(
            report["activity_end_time"], unit="ms", utc=True
        ).dt.tz_convert("America/New_York")
        report["activity_start_time"] = pd.to_datetime(
            report["activity_start_time"], unit="ms", utc=True
        ).dt.tz_convert("America/New_York")
        return report, activity


class CrashPreprocessor(ReportPreprocessor):
    """Preprocessor for handling crashes.

    Note: Relies on report being ordered by timestamp.
    """

    def __call__(
        self, report: pd.DataFrame, activity: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Handle crashes in report."""
        if not report["activity_start_time"].is_monotonic_decreasing:
            raise ValueError(
                "Report must be ordered by timestamp for CrashPreprocessor to work "
                "correctly. Disable CrashPreprocessor or sort report."
            )
        # Handle crashes, which appear as duplicate study_id, item_id rows.
        # For manual skip responses, keep earliest response.
        # For auto-skip responses, no duplicates should exist in report, do not modify.
        # Do not modify activity_user_journey, so that skip events are preserved
        # for analysis.
        report = report.drop_duplicates(subset=["study_id", "item_id"], keep="last")
        return report, activity
